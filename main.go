// AMD GPU device plugin for gVisor amdproxy.
//
// Advertises AMD GPU memory in MiB blocks as "amd.com/gpu-vram-mib".
// One allocation exposes /dev/kfd and /dev/dri/renderD128 to the container.
// Intended to be used alongside the gvisor RuntimeClass; annotation injection
// is handled by a separate MutatingWebhook.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
)

const (
	resourceName    = "amd.com/gpu-vram-mib"
	socketName      = "amd-gpu-vram.sock"
	socketDir       = "/var/lib/kubelet/device-plugins"
	kubeletSocket   = socketDir + "/kubelet.sock"
	devicePluginDir = socketDir
)

var (
	kfdDevice    = flag.String("kfd", "/dev/kfd", "Path to /dev/kfd")
	renderDevice = flag.String("render", "/dev/dri/renderD128", "Path to DRM render node")
	totalVRAMMiB = flag.Int("vram-mib", 0, "Total GPU VRAM in MiB to advertise; 0 reads it from the KFD topology")
	sliceMiB     = flag.Int("slice-mib", 512, "Granularity of each resource unit in MiB")
	kfdSysfs     = flag.String("kfd-sysfs", "/sys/class/kfd/kfd/topology", "KFD topology root")
	nodeName     = flag.String("node-name", "", "this node's name; defaults to $NODE_NAME")
	dryRun       = flag.Bool("dry-run", false, "print what would be advertised and published, then exit")
)

type AMDDevicePlugin struct {
	devices []*pluginapi.Device
	server  *grpc.Server
	socket  string
	stopCh  chan struct{}
}

func newPlugin() *AMDDevicePlugin {
	units := *totalVRAMMiB / *sliceMiB
	devices := make([]*pluginapi.Device, units)
	for i := range devices {
		devices[i] = &pluginapi.Device{
			ID:     fmt.Sprintf("amd-vram-%04d", i),
			Health: pluginapi.Healthy,
		}
	}
	log.Printf("advertising %d x %d-MiB units (%d MiB total) as %s",
		units, *sliceMiB, units**sliceMiB, resourceName)
	return &AMDDevicePlugin{
		devices: devices,
		socket:  filepath.Join(socketDir, socketName),
		stopCh:  make(chan struct{}),
	}
}

func (p *AMDDevicePlugin) GetDevicePluginOptions(_ context.Context, _ *pluginapi.Empty) (*pluginapi.DevicePluginOptions, error) {
	return &pluginapi.DevicePluginOptions{}, nil
}

func (p *AMDDevicePlugin) ListAndWatch(_ *pluginapi.Empty, stream pluginapi.DevicePlugin_ListAndWatchServer) error {
	stream.Send(&pluginapi.ListAndWatchResponse{Devices: p.devices})
	<-p.stopCh
	return nil
}

func (p *AMDDevicePlugin) Allocate(_ context.Context, req *pluginapi.AllocateRequest) (*pluginapi.AllocateResponse, error) {
	resp := &pluginapi.AllocateResponse{}
	for _, r := range req.ContainerRequests {
		mib := len(r.DevicesIDs) * *sliceMiB
		limitBytes := int64(mib) * 1024 * 1024
		resp.ContainerResponses = append(resp.ContainerResponses, &pluginapi.ContainerAllocateResponse{
			Devices: []*pluginapi.DeviceSpec{
				{ContainerPath: *kfdDevice, HostPath: *kfdDevice, Permissions: "rw"},
				{ContainerPath: *renderDevice, HostPath: *renderDevice, Permissions: "rw"},
			},
			Envs: map[string]string{
				// Not used for enforcement (gVisor reads annotations, not envs).
				// Kept for debugging visibility inside the container.
				"AMDGPU_ALLOC_VRAM_MIB":     fmt.Sprintf("%d", mib),
				"AMDGPU_LIMIT_BYTES":        fmt.Sprintf("%d", limitBytes),
				"HSA_USERPTR_FOR_PAGED_MEM": "0",
			},
		})
	}
	return resp, nil
}

func (p *AMDDevicePlugin) PreStartContainer(_ context.Context, _ *pluginapi.PreStartContainerRequest) (*pluginapi.PreStartContainerResponse, error) {
	return &pluginapi.PreStartContainerResponse{}, nil
}

func (p *AMDDevicePlugin) GetPreferredAllocation(_ context.Context, req *pluginapi.PreferredAllocationRequest) (*pluginapi.PreferredAllocationResponse, error) {
	resp := &pluginapi.PreferredAllocationResponse{}
	for _, r := range req.ContainerRequests {
		resp.ContainerResponses = append(resp.ContainerResponses, &pluginapi.ContainerPreferredAllocationResponse{
			DeviceIDs: r.AvailableDeviceIDs[:min(int(r.AllocationSize), len(r.AvailableDeviceIDs))],
		})
	}
	return resp, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (p *AMDDevicePlugin) serve() error {
	os.Remove(p.socket)
	lis, err := net.Listen("unix", p.socket)
	if err != nil {
		return fmt.Errorf("listen %s: %w", p.socket, err)
	}
	p.server = grpc.NewServer()
	pluginapi.RegisterDevicePluginServer(p.server, p)
	go func() {
		if err := p.server.Serve(lis); err != nil {
			log.Printf("gRPC server error: %v", err)
		}
	}()
	return nil
}

func (p *AMDDevicePlugin) register() error {
	conn, err := grpc.NewClient("unix://"+kubeletSocket,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial kubelet: %w", err)
	}
	defer conn.Close()
	client := pluginapi.NewRegistrationClient(conn)
	_, err = client.Register(context.Background(), &pluginapi.RegisterRequest{
		Version:      pluginapi.Version,
		Endpoint:     socketName,
		ResourceName: resourceName,
	})
	return err
}

func main() {
	flag.Parse()

	// Read the card's real shape rather than trusting a flag. VRAM, compute
	// unit count and queue-mask granularity all differ between an APU and a
	// discrete card, and a wrong number here is silent: the node advertises a
	// capacity the hardware does not have and pods are placed against it.
	gpus, err := readTopology(*kfdSysfs)
	if err != nil || len(gpus) == 0 {
		log.Printf("WARNING no GPU found in %s (%v); falling back to --vram-mib", *kfdSysfs, err)
	} else {
		for _, g := range gpus {
			log.Printf("found %s: node %d, %d MiB VRAM, %d CUs, gfx%d, queue masks in groups of %d",
				g.name, g.nodeIndex, g.vramMiB, g.cus, g.gfxVersion, g.cuGroup)
		}
		if *totalVRAMMiB == 0 {
			*totalVRAMMiB = gpus[0].vramMiB
		}
	}
	if *totalVRAMMiB == 0 {
		log.Fatalf("no VRAM size: the topology gave none and --vram-mib was not set")
	}

	name := *nodeName
	if name == "" {
		name = os.Getenv("NODE_NAME")
	}
	if *dryRun {
		dumpRegistration(name, gpus, *sliceMiB)
		return
	}
	if name == "" {
		log.Printf("WARNING no node name (set NODE_NAME); not publishing %s", RegisterAnnotation)
	} else if len(gpus) > 0 {
		if err := publishRegistration(name, gpus, *sliceMiB); err != nil {
			// Not fatal: the resource still reaches the kubelet, so ordinary
			// extended-resource accounting still places pods. Only HAMi's own
			// view of the node is missing.
			log.Printf("WARNING publishing %s: %v", RegisterAnnotation, err)
		} else {
			log.Printf("published %s for %d GPU(s) on node %s", RegisterAnnotation, len(gpus), name)
		}
	}

	p := newPlugin()
	if err := p.serve(); err != nil {
		log.Fatalf("start server: %v", err)
	}
	// Retry registration until kubelet is ready.
	for {
		if err := p.register(); err != nil {
			log.Printf("register (retrying): %v", err)
			time.Sleep(5 * time.Second)
			continue
		}
		log.Printf("registered %s with kubelet", resourceName)
		break
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	<-sig
	close(p.stopCh)
	p.server.Stop()
}
