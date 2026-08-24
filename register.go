// Topology discovery and HAMi node registration.
//
// This file was reconstructed on 2026-08-24. The binary deployed on sens1 does
// all of this, and its source was lost: the directory it was built from is not
// a git repository, main.go on disk predates it (it still defaults --vram-mib
// to an APU's 14170 and reads no topology at all), and the build stamp names a
// revision -- 3eec1b5c9eeb, vcs.modified=true -- that exists in no repository
// we have. What is reproduced here is the *observed behaviour* of that binary:
// its log lines and, exactly, the annotation it publishes. It is not a byte
// recovery and should not be described as one.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// RegisterAnnotation is the annotation HAMi's AMD support reads to learn what
// hardware a node has. Its schema is HAMi's device.DeviceInfo, mirrored here
// rather than imported: this plugin should not take a dependency on the whole
// scheduler to publish one annotation, and the fields are few and stable.
const RegisterAnnotation = "hami.io/node-amd-register"

// deviceInfo is HAMi's device.DeviceInfo, only the fields this publishes.
type deviceInfo struct {
	ID      string `json:"id,omitempty"`
	Index   uint   `json:"index,omitempty"`
	Count   int32  `json:"count,omitempty"`
	Devmem  int32  `json:"devmem,omitempty"`
	Devcore int32  `json:"devcore,omitempty"`
	Type    string `json:"type,omitempty"`
	// No omitempty: the field is published even when zero, which is what
	// HAMi's own writers do and what the annotation on the node shows.
	Numa         int            `json:"numa"`
	Health       bool           `json:"health,omitempty"`
	DeviceVendor string         `json:"devicevendor,omitempty"`
	CustomInfo   map[string]any `json:"custominfo,omitempty"`
}

// gpuTopology is what one KFD topology node says about a GPU.
type gpuTopology struct {
	nodeIndex  int
	vramMiB    int
	cus        int
	gfxVersion int
	cuGroup    int
	name       string
}

// readTopology reads the GPUs out of the KFD topology.
//
// Nothing here may be hardcoded. The GPU id and the KFD major are allocated
// dynamically and genuinely differ between machines, and an APU reports a
// different shape again from a discrete card.
func readTopology(root string) ([]gpuTopology, error) {
	nodes, err := filepath.Glob(filepath.Join(root, "nodes", "*"))
	if err != nil {
		return nil, err
	}
	var out []gpuTopology
	for _, dir := range nodes {
		props, err := os.ReadFile(filepath.Join(dir, "properties"))
		if err != nil {
			continue
		}
		p := parseProperties(string(props))
		// simd_count is 0 on CPU nodes, which have no compute units and are
		// not GPUs.
		if p["simd_count"] == 0 {
			continue
		}
		idx, err := strconv.Atoi(filepath.Base(dir))
		if err != nil {
			continue
		}
		simdPerCU := p["simd_per_cu"]
		if simdPerCU <= 0 {
			simdPerCU = 2
		}
		g := gpuTopology{
			nodeIndex:  idx,
			cus:        int(p["simd_count"] / simdPerCU),
			gfxVersion: int(p["gfx_target_version"]),
			// RDNA pairs compute units into workgroup processors and KFD
			// refuses a queue mask that splits a pair; GCN and CDNA do not
			// pair. Everything from gfx10 on is RDNA.
			cuGroup: 1,
		}
		if g.gfxVersion/10000 >= 10 {
			g.cuGroup = 2
		}
		g.vramMiB = int(vramMiBOf(dir))
		g.name = strings.TrimSpace(readFile(filepath.Join(dir, "name")))
		out = append(out, g)
	}
	return out, nil
}

func readFile(p string) string {
	b, err := os.ReadFile(p)
	if err != nil {
		return ""
	}
	return string(b)
}

// parseProperties reads KFD's "key value" property file.
func parseProperties(s string) map[string]int64 {
	out := map[string]int64{}
	for _, line := range strings.Split(s, "\n") {
		f := strings.Fields(line)
		if len(f) != 2 {
			continue
		}
		v, err := strconv.ParseInt(f[1], 10, 64)
		if err != nil {
			continue
		}
		out[f[0]] = v
	}
	return out
}

// vramMiBOf sums the node's VRAM heaps.
//
// heap_type 1 and 2 are both VRAM: RDNA reports it differently from CDNA, and
// matching only one of them is a mistake this branch has already made once, in
// the sysfs rewrite that left a sandbox seeing the whole card's total.
func vramMiBOf(nodeDir string) int64 {
	banks, _ := filepath.Glob(filepath.Join(nodeDir, "mem_banks", "*", "properties"))
	var total int64
	for _, b := range banks {
		p := parseProperties(readFile(b))
		switch p["heap_type"] {
		case 1, 2:
			total += p["size_in_bytes"]
		}
	}
	return total / (1024 * 1024)
}

// publishRegistration writes what the node has where HAMi will read it.
//
// A patch rather than an update, so that two writers on the same node cannot
// drop each other's work, and failure is a warning rather than fatal: the
// resource is already advertised to the kubelet by then, so a pod can still be
// placed by ordinary extended-resource accounting. Only HAMi's own view is
// missing, and it will be written again on the next attempt.
func publishRegistration(nodeName string, gpus []gpuTopology, sliceMiB int) error {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return err
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return err
	}
	infos := make([]deviceInfo, 0, len(gpus))
	for _, g := range gpus {
		infos = append(infos, deviceInfo{
			ID:      fmt.Sprintf("%s-AMDGPU-%d", nodeName, g.nodeIndex),
			Index:   uint(g.nodeIndex),
			Count:   int32(g.vramMiB / sliceMiB),
			Devmem:  int32(g.vramMiB),
			Devcore: int32(g.cus),
			Type:    g.name,
			Numa:    0,
			Health:  true,
			// Lowercase, matching what HAMi compares against.
			DeviceVendor: "amd",
			// cuGroup and sliceMiB are read by the HAMi fork's CU-mask
			// allocator. Upstream ignores unknown custominfo keys, so
			// publishing them costs nothing when the fork is not in use.
			CustomInfo: map[string]any{
				"cuGroup":    g.cuGroup,
				"gfxVersion": g.gfxVersion,
				"sliceMiB":   sliceMiB,
			},
		})
	}
	encoded, err := json.Marshal(infos)
	if err != nil {
		return err
	}
	patch, err := json.Marshal(map[string]any{
		"metadata": map[string]any{
			"annotations": map[string]string{RegisterAnnotation: string(encoded)},
		},
	})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, err = cs.CoreV1().Nodes().Patch(ctx, nodeName, types.StrategicMergePatchType, patch, metav1.PatchOptions{})
	return err
}

// dumpRegistration prints what would be advertised and published without
// touching the cluster, so a reconstruction can be checked against the
// annotation a running plugin already wrote.
func dumpRegistration(nodeName string, gpus []gpuTopology, sliceMiB int) {
	if nodeName == "" {
		nodeName = "NODE"
	}
	infos := make([]deviceInfo, 0, len(gpus))
	for _, g := range gpus {
		infos = append(infos, deviceInfo{
			ID:           fmt.Sprintf("%s-AMDGPU-%d", nodeName, g.nodeIndex),
			Index:        uint(g.nodeIndex),
			Count:        int32(g.vramMiB / sliceMiB),
			Devmem:       int32(g.vramMiB),
			Devcore:      int32(g.cus),
			Type:         g.name,
			Numa:         0,
			Health:       true,
			DeviceVendor: "amd",
			CustomInfo: map[string]any{
				"cuGroup":    g.cuGroup,
				"gfxVersion": g.gfxVersion,
				"sliceMiB":   sliceMiB,
			},
		})
	}
	b, _ := json.Marshal(infos)
	fmt.Printf("%s=%s\n", RegisterAnnotation, string(b))
}
