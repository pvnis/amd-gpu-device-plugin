FROM scratch
COPY amd-gpu-device-plugin /amd-gpu-device-plugin
ENTRYPOINT ["/amd-gpu-device-plugin"]
