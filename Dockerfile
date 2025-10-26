FROM alpine:3.22.2
ARG TARGETARCH
RUN apk add --no-cache ca-certificates util-linux nbd-client coreutils e2fsprogs xfsprogs btrfs-progs zfs
COPY out/zerofs-csi-driver-linux-${TARGETARCH} /app/zerofs-csi-driver
ENTRYPOINT ["/app/zerofs-csi-driver"]
