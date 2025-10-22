# ZeroFS CSI Driver

A Container Storage Interface (CSI) driver for [ZeroFS](https://www.zerofs.net/), allowing Kubernetes block devices to run on top of S3.

ZeroFS does all the work translating I/O to S3/SlateDB. This CSI driver is only managing the lifecycle of the ZeroFS processes, data, and mounts.

## Notes

This software is under heavy active development. Expect breaking changes. Do not store data in here that isn't backed up elsewhere. I'll do my best to tag releases at stable commits that I'm using and release migration notes when necessary.

## Requirements

- S3-compatible endpoint (e.g. Minio or AWS S3)
- Kubernetes cluster with CSI support

## Features

- Infinitely scalable block storage on Kubernetes
- Specify any filesystem (`ext4`,`xfs`,`btrfs`)
- CPU and memory resource quotas for systemd services
- Optional [Envoy](https://www.envoyproxy.io/) sidecar (off by default) - if your S3 provider is very flakey introducing Envoy with some larger timeouts / retries might help (note that adding retries breaks fencing guarantees)

See the [ZeroFS docs](https://www.zerofs.net) for more features in ZeroFS.

## Future Work

- VolumeSnapshots on supported filesystems (`btrfs` likely first, `zfs` second) - in the future this may be implemented in ZeroFS at the 9P layer
- 9P support (run natively on 9P instead of a block device, if desirable)
- Volume Expansion (ZeroFS itself [does not support this](https://www.zerofs.net/nbd-devices#managing-device-files)) but we might be able to do offline volume expansion (worst case copying all the data to a new, bigger file)
- ReadWriteMany - while possible, this may not be desirable / useful today as the ZeroFS process would run as a single pod on one system. If that pod goes down all the clients are affected. There might be a way to make it HA with fencing and/or leader election in the future
- Delete data from PVCs - right now deleting a PVC does not delete the data from S3. You will need to perform this action manually. Once there's sufficient test coverage I will revisit this
- Leader election - was having trouble getting it working alongside the various CSI sidecar pods

## Current Bugs

- NBD exhaustion - I've noticed at times `zerofs-csi-driver` can get in a loop and exhaust all the NBD mounts on a system. The code needs to be improved to handle this / be more idempotent
- Long terminating zerofs pvc pods - something must not be respecting `SIGTERM` or possibly taking too long, I haven't checked yet
- With HA Minio clusters I am seeing ZeroFS pods restart several times due to fencing errors. This is either a bug in ZeroFS, Minio, or my configuration

## Architecture

When a pod attaches a ZeroFS PVC, it will spawn a new pod in the `zerofs` namespace on the same node as the workload. This new ZeroFS pod will run a copy of ZeroFS with a `Secret` holding the dynamically generated `config.toml`. It is configured to create sockets for `9p` and `nbd`.

Once ZeroFS creates the sockets, `zerofs-csi-driver` will mount the `9p` filesystem and create a sparse file in the `.nbd` directory of the requested volume size. Once this is complete, the `9p` filesystem will be unmounted. Today the CSI driver only ever makes ZeroFS filesystems with one file, an NBD disk called `disk`.

Next, `zerofs-csi-driver` uses `nbd-client` to attach the block device to the system via the `.nbd.sock` file. Once this completes, it will check if there is a filesystem on the device. If not, it will use `mkfs.$filesystem`.

Once a filesystem has been established, it is mounted with `mount -t $filesystem -o $mountOptions /path/to/kubelet/mount` and then control passes over to the workload pod.

On a volume detachment, we do the same steps in reverse: unmount the filesystem, disconnect the nbd device, delete the ZeroFS pod and `config.toml` `Secret`.

The S3 data for deleted PVCs is not deleted at this time.

## Usage

First define a `ConfigMap` with your ZeroFS settings. These settings map to fields in the ZeroFS `config.toml`. If I'm missing any or doing it in a way that doesn't work with your storage provider feel free to open a PR or issue.

This was done since `parameters` on `StorageClasses` are immutible. You "could" delete the `StorageClass` each time The location / password of your volume could change over time.

The [VolumeAttributesClass](https://kubernetes.io/docs/concepts/storage/volume-attributes-classes/) might be a better implementation of this in the future.

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: zerofs-btrfs-config
  namespace: zerofs
data:
  awsAllowHTTP: "true"
  awsCredentialsSecretName: minio-zerofs-credentials
  awsDefaultRegion: us-east-1
  awsEndpoint: http://minio.minio:9000
  encryptionPasswordSecretName: zerofs-encryption-password
```

When making a `StorageClass`, specify a `filesystem`, `mountOptions`, and refer to this `ConfigMap` with the key `configMapName`. `zerofs-csi-driver` will read it during `NodePublishVolume`:

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: zerofs-btrfs
provisioner: zerofs.csi.driver
parameters:
  filesystem: btrfs
  configMapName: zerofs-btrfs-config
mountOptions: compress-force=zstd # compress-force runs zstd compression regardless of btrfs heuristic results
```

The filesystem will be passed to `mkfs` on a fresh volume and the `mountOptions` will be appended onto the `mount` command (after `-o`).

You can specify any filesystem that supports `mkfs`. Support for other filesystems like ZFS will come with time.

Create a PVC using this new `StorageClass`:

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: my-pvc
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 10Gi
  storageClassName: zerofs-btrfs
```

Attach it to a pod to use it. See the `zerofs` namespace and `events` for troubleshooting.
