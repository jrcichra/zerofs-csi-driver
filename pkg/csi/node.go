package csi

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type nodeService struct {
	csi.UnimplementedNodeServer
	name    string
	nodeID  string
	logger  *logrus.Logger
	mounter *zerofsMounter
}

// NodeStageVolume implements NodeServer
func (ns *nodeService) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	ns.logger.Infof("NodeStageVolume called: %s", req.VolumeId)

	// Validate request
	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID cannot be empty")
	}

	if req.StagingTargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "staging target path cannot be empty")
	}

	return &csi.NodeStageVolumeResponse{}, nil
}

// NodeUnstageVolume implements NodeServer
func (ns *nodeService) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	ns.logger.Infof("NodeUnstageVolume called: %s", req.VolumeId)

	// Validate request
	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID cannot be empty")
	}

	if req.StagingTargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "staging target path cannot be empty")
	}

	return &csi.NodeUnstageVolumeResponse{}, nil
}

// NodePublishVolume implements NodeServer
func (ns *nodeService) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	ns.logger.Infof("NodePublishVolume called: %s", req.VolumeId)

	// Validate request
	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID cannot be empty")
	}

	if req.TargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "target path cannot be empty")
	}

	if req.VolumeCapability == nil {
		return nil, status.Error(codes.InvalidArgument, "volume capability cannot be empty")
	}

	nodeName := ns.nodeID

	// Get volume information from publish context

	volumeId := ""
	if val, exists := req.PublishContext["volumeId"]; exists {
		volumeId = val
	}

	filesystem, exists := req.VolumeContext["filesystem"]
	if !exists {
		ns.logger.Errorf("Failed to determine filesystem for volume, ensure filesystem is specified")
		return nil, status.Error(codes.InvalidArgument, "failed to determine filesystem for volume, ensure filesystem is specified")
	}

	ns.logger.Infof("Mounting volume %s to %s on node %s", volumeId, req.TargetPath, nodeName)

	// Create the target directory if it doesn't exist
	if err := os.MkdirAll(req.TargetPath, 0755); err != nil {
		ns.logger.Errorf("Failed to create target directory %s: %v", req.TargetPath, err)
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to create target directory: %v", err))
	}

	configMapName, exists := req.VolumeContext["configMapName"]
	if !exists {
		ns.logger.Errorf("Failed to determine configMapName for volume, ensure configMapName is specified")
		return nil, status.Error(codes.InvalidArgument, "failed to determine configMapName for volume, ensure configMapName is specified")
	}

	// Create pod for the zerofs server
	if err := ns.mounter.CreatePod(ctx, volumeId, nodeName, configMapName); err != nil {
		ns.logger.Errorf("Failed to create pod for volume %s: %v", volumeId, err)
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to create pod: %v", err))
	}

	socketPath := filepath.Join(ZeroFSBaseDir, fmt.Sprintf("zerofs-%s.9p.sock", volumeId))
	nbdSocketPath := filepath.Join(ZeroFSBaseDir, fmt.Sprintf("zerofs-%s.nbd.sock", volumeId))

	// Add a retry loop to check for socket file existence before attempting mount
	// This handles race conditions where the service might not have fully initialized
	maxRetries := 6
	retryDelay := 10 * time.Second

	ns.logger.Infof("Checking if socket file exists before mounting: %s", socketPath)

	for attempt := 0; attempt < maxRetries; attempt++ {
		if _, err := os.Stat(socketPath); err == nil {
			// Socket file exists, proceed with mount
			break
		} else if os.IsNotExist(err) {
			ns.logger.Warnf("Socket file does not exist (attempt %d/%d): %s", attempt+1, maxRetries, socketPath)
			if attempt < maxRetries-1 { // Don't sleep on the last attempt
				time.Sleep(retryDelay)
				continue
			}
		} else {
			// Some other error occurred when checking file
			ns.logger.Errorf("Error checking socket file: %v", err)
			return nil, status.Error(codes.Internal, fmt.Sprintf("error checking socket file: %v", err))
		}

		// If we reach here, it means we've exhausted retries and socket still doesn't exist
		ns.logger.Errorf("Socket file still does not exist after %d attempts: %s", maxRetries, socketPath)

		return nil, status.Error(codes.Internal, fmt.Sprintf("socket file does not exist after %d attempts: %s", maxRetries, socketPath))
	}

	cmd := exec.Command("mount", "-t", "9p", "-o", "trans=unix,version=9p2000.L", socketPath, req.TargetPath)

	// Check if the target path is already mounted
	checkCmd := exec.Command("mountpoint", "-d", req.TargetPath)
	if err := checkCmd.Run(); err == nil {
		ns.logger.Infof("Volume %s is already mounted at %s, skipping mount", volumeId, req.TargetPath)
	} else {
		output, err := cmd.CombinedOutput()
		if err != nil {
			ns.logger.Errorf("Failed to mount volume %s to %s: %v, output: %s", volumeId, req.TargetPath, err, string(output))
			return nil, status.Error(codes.Internal, fmt.Sprintf("failed to mount volume: %v", err))
		}
		ns.logger.Infof("Volume %s mounted successfully using 9p protocol", volumeId)
	}

	// we need to make an nbd volume with the filesystem provided
	ns.logger.Infof("Creating NBD volume with filesystem: %s", filesystem)

	// if the disk file doesn't exist, create it
	diskFilePath := filepath.Join(req.TargetPath, ".nbd", "disk")
	if _, err := os.Stat(diskFilePath); os.IsNotExist(err) {
		ns.logger.Infof("Creating disk file at %s", diskFilePath)

		// Get volume capacity from context
		capacityBytes, exists := req.VolumeContext["capacity_bytes"]

		if !exists {
			ns.logger.Errorf("No capacity information found in volume context")
			return nil, status.Error(codes.InvalidArgument, "no capacity information provided for volume")
		}

		ns.logger.Infof("Creating disk file with size: %s", capacityBytes)
		cmd := exec.Command("truncate", "-s", capacityBytes, diskFilePath)
		output, err := cmd.CombinedOutput()
		if err != nil {
			ns.logger.Errorf("Failed to create disk file %s: %v, output: %s", diskFilePath, err, string(output))
			return nil, status.Error(codes.Internal, fmt.Sprintf("failed to create disk file: %v", err))
		}

		ns.logger.Infof("Successfully created disk file at %s with size %s", diskFilePath, capacityBytes)
	} else if err != nil {
		ns.logger.Errorf("Error checking disk file %s: %v", diskFilePath, err)
		return nil, status.Error(codes.Internal, fmt.Sprintf("error checking disk file: %v", err))
	} else {
		ns.logger.Infof("Disk file %s already exists", diskFilePath)
	}

	// we no longer need the 9p filesystem, as we have a disk we can mount via nbd instead
	cmd = exec.Command("umount", req.TargetPath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		ns.logger.Errorf("Failed to unmount 9p volume %s from %s: %v, output: %s", req.VolumeId, req.TargetPath, err, string(output))
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to unmount 9p volume: %v", err))
	}

	// Connect to the NBD server using nbd-client
	cmd = exec.Command("nbd-client", "-unix", nbdSocketPath, "-N", "disk", "-persist", "-timeout", "0", "-connections", "4")
	output, err = cmd.CombinedOutput()
	if err != nil {
		ns.logger.Errorf("Failed to connect to NBD device: %v output: %s", err, string(output))
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to connect to NBD device: %v output: %s", err, string(output)))
	}

	// parse out the device name we were assigned (/dev/nbd*)
	nbdDevicePath, err := parseNBDDevice(string(output))
	if err != nil {
		ns.logger.Errorf("Failed to discover NBD device name: %v", err)
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to discover NBD device name: %v", err))
	}

	// Check if the device already has a filesystem
	ns.logger.Infof("Checking if NBD device %s already has a filesystem", nbdDevicePath)
	cmd = exec.Command("blkid", "-o", "value", "-s", "TYPE", nbdDevicePath)
	output, _ = cmd.CombinedOutput()
	// Check if the output is empty (no filesystem found)
	fsType := strings.TrimSpace(string(output))
	if fsType != "" {
		ns.logger.Infof("NBD device %s already has filesystem of type: %s", nbdDevicePath, fsType)
		// Device already has a filesystem, skip formatting
		ns.logger.Infof("Skipping mkfs as filesystem already exists on %s", nbdDevicePath)
	} else {
		// No filesystem found, proceed with formatting
		ns.logger.Infof("Formatting NBD device %s with filesystem: %s", nbdDevicePath, filesystem)

		// Use mkfs to format the device
		cmd = exec.Command("mkfs", "-t", filesystem, nbdDevicePath)
		output, err = cmd.CombinedOutput()
		if err != nil {
			ns.logger.Errorf("Failed to format NBD device %s with filesystem %s: %v, output: %s",
				nbdDevicePath, filesystem, err, string(output))
			return nil, status.Error(codes.Internal, fmt.Sprintf("failed to format NBD device: %v", err))
		}
		ns.logger.Infof("Successfully formatted NBD device %s with filesystem %s", nbdDevicePath, filesystem)

	}

	// Mount the NBD device to the target path
	ns.logger.Infof("Mounting NBD device %s to %s", nbdDevicePath, req.TargetPath)

	mountOptions := req.GetVolumeCapability().GetMount().GetMountFlags()

	// Create mount command for NBD device with specified filesystem and options
	var mountArgs []string
	mountArgs = append(mountArgs, "-t", filesystem)

	// Add mount options if they exist
	if len(mountOptions) > 0 {
		// Join the options into a single comma-separated string
		optionsString := strings.Join(mountOptions, ",")
		mountArgs = append(mountArgs, "-o", optionsString)
	}

	mountArgs = append(mountArgs, nbdDevicePath, req.TargetPath)

	cmd = exec.Command("mount", mountArgs...)
	output, err = cmd.CombinedOutput()
	if err != nil {
		ns.logger.Errorf("Failed to mount NBD device %s to %s: %v, output: %s", nbdDevicePath, req.TargetPath, err, string(output))
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to mount NBD device: %v", err))
	}

	ns.logger.Infof("Volume %s mounted successfully using NBD device %s", volumeId, nbdDevicePath)

	return &csi.NodePublishVolumeResponse{}, nil
}

// NodeUnpublishVolume implements NodeServer
func (ns *nodeService) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	ns.logger.Infof("NodeUnpublishVolume called: %s", req.VolumeId)

	// Validate request
	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID cannot be empty")
	}

	if req.TargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "target path cannot be empty")
	}

	// Determine which nbd block device this volume is using so we can detach it after unmounting
	cmd := exec.Command("findmnt", "-n", "-o", "SOURCE", req.TargetPath)
	nbdDevice, err := cmd.CombinedOutput()
	if err != nil {
		ns.logger.Warnf("Failed to find volume mount for %s from %s: %v, output: %s", req.VolumeId, req.TargetPath, err, string(nbdDevice))
	}

	// Unmount the filesystem
	ns.logger.Infof("Unmounting volume %s from %s", req.VolumeId, req.TargetPath)
	cmd = exec.Command("umount", req.TargetPath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		ns.logger.Warnf("Failed to unmount volume %s from %s: %v, output: %s", req.VolumeId, req.TargetPath, err, string(output))
	}

	// Remove the nbd-client device
	ns.logger.Infof("Detaching volume %s from %s", req.VolumeId, nbdDevice)
	cmd = exec.Command("nbd-client", "-d", strings.TrimSpace(string(nbdDevice)))
	_, err = cmd.CombinedOutput()
	if err != nil {
		ns.logger.Warnf("Failed to detach nbd for volume %s: %v", req.VolumeId, err)
	}

	// Remove pod for the zerofs server
	if err := ns.mounter.RemovePod(ctx, req.VolumeId); err != nil {
		ns.logger.Warnf("Failed to remove pod for volume %s: %v", req.VolumeId, err)
		// Continue with cleanup even if pod removal fails
	}

	// Wait for pod deletion to complete before returning
	podName := fmt.Sprintf("zerofs-volume-%s", req.VolumeId)
	ns.logger.Infof("Waiting for pod %s to be fully deleted", podName)
	if err := ns.mounter.waitForPodDeletion(podName, ns.mounter.namespace, 30*time.Second); err != nil {
		ns.logger.Warnf("Warning: failed to wait for pod deletion: %v", err)
		// Continue with response even if we couldn't verify deletion
	}

	ns.logger.Infof("Volume %s unpublished successfully", req.VolumeId)
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

// NodeGetVolumeStats implements NodeServer
func (ns *nodeService) NodeGetVolumeStats(ctx context.Context, req *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	ns.logger.Infof("NodeGetVolumeStats called: %s", req.VolumeId)

	// Validate request
	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID cannot be empty")
	}

	if req.VolumePath == "" {
		return nil, status.Error(codes.InvalidArgument, "volume path cannot be empty")
	}

	// Get filesystem stats for the volume path
	stat, err := os.Stat(req.VolumePath)
	if err != nil {
		ns.logger.Errorf("Failed to get stats for %s: %v", req.VolumePath, err)
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to get volume stats: %v", err))
	}

	ns.logger.Infof("Volume %s stats retrieved. Size: %d bytes", req.VolumeId, stat.Size())

	return &csi.NodeGetVolumeStatsResponse{
		Usage: []*csi.VolumeUsage{
			{
				Unit:  csi.VolumeUsage_UNKNOWN,
				Total: stat.Size(),
				Used:  stat.Size(),
			},
		},
	}, nil
}

// NodeGetCapabilities implements NodeServer
func (ns *nodeService) NodeGetCapabilities(ctx context.Context, req *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	ns.logger.Infof("NodeGetCapabilities called")

	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: []*csi.NodeServiceCapability{
			{
				Type: &csi.NodeServiceCapability_Rpc{
					Rpc: &csi.NodeServiceCapability_RPC{
						Type: csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
					},
				},
			},
			{
				Type: &csi.NodeServiceCapability_Rpc{
					Rpc: &csi.NodeServiceCapability_RPC{
						Type: csi.NodeServiceCapability_RPC_GET_VOLUME_STATS,
					},
				},
			},
		},
	}, nil
}

// parseNBDDevice extracts the NBD device path from nbd-client output
func parseNBDDevice(output string) (string, error) {
	re := regexp.MustCompile(`Connected (/dev/nbd\d+)`)
	if m := re.FindStringSubmatch(output); len(m) > 1 {
		return m[1], nil
	}
	return "", fmt.Errorf("could not find nbd device")
}

// NodeGetInfo implements NodeServer
func (ns *nodeService) NodeGetInfo(ctx context.Context, req *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	ns.logger.Infof("NodeGetInfo called for node: %s", ns.nodeID)

	return &csi.NodeGetInfoResponse{
		NodeId:            ns.nodeID,
		MaxVolumesPerNode: 0,
	}, nil
}
