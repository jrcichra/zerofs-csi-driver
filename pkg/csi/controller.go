package csi

import (
	"context"
	"fmt"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/sirupsen/logrus"
)

type controllerService struct {
	csi.UnimplementedControllerServer
	name    string
	nodeID  string
	logger  *logrus.Logger
	mounter *zerofsMounter
}

// CreateVolume implements ControllerServer
func (cs *controllerService) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	cs.logger.Infof("CreateVolume called: %s", req.Name)

	// Validate request
	if req.Name == "" {
		return nil, fmt.Errorf("volume name cannot be empty")
	}

	if req.VolumeCapabilities == nil {
		return nil, fmt.Errorf("volume capabilities cannot be empty")
	}

	// Create volume specification
	volume := &csi.Volume{
		VolumeId:      req.Name,
		CapacityBytes: req.CapacityRange.GetRequiredBytes(),
		VolumeContext: req.Parameters,
	}

	// Add capacity information to volume context for node service
	if volume.VolumeContext == nil {
		volume.VolumeContext = make(map[string]string)
	}
	volume.VolumeContext["capacity_bytes"] = fmt.Sprintf("%d", req.CapacityRange.GetRequiredBytes())

	return &csi.CreateVolumeResponse{
		Volume: volume,
	}, nil
}

// DeleteVolume implements ControllerServer
func (cs *controllerService) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	cs.logger.Infof("DeleteVolume called: %s", req.VolumeId)
	// Validate request
	if req.VolumeId == "" {
		return nil, fmt.Errorf("volume ID cannot be empty")
	}
	return &csi.DeleteVolumeResponse{}, nil
}

// ControllerPublishVolume implements ControllerServer
func (cs *controllerService) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	cs.logger.Infof("ControllerPublishVolume called: %s", req.VolumeId)

	// Validate request
	if req.VolumeId == "" {
		return nil, fmt.Errorf("volume ID cannot be empty")
	}

	if req.NodeId == "" {
		return nil, fmt.Errorf("node ID cannot be empty")
	}

	return &csi.ControllerPublishVolumeResponse{
		PublishContext: map[string]string{
			"nodeName": req.NodeId,
			"volumeId": req.VolumeId,
		},
	}, nil
}

// ControllerUnpublishVolume implements ControllerServer
func (cs *controllerService) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	cs.logger.Infof("ControllerUnpublishVolume called: %s", req.VolumeId)

	// Validate request
	if req.VolumeId == "" {
		return nil, fmt.Errorf("volume ID cannot be empty")
	}

	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

// ValidateVolumeCapabilities implements ControllerServer
func (cs *controllerService) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	cs.logger.Infof("ValidateVolumeCapabilities called: %s", req.VolumeId)

	// Validate request
	if req.VolumeId == "" {
		return nil, fmt.Errorf("volume ID cannot be empty")
	}

	if req.VolumeCapabilities == nil {
		return nil, fmt.Errorf("volume capabilities cannot be empty")
	}

	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
			VolumeCapabilities: req.VolumeCapabilities,
		},
	}, nil
}

// ListVolumes implements ControllerServer
func (cs *controllerService) ListVolumes(ctx context.Context, req *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	cs.logger.Infof("ListVolumes called")

	return &csi.ListVolumesResponse{
		Entries: []*csi.ListVolumesResponse_Entry{},
	}, nil
}

// ControllerGetCapabilities implements ControllerServer
func (cs *controllerService) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	cs.logger.Infof("ControllerGetCapabilities called")

	return &csi.ControllerGetCapabilitiesResponse{
		Capabilities: []*csi.ControllerServiceCapability{
			{
				Type: &csi.ControllerServiceCapability_Rpc{
					Rpc: &csi.ControllerServiceCapability_RPC{
						Type: csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
					},
				},
			},
			{
				Type: &csi.ControllerServiceCapability_Rpc{
					Rpc: &csi.ControllerServiceCapability_RPC{
						Type: csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
					},
				},
			},
			{
				Type: &csi.ControllerServiceCapability_Rpc{
					Rpc: &csi.ControllerServiceCapability_RPC{
						Type: csi.ControllerServiceCapability_RPC_LIST_VOLUMES,
					},
				},
			},
		},
	}, nil
}

// CreateSnapshot implements ControllerServer
func (cs *controllerService) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	cs.logger.Infof("CreateSnapshot called")

	return &csi.CreateSnapshotResponse{
		Snapshot: &csi.Snapshot{
			SnapshotId:     "snapshot-" + req.SourceVolumeId,
			SourceVolumeId: req.SourceVolumeId,
			CreationTime:   nil,
			ReadyToUse:     true,
		},
	}, nil
}

// DeleteSnapshot implements ControllerServer
func (cs *controllerService) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	cs.logger.Infof("DeleteSnapshot called")

	return &csi.DeleteSnapshotResponse{}, nil
}
