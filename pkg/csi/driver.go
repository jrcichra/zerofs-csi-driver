package csi

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	driverName = "zerofs.csi.driver"
	version    = "0.1.0"
)

type zeroFSDriver struct {
	name             string
	version          string
	nodeID           string
	enableController bool
	enableNode       bool
	namespace        string
	mounter          *zerofsMounter

	client client.Client

	// gRPC server
	server *grpc.Server

	// Logging
	logger *logrus.Logger
}

// NewZeroFSDriver creates a new CSI driver instance
func NewZeroFSDriver(enableController, enableNode bool, namespace string, client client.Client) *zeroFSDriver {
	return &zeroFSDriver{
		name:             driverName,
		version:          version,
		logger:           logrus.New(),
		enableController: enableController,
		enableNode:       enableNode,
		namespace:        namespace,
		client:           client,
	}
}

func (d *zeroFSDriver) Start(ctx context.Context) error {
	d.logger.Infof("Starting %s driver version %s", d.name, d.version)

	// Get node ID
	d.nodeID = os.Getenv("NODE_ID")

	// Create gRPC server with custom options
	serverOptions := []grpc.ServerOption{
		grpc.ConnectionTimeout(30 * time.Second),
	}
	d.server = grpc.NewServer(serverOptions...)

	d.mounter = NewZeroFSMounter(ctx, d.logger, d.namespace, d.client)

	// Register CSI services - use embedded unimplemented servers for proper interface compliance
	csi.RegisterIdentityServer(d.server, &identityServer{})

	if d.enableController {
		d.logger.Infof("Registering Controller service")
		csi.RegisterControllerServer(d.server, &controllerService{name: driverName, nodeID: d.nodeID, logger: d.logger, mounter: d.mounter})
	} else {
		d.logger.Infof("Controller service disabled")
	}

	if d.enableNode {
		d.logger.Infof("Registering Node service")
		csi.RegisterNodeServer(d.server, &nodeService{name: driverName, nodeID: d.nodeID, logger: d.logger, mounter: d.mounter})
	} else {
		d.logger.Infof("Node service disabled")
	}

	// Start the server
	socketPath := filepath.Join("/var/lib/kubelet/plugins", driverName, "csi.sock")
	if err := os.MkdirAll(filepath.Dir(socketPath), 0755); err != nil {
		return fmt.Errorf("failed to create directory for socket: %w", err)
	}

	// Remove existing socket file
	if err := os.Remove(socketPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove existing socket: %w", err)
	}

	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		return fmt.Errorf("failed to listen on socket: %w", err)
	}

	d.logger.Infof("Starting gRPC server on %s", socketPath)

	go func() {
		if err := d.server.Serve(listener); err != nil {
			d.logger.Errorf("Failed to serve: %v", err)
		}
	}()

	// Wait for context cancellation or shutdown signal
	<-ctx.Done()
	d.logger.Infof("Shutdown signal received, stopping driver...")

	// Graceful shutdown
	d.Stop()

	return ctx.Err()
}

// Stop stops the CSI driver
func (d *zeroFSDriver) Stop() {
	if d.server != nil {
		d.server.GracefulStop()
	}
}

// identityServer implements IdentityServer with embedded unimplemented server
type identityServer struct {
	csi.UnimplementedIdentityServer
}

// GetPluginInfo implements IdentityServer
func (d *identityServer) GetPluginInfo(ctx context.Context, req *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{
		Name:          driverName,
		VendorVersion: version,
	}, nil
}

// GetPluginCapabilities implements IdentityServer
func (d *identityServer) GetPluginCapabilities(ctx context.Context, req *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	return &csi.GetPluginCapabilitiesResponse{
		Capabilities: []*csi.PluginCapability{
			{
				Type: &csi.PluginCapability_Service_{
					Service: &csi.PluginCapability_Service{
						Type: csi.PluginCapability_Service_CONTROLLER_SERVICE,
					},
				},
			},
			{
				Type: &csi.PluginCapability_Service_{
					Service: &csi.PluginCapability_Service{
						Type: csi.PluginCapability_Service_VOLUME_ACCESSIBILITY_CONSTRAINTS,
					},
				},
			},
		},
	}, nil
}

// Probe implements IdentityServer
func (d *identityServer) Probe(ctx context.Context, req *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	return &csi.ProbeResponse{
		Ready: wrapperspb.Bool(true),
	}, nil
}
