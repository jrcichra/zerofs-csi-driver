package main

import (
	"flag"
	"fmt"
	"os"

	"zerofs-csi-driver/pkg/csi"

	"github.com/sirupsen/logrus"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

func main() {
	enableController := flag.Bool("controller", false, "Enable the CSI Controller server")
	enableNode := flag.Bool("node", false, "Enable the CSI Node server")
	leaderElection := flag.Bool("leader-election", false, "Enable leader election for the controller manager. Enabling this will ensure there is only one active controller manager.")
	leaderElectionID := flag.String("leader-election-id", "zerofs-csi-driver", "The ID to use for leader election.")
	zerofsNamespace := flag.String("namespace", "zerofs", "Kubernetes namespace for ZeroFS pods")

	flag.Parse()

	config := ctrl.GetConfigOrDie()
	mgrOptions := ctrl.Options{
		LeaderElection:   *leaderElection,
		LeaderElectionID: *leaderElectionID,
		NewCache: func(config *rest.Config, opts cache.Options) (cache.Cache, error) {
			// Set default namespace for internal resources
			opts.DefaultNamespaces = map[string]cache.Config{
				*zerofsNamespace: {},
			}
			return cache.New(config, opts)
		},
	}

	// Create a logrus logger
	logger := logrus.New()

	// Set up controller-runtime to use the logrus logger
	logf.SetLogger(zap.New(zap.UseDevMode(true), zap.WriteTo(logger.Writer())))

	mgr, err := ctrl.NewManager(config, mgrOptions)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create manager: %v\n", err)
		os.Exit(1)
	}

	driver := csi.NewZeroFSDriver(*enableController, *enableNode, *zerofsNamespace, mgr.GetClient())

	if err := mgr.Add(driver); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to add controller: %v\n", err)
		os.Exit(1)
	}

	// Start the manager and block forever
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to start manager: %v\n", err)
		os.Exit(1)
	}
}
