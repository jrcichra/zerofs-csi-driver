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
	// Enable leader election by default as per the plan.
	leaderElection := flag.Bool("leader-election", true, "Enable leader election for the controller manager. Enabling this will ensure there is only one active controller manager.")
	// Use a more specific leader election ID as per the plan and feedback.
	leaderElectionID := flag.String("leader-election-id", "zerofs-csi-driver-leader-election", "The ID to use for leader election.")
	zerofsNamespace := flag.String("namespace", "zerofs", "Kubernetes namespace for ZeroFS pods")

	// Add a flag for leader election namespace, defaulting to POD_NAMESPACE env var.
	// This makes it configurable while still respecting common Kubernetes deployment patterns.
	var leaderElectionNamespace string
	flag.StringVar(&leaderElectionNamespace, "leader-election-namespace", os.Getenv("POD_NAMESPACE"),
		"The namespace to use for leader election. Defaults to the POD_NAMESPACE environment variable if set.")

	flag.Parse()

	// Robustness check: If leader election is enabled, the namespace must be set.
	// This addresses the feedback regarding panicking if POD_NAMESPACE is empty.
	if *leaderElection && leaderElectionNamespace == "" {
		fmt.Fprintf(os.Stderr, "Error: leader election is enabled but 'leader-election-namespace' is not set and POD_NAMESPACE environment variable is empty. "+
			"Leader election requires a namespace to create lease locks. Please set one of them.\n")
		os.Exit(1)
	}

	config := ctrl.GetConfigOrDie()
	mgrOptions := ctrl.Options{
		LeaderElection:          *leaderElection,
		LeaderElectionID:        *leaderElectionID,
		// Assign the determined leader election namespace.
		LeaderElectionNamespace: leaderElectionNamespace,
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