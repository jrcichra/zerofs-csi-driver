package csi

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"strings"
	"text/template"
	"time"

	"github.com/sirupsen/logrus"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const ZeroFSBaseDir = "/var/lib/kubelet/plugins/zerofs.csi.driver"
const ZeroFSDefaultImage = "ghcr.io/barre/zerofs:0.16.2"
const EnvoyDefaultImage = "envoyproxy/envoy-distroless:v1.34.4"

// zerofsMounter handles actual zerofs mounting operations
type zerofsMounter struct {
	logger    *logrus.Logger
	ctx       context.Context
	client    client.Client
	namespace string
}

// NewZeroFSMounter creates a new zerofs mounter
func NewZeroFSMounter(ctx context.Context, logger *logrus.Logger, namespace string, client client.Client) *zerofsMounter {
	return &zerofsMounter{
		logger:    logger,
		ctx:       ctx,
		namespace: namespace,
		client:    client,
	}
}

// getDaemonSetUID gets the UID of the zerofs-csi-node DaemonSet
func (m *zerofsMounter) getDaemonSetUID(ctx context.Context) (types.UID, error) {
	daemonSet := &appsv1.DaemonSet{}
	err := m.client.Get(ctx, types.NamespacedName{Name: "zerofs-csi-node", Namespace: m.namespace}, daemonSet)
	if err != nil {
		return "", fmt.Errorf("failed to get DaemonSet: %w", err)
	}
	return daemonSet.UID, nil
}

// CreatePod creates a pod for a zerofs server
func (m *zerofsMounter) CreatePod(ctx context.Context, volumeID, nodeName, configMapName string) error {
	m.logger.Infof("Creating pod for volume %s", volumeID)

	var configMap corev1.ConfigMap
	if err := m.client.Get(ctx, types.NamespacedName{Namespace: m.namespace, Name: configMapName}, &configMap); err != nil {
		return fmt.Errorf("failed to get zerofs configmap: %w", err)
	}
	configMapData := configMap.Data

	// Get the secret data for the encryption password
	var encryptionPasswordSecret corev1.Secret
	if err := m.client.Get(ctx, types.NamespacedName{Namespace: m.namespace, Name: configMapData["encryptionPasswordSecretName"]}, &encryptionPasswordSecret); err != nil {
		return fmt.Errorf("failed to get zerofs password secret: %w", err)
	}
	encryptionPassword := string(encryptionPasswordSecret.Data["password"])
	if encryptionPassword == "" {
		return fmt.Errorf("failed to get zerofs password: 'password' is empty")
	}

	// Get the secret data for the aws credentials
	var awsCredentialsSecret corev1.Secret
	if err := m.client.Get(ctx, types.NamespacedName{Namespace: m.namespace, Name: configMapData["awsCredentialsSecretName"]}, &awsCredentialsSecret); err != nil {
		return fmt.Errorf("failed to get aws secret: %w", err)
	}

	awsAccessKeyID := cmp.Or(string(awsCredentialsSecret.Data["AWS_ACCESS_KEY_ID"]), string(awsCredentialsSecret.Data["username"]))
	if awsAccessKeyID == "" {
		return fmt.Errorf("failed to get aws access key id: all checked fields are empty")
	}

	awsSecretAccessKey := cmp.Or(string(awsCredentialsSecret.Data["AWS_SECRET_ACCESS_KEY"]), string(awsCredentialsSecret.Data["password"]))
	if awsSecretAccessKey == "" {
		return fmt.Errorf("failed to get aws secret access key: all checked fields are empty")
	}

	templateContent := `[cache]
dir = "{{.CacheDir}}"
disk_size_gb = {{.CacheDiskSizeGB}}
memory_size_gb = {{.CacheMemorySizeGB}}

[storage]
url = "{{.StorageURL}}"
encryption_password = "{{.EncryptionPassword}}"

[servers.ninep]
addresses = []
unix_socket = "{{.BaseDir}}/zerofs-{{.VolumeID}}.9p.sock"

[servers.nbd]
addresses = []
unix_socket = "{{.BaseDir}}/zerofs-{{.VolumeID}}.nbd.sock"

[aws]
secret_access_key = "{{.AWSSecretAccessKey}}"
access_key_id = "{{.AWSAccessKeyID}}"
default_region = "{{.AWSDefaultRegion}}"
allow_http = "{{.AWSAllowHTTP}}"
endpoint = "{{.AWSEndpoint}}"
`

	// Parse the template
	tmpl, err := template.New("zerofs").Parse(templateContent)
	if err != nil {
		return fmt.Errorf("failed to parse zerofs template: %w", err)
	}

	configValues := map[string]any{
		"VolumeID":           volumeID,
		"BaseDir":            ZeroFSBaseDir,
		"CacheDir":           cmp.Or(configMapData["cacheDir"], "/tmp"),
		"CacheDiskSizeGB":    cmp.Or(configMapData["cacheDiskSizeGB"], "1.0"),
		"CacheMemorySizeGB":  cmp.Or(configMapData["cacheMemorySizeGB"], "1.0"),
		"StorageURL":         fmt.Sprintf("%s/%s", cmp.Or(configMapData["storageURL"], "s3://zerofs"), volumeID),
		"EncryptionPassword": encryptionPassword,
		"AWSAccessKeyID":     awsAccessKeyID,
		"AWSSecretAccessKey": awsSecretAccessKey,
		"AWSDefaultRegion":   configMapData["awsDefaultRegion"],
		"AWSAllowHTTP":       configMapData["awsAllowHTTP"],
		"AWSEndpoint":        configMapData["awsEndpoint"],
	}

	// Execute the template
	configData := new(bytes.Buffer)
	err = tmpl.Execute(configData, configValues)
	if err != nil {
		return fmt.Errorf("failed to execute zerofs template: %w", err)
	}

	// Create zerofs config as a Secret
	secretName := fmt.Sprintf("zerofs-config-%s", volumeID)
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: m.namespace,
		},
		Data: map[string][]byte{
			"config.toml": configData.Bytes(),
		},
	}

	// Check if Secret already exists
	var existingSecret corev1.Secret
	if err := m.client.Get(ctx, types.NamespacedName{Namespace: m.namespace, Name: secretName}, &existingSecret); err != nil {
		// If not found, create it
		if err := m.client.Create(ctx, secret); err != nil {
			return fmt.Errorf("failed to create secret: %w", err)
		}
		m.logger.Infof("Created Secret %s for volume %s", secretName, volumeID)
	} else {
		// If found, update it
		if err := m.client.Update(ctx, secret); err != nil {
			return fmt.Errorf("failed to update secret: %w", err)
		}
		m.logger.Infof("Updated Secret %s for volume %s", secretName, volumeID)
	}

	// Create a pod manifest for the zerofs
	podName := fmt.Sprintf("zerofs-volume-%s", volumeID)

	// Get the DaemonSet UID for OwnerReference
	daemonSetUID, err := m.getDaemonSetUID(ctx)
	if err != nil {
		m.logger.Warnf("Failed to get DaemonSet UID, continuing without it: %v", err)
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: m.namespace,
			Labels: map[string]string{
				"app":    "zerofs",
				"type":   "volume",
				"volume": volumeID,
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "v1",
					Kind:       "DaemonSet",
					Name:       "zerofs-csi-node",
					UID:        daemonSetUID,
				},
			},
		},
		Spec: corev1.PodSpec{
			NodeName: nodeName,
			SecurityContext: &corev1.PodSecurityContext{
				RunAsUser:  ptr.To(int64(0)),
				RunAsGroup: ptr.To(int64(0)),
			},
			Containers: []corev1.Container{
				{
					Name:  "zerofs",
					Image: cmp.Or(configMapData["zerofsImage"], ZeroFSDefaultImage),
					Args: []string{
						"run", "--config", "/etc/zerofs/config.toml",
					},
					Env: []corev1.EnvVar{
						{
							Name:  "RUST_LOG",
							Value: "zerofs=debug,slatedb=debug",
						},
					},
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      "zerofs-config",
							MountPath: "/etc/zerofs",
							ReadOnly:  true,
						},
						{
							Name:             "plugin-dir",
							MountPath:        "/var/lib/kubelet/plugins/zerofs.csi.driver",
							MountPropagation: ptr.To(corev1.MountPropagationBidirectional),
						},
						{
							Name:             "pod-mount-dir",
							MountPath:        "/var/lib/kubelet/pods",
							MountPropagation: ptr.To(corev1.MountPropagationBidirectional),
						},
					},
					SecurityContext: &corev1.SecurityContext{
						Privileged: ptr.To(true),
					},
					ReadinessProbe: &corev1.Probe{
						ProbeHandler: corev1.ProbeHandler{
							Exec: &corev1.ExecAction{
								Command: []string{
									"sh", "-c",
									fmt.Sprintf("test -S %s/zerofs-%s.9p.sock", ZeroFSBaseDir, volumeID),
								},
							},
						},
						InitialDelaySeconds: 10,
					},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "zerofs-config",
					VolumeSource: corev1.VolumeSource{
						Secret: &corev1.SecretVolumeSource{
							SecretName: secretName,
						},
					},
				},
				{
					Name: "pod-mount-dir",
					VolumeSource: corev1.VolumeSource{
						HostPath: &corev1.HostPathVolumeSource{
							Path: "/var/lib/kubelet/pods",
							Type: ptr.To(corev1.HostPathDirectoryOrCreate),
						},
					},
				},
				{
					Name: "plugin-dir",
					VolumeSource: corev1.VolumeSource{
						HostPath: &corev1.HostPathVolumeSource{
							Path: "/var/lib/kubelet/plugins/zerofs.csi.driver",
							Type: ptr.To(corev1.HostPathDirectoryOrCreate),
						},
					},
				},
			},
		},
	}

	// Only set resource requirements if they were provided
	zerofsResourceRequirements := m.parseResourceRequirements(configMapData)
	if zerofsResourceRequirements != nil {
		pod.Spec.Containers[0].Resources = *zerofsResourceRequirements
	}

	// Add envoy container if enabled
	if strings.EqualFold(configMapData["envoyEnabled"], "true") {
		pod.Spec.InitContainers = append(pod.Spec.InitContainers, corev1.Container{
			Name:          "envoy",
			Image:         cmp.Or(configMapData["envoyImage"], EnvoyDefaultImage),
			RestartPolicy: ptr.To(corev1.ContainerRestartPolicyAlways), // https://kubernetes.io/docs/concepts/workloads/pods/sidecar-containers/
			VolumeMounts: []corev1.VolumeMount{
				{
					Name:      "envoy-config",
					MountPath: "/etc/envoy",
					ReadOnly:  true,
				},
			},
		})

		// Add envoy config volume
		pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
			Name: "envoy-config",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: cmp.Or(configMapData["envoyConfigMapRef"], "envoy-config"),
					},
				},
			},
		})
	}

	// Apply the pod
	var existingPod corev1.Pod
	if err := m.client.Get(ctx, types.NamespacedName{Namespace: m.namespace, Name: podName}, &existingPod); err != nil {
		// If not found, create it
		if err := m.client.Create(ctx, pod); err != nil {
			return fmt.Errorf("failed to create pod: %w", err)
		}
		m.logger.Infof("Created Pod %s for volume %s", podName, volumeID)
	} else {
		// If found, check if it's on the correct node
		if existingPod.Spec.NodeName == nodeName {
			// Pod is already on the correct node, skip creation
			m.logger.Infof("Pod %s already exists on correct node %s for volume %s, skipping creation", podName, nodeName, volumeID)
			return nil
		} else {
			// Pod exists but is on wrong node, delete it and create a new one
			m.logger.Infof("Pod %s found on wrong node %s, deleting and recreating on node %s", podName, existingPod.Spec.NodeName, nodeName)

			// Delete the existing pod
			if err := m.client.Delete(ctx, &existingPod); err != nil {
				return fmt.Errorf("failed to delete existing pod: %w", err)
			}

			// Wait for pod deletion to complet
			err = m.waitForPodDeletion(podName, m.namespace, 30*time.Second)
			if err != nil {
				m.logger.Warnf("Warning: failed to wait for pod deletion: %v", err)
				// Continue anyway as the pod may be in process of deletion
			}

			// Create new pod on correct node
			if err := m.client.Create(ctx, pod); err != nil {
				return fmt.Errorf("failed to create pod: %w", err)
			}
			m.logger.Infof("Created Pod %s for volume %s", podName, volumeID)
		}
	}

	// Wait for pod to be ready
	err = m.waitForPodReady(podName, m.namespace, 30*time.Second)
	if err != nil {
		return fmt.Errorf("pod failed to become ready: %w", err)
	}

	m.logger.Infof("Pod created successfully for volume %s", volumeID)
	return nil
}

// waitForPodDeletion waits for a pod to be deleted
func (m *zerofsMounter) waitForPodDeletion(podName string, namespace string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(m.ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for pod to be deleted")
		case <-ticker.C:
			var pod corev1.Pod
			if err := m.client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: podName}, &pod); err != nil {
				if strings.Contains(err.Error(), "not found") {
					m.logger.Infof("Pod %s has been deleted", podName)
					return nil
				}
				return fmt.Errorf("error checking pod status: %w", err)
			}
			m.logger.Debugf("Waiting for pod %s to be deleted...", podName)
		}
	}
}

// waitForPodReady waits for a pod to reach Ready status
func (m *zerofsMounter) waitForPodReady(podName string, namespace string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(m.ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for pod to be ready")
		case <-ticker.C:
			var pod corev1.Pod
			if err := m.client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: podName}, &pod); err != nil {
				m.logger.Warnf("Failed to get pod %s: %v", podName, err)
				continue
			}
			// Check if pod is ready
			for _, condition := range pod.Status.Conditions {
				if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
					m.logger.Infof("Pod %s is ready", podName)
					return nil
				}
			}
		}
	}
}

// RemovePod removes a pod for a zerofs server
func (m *zerofsMounter) RemovePod(ctx context.Context, volumeID string) error {
	m.logger.Infof("Removing pod for volume %s", volumeID)

	podName := fmt.Sprintf("zerofs-volume-%s", volumeID)

	// Delete the pod
	if err := m.client.Delete(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: m.namespace,
		},
	}); err != nil {
		// Check if the error is because the pod doesn't exist
		if strings.Contains(err.Error(), "not found") {
			m.logger.Infof("Pod %s does not exist, skipping deletion", podName)
		} else {
			m.logger.Warnf("Failed to delete pod %s: %v", podName, err)
			return fmt.Errorf("failed to delete pod: %w", err)
		}
	}

	// Delete the associated secret
	secretName := fmt.Sprintf("zerofs-config-%s", volumeID)
	if err := m.client.Delete(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: m.namespace,
		},
	}); err != nil {
		m.logger.Warnf("Failed to delete secret %s: %v", secretName, err)
		// Don't return error here as pod deletion was successful
		// The secret cleanup failure is not critical for the overall operation
	} else {
		m.logger.Infof("Secret removed successfully for volume %s", volumeID)
	}

	m.logger.Infof("Pod removed successfully for volume %s", volumeID)
	return nil
}

// parseResourceRequirements parses CPU and memory requests/limits from configMapData
func (m *zerofsMounter) parseResourceRequirements(configMapData map[string]string) *corev1.ResourceRequirements {
	var zerofsResourceRequirements *corev1.ResourceRequirements
	cpuRequest, cpuLimit, memoryRequest, memoryLimit := configMapData["cpuRequest"], configMapData["cpuLimit"], configMapData["memoryRequest"], configMapData["memoryLimit"]
	if cpuRequest != "" || cpuLimit != "" || memoryRequest != "" || memoryLimit != "" {
		zerofsResourceRequirements = &corev1.ResourceRequirements{}

		// Parse CPU requests and limits
		if cpuRequest != "" {
			cpuRequestQuantity, err := resource.ParseQuantity(cpuRequest)
			if err != nil {
				m.logger.Warnf("Failed to parse cpuRequest '%s': %v", cpuRequest, err)
			} else {
				if zerofsResourceRequirements.Requests == nil {
					zerofsResourceRequirements.Requests = corev1.ResourceList{}
				}
				zerofsResourceRequirements.Requests[corev1.ResourceCPU] = cpuRequestQuantity
			}
		}

		if cpuLimit != "" {
			cpuLimitQuantity, err := resource.ParseQuantity(cpuLimit)
			if err != nil {
				m.logger.Warnf("Failed to parse cpuLimit '%s': %v", cpuLimit, err)
			} else {
				if zerofsResourceRequirements.Limits == nil {
					zerofsResourceRequirements.Limits = corev1.ResourceList{}
				}
				zerofsResourceRequirements.Limits[corev1.ResourceCPU] = cpuLimitQuantity
			}
		}

		// Parse memory requests and limits
		if memoryRequest != "" {
			memoryRequestQuantity, err := resource.ParseQuantity(memoryRequest)
			if err != nil {
				m.logger.Warnf("Failed to parse memoryRequest '%s': %v", memoryRequest, err)
			} else {
				if zerofsResourceRequirements.Requests == nil {
					zerofsResourceRequirements.Requests = corev1.ResourceList{}
				}
				zerofsResourceRequirements.Requests[corev1.ResourceMemory] = memoryRequestQuantity
			}
		}

		if memoryLimit != "" {
			memoryLimitQuantity, err := resource.ParseQuantity(memoryLimit)
			if err != nil {
				m.logger.Warnf("Failed to parse memoryLimit '%s': %v", memoryLimit, err)
			} else {
				if zerofsResourceRequirements.Limits == nil {
					zerofsResourceRequirements.Limits = corev1.ResourceList{}
				}
				zerofsResourceRequirements.Limits[corev1.ResourceMemory] = memoryLimitQuantity
			}
		}
	}

	return zerofsResourceRequirements
}
