/*
Copyright 2025 The Volcano Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package framework provides the e2e test framework for volcano-global.
//
// TODO:
// - Add karmada-host cluster client for verifying controller deployments
// - Add metrics collection helpers for performance testing
// - Add chaos testing utilities (pod deletion, network partition)
// - Add resource cleanup verification
// - Add parallel test execution support

package framework

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"

	karmadaclientset "github.com/karmada-io/karmada/pkg/generated/clientset/versioned"
	volcanoclientset "volcano.sh/apis/pkg/client/clientset/versioned"
)

const (
	// DefaultTimeout is the default timeout for waiting operations
	DefaultTimeout = 5 * time.Minute
	// DefaultInterval is the default polling interval
	DefaultInterval = 2 * time.Second
	// TestNamespacePrefix is the prefix for test namespaces
	TestNamespacePrefix = "volcano-global-e2e-"
)

var (
	// KarmadaConfig holds the kubeconfig for Karmada control plane
	KarmadaConfig *rest.Config
	// MemberConfigs holds the kubeconfigs for member clusters
	MemberConfigs map[string]*rest.Config

	// KarmadaClient is the Kubernetes client for Karmada API server
	KarmadaClient kubernetes.Interface
	// KarmadaKarmadaClient is the Karmada-specific client
	KarmadaKarmadaClient karmadaclientset.Interface
	// KarmadaVolcanoClient is the Volcano client for Karmada API server
	KarmadaVolcanoClient volcanoclientset.Interface

	// MemberClients holds Kubernetes clients for member clusters
	MemberClients map[string]kubernetes.Interface
	// MemberVolcanoClients holds Volcano clients for member clusters
	MemberVolcanoClients map[string]volcanoclientset.Interface

	// testNamespaces tracks created test namespaces for cleanup
	testNamespaces = make(map[string]bool)
	namespaceMutex sync.Mutex

	// initialized tracks if framework is initialized
	initialized bool
)

// Initialize sets up the test framework
func Initialize() error {
	if initialized {
		return nil
	}

	klog.Info("Initializing e2e test framework")

	// Get kubeconfig paths from environment
	karmadaKubeconfig := os.Getenv("KARMADA_KUBECONFIG")
	if karmadaKubeconfig == "" {
		karmadaKubeconfig = filepath.Join(os.Getenv("HOME"), ".kube", "karmada.config")
	}

	membersKubeconfig := os.Getenv("MEMBERS_KUBECONFIG")
	if membersKubeconfig == "" {
		membersKubeconfig = filepath.Join(os.Getenv("HOME"), ".kube", "members.config")
	}

	// Initialize Karmada clients
	var err error
	KarmadaConfig, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		&clientcmd.ClientConfigLoadingRules{ExplicitPath: karmadaKubeconfig},
		&clientcmd.ConfigOverrides{CurrentContext: "karmada-apiserver"},
	).ClientConfig()
	if err != nil {
		return fmt.Errorf("failed to load Karmada config: %w", err)
	}

	KarmadaClient, err = kubernetes.NewForConfig(KarmadaConfig)
	if err != nil {
		return fmt.Errorf("failed to create Karmada Kubernetes client: %w", err)
	}

	KarmadaKarmadaClient, err = karmadaclientset.NewForConfig(KarmadaConfig)
	if err != nil {
		return fmt.Errorf("failed to create Karmada client: %w", err)
	}

	KarmadaVolcanoClient, err = volcanoclientset.NewForConfig(KarmadaConfig)
	if err != nil {
		return fmt.Errorf("failed to create Volcano client for Karmada: %w", err)
	}

	// Initialize member cluster clients
	MemberConfigs = make(map[string]*rest.Config)
	MemberClients = make(map[string]kubernetes.Interface)
	MemberVolcanoClients = make(map[string]volcanoclientset.Interface)

	// Get number of member clusters from environment or default to 2
	numMemberClusters := 2
	if numStr := os.Getenv("NUM_MEMBER_CLUSTERS"); numStr != "" {
		if num, err := strconv.Atoi(numStr); err == nil && num > 0 {
			numMemberClusters = num
		}
	}

	for i := 1; i <= numMemberClusters; i++ {
		member := fmt.Sprintf("member%d", i)
		config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
			&clientcmd.ClientConfigLoadingRules{ExplicitPath: membersKubeconfig},
			&clientcmd.ConfigOverrides{CurrentContext: member},
		).ClientConfig()
		if err != nil {
			return fmt.Errorf("failed to load config for %s: %w", member, err)
		}
		MemberConfigs[member] = config

		kubeClient, err := kubernetes.NewForConfig(config)
		if err != nil {
			return fmt.Errorf("failed to create Kubernetes client for %s: %w", member, err)
		}
		MemberClients[member] = kubeClient

		volcanoClient, err := volcanoclientset.NewForConfig(config)
		if err != nil {
			return fmt.Errorf("failed to create Volcano client for %s: %w", member, err)
		}
		MemberVolcanoClients[member] = volcanoClient
	}

	initialized = true
	klog.Info("E2E test framework initialized successfully")
	return nil
}

// VerifyClusterConnectivity verifies that all clusters are accessible
func VerifyClusterConnectivity() error {
	ctx := context.Background()

	// Check Karmada API server connectivity
	klog.Info("Checking Karmada API server connectivity")
	_, err := KarmadaClient.CoreV1().Namespaces().List(ctx, metav1.ListOptions{Limit: 1})
	if err != nil {
		return fmt.Errorf("failed to connect to Karmada API server: %w", err)
	}

	// Check Karmada clusters
	klog.Info("Checking Karmada clusters")
	clusters, err := KarmadaKarmadaClient.ClusterV1alpha1().Clusters().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list Karmada clusters: %w", err)
	}
	klog.Infof("Found %d Karmada clusters", len(clusters.Items))

	// Check member cluster connectivity
	for member, client := range MemberClients {
		klog.Infof("Checking %s cluster connectivity", member)
		_, err := client.CoreV1().Namespaces().List(ctx, metav1.ListOptions{Limit: 1})
		if err != nil {
			return fmt.Errorf("failed to connect to %s cluster: %w", member, err)
		}
	}

	return nil
}

// Cleanup performs cleanup of test resources
func Cleanup() {
	klog.Info("Cleaning up test resources")

	ctx := context.Background()

	namespaceMutex.Lock()
	defer namespaceMutex.Unlock()

	for ns := range testNamespaces {
		klog.Infof("Deleting test namespace: %s", ns)
		err := KarmadaClient.CoreV1().Namespaces().Delete(ctx, ns, metav1.DeleteOptions{})
		if err != nil && !errors.IsNotFound(err) {
			klog.Warningf("Failed to delete namespace %s: %v", ns, err)
		}
	}

	testNamespaces = make(map[string]bool)
}

// CreateTestNamespace creates a unique namespace for a test
func CreateTestNamespace(baseName string) string {
	ctx := context.Background()
	nsName := fmt.Sprintf("%s%s-%d", TestNamespacePrefix, baseName, time.Now().UnixNano())

	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: nsName,
			Labels: map[string]string{
				"e2e-test": "true",
			},
		},
	}

	By(fmt.Sprintf("Creating test namespace: %s", nsName))
	_, err := KarmadaClient.CoreV1().Namespaces().Create(ctx, ns, metav1.CreateOptions{})
	Expect(err).NotTo(HaveOccurred(), "Failed to create test namespace")

	namespaceMutex.Lock()
	testNamespaces[nsName] = true
	namespaceMutex.Unlock()

	return nsName
}

// DeleteTestNamespace deletes a test namespace
func DeleteTestNamespace(nsName string) {
	ctx := context.Background()

	By(fmt.Sprintf("Deleting test namespace: %s", nsName))
	err := KarmadaClient.CoreV1().Namespaces().Delete(ctx, nsName, metav1.DeleteOptions{})
	if err != nil && !errors.IsNotFound(err) {
		klog.Warningf("Failed to delete namespace %s: %v", nsName, err)
	}

	namespaceMutex.Lock()
	delete(testNamespaces, nsName)
	namespaceMutex.Unlock()
}

// WaitForCondition waits for a condition to be met
func WaitForCondition(timeout time.Duration, conditionFunc func() (bool, error)) error {
	return wait.PollUntilContextTimeout(context.Background(), DefaultInterval, timeout, true, func(ctx context.Context) (bool, error) {
		return conditionFunc()
	})
}

// WaitForNamespaceReady waits for a namespace to be ready on member clusters
func WaitForNamespaceReady(nsName string, memberClusters ...string) error {
	for _, member := range memberClusters {
		client, ok := MemberClients[member]
		if !ok {
			return fmt.Errorf("member cluster %s not found", member)
		}

		err := WaitForCondition(DefaultTimeout, func() (bool, error) {
			ns, err := client.CoreV1().Namespaces().Get(context.Background(), nsName, metav1.GetOptions{})
			if err != nil {
				if errors.IsNotFound(err) {
					return false, nil
				}
				return false, err
			}
			return ns.Status.Phase == corev1.NamespaceActive, nil
		})
		if err != nil {
			return fmt.Errorf("namespace %s not ready on %s: %w", nsName, member, err)
		}
	}
	return nil
}

// WaitForDeploymentReady waits for a deployment to be ready
func WaitForDeploymentReady(namespace, deploymentName string, timeout time.Duration) error {
	By(fmt.Sprintf("Waiting for deployment %s/%s to be ready", namespace, deploymentName))

	return WaitForCondition(timeout, func() (bool, error) {
		dep, err := KarmadaClient.AppsV1().Deployments(namespace).Get(context.Background(), deploymentName, metav1.GetOptions{})
		if err != nil {
			if errors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
		// Check if deployment is ready
		if dep.Status.ReadyReplicas == *dep.Spec.Replicas && dep.Status.ReadyReplicas > 0 {
			return true, nil
		}
		return false, nil
	})
}
