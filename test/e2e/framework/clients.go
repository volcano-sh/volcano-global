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

package framework

import (
	"fmt"

	karmadaclientset "github.com/karmada-io/karmada/pkg/generated/clientset/versioned"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	volcanoclient "volcano.sh/apis/pkg/client/clientset/versioned"
)

// Clients holds all the Kubernetes clients needed for e2e tests.
type Clients struct {
	// KarmadaClient is the clientset for the Karmada API server (ResourceBinding, PropagationPolicy, etc.)
	KarmadaClient karmadaclientset.Interface

	// KubeClient is the Kubernetes clientset connected to the Karmada API server
	KubeClient kubernetes.Interface

	// VolcanoClient is the Volcano clientset connected to the Karmada API server (VCJob, Queue, HyperJob)
	VolcanoClient volcanoclient.Interface

	// HostKubeClient is the Kubernetes clientset connected to the Karmada host cluster
	HostKubeClient kubernetes.Interface

	// MemberClients maps member cluster names to their Kubernetes clientsets
	MemberClients map[string]kubernetes.Interface

	// MemberVolcanoClients maps member cluster names to their Volcano clientsets
	MemberVolcanoClients map[string]volcanoclient.Interface
}

// NewClients creates all required clients for e2e testing.
func NewClients(karmadaKubeconfig, karmadaContext, memberKubeconfig string) (*Clients, error) {
	karmadaConfig, err := buildConfig(karmadaKubeconfig, karmadaContext)
	if err != nil {
		return nil, fmt.Errorf("failed to build karmada config: %v", err)
	}

	hostConfig, err := buildConfig(karmadaKubeconfig, "karmada-host")
	if err != nil {
		return nil, fmt.Errorf("failed to build karmada-host config: %v", err)
	}

	kubeClient, err := kubernetes.NewForConfig(karmadaConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create kube client: %v", err)
	}

	karmadaClient, err := karmadaclientset.NewForConfig(karmadaConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create karmada client: %v", err)
	}

	volcanoClient, err := volcanoclient.NewForConfig(karmadaConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create volcano client: %v", err)
	}

	hostKubeClient, err := kubernetes.NewForConfig(hostConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create host kube client: %v", err)
	}

	memberClients := make(map[string]kubernetes.Interface)
	memberVolcanoClients := make(map[string]volcanoclient.Interface)

	memberClusters := []string{"member1", "member2", "member3"}
	for _, cluster := range memberClusters {
		memberConfig, err := buildConfig(memberKubeconfig, cluster)
		if err != nil {
			return nil, fmt.Errorf("failed to build config for %s: %v", cluster, err)
		}

		mc, err := kubernetes.NewForConfig(memberConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create kube client for %s: %v", cluster, err)
		}
		memberClients[cluster] = mc

		mvc, err := volcanoclient.NewForConfig(memberConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create volcano client for %s: %v", cluster, err)
		}
		memberVolcanoClients[cluster] = mvc
	}

	return &Clients{
		KarmadaClient:        karmadaClient,
		KubeClient:           kubeClient,
		VolcanoClient:        volcanoClient,
		HostKubeClient:       hostKubeClient,
		MemberClients:        memberClients,
		MemberVolcanoClients: memberVolcanoClients,
	}, nil
}

func buildConfig(kubeconfig, context string) (*rest.Config, error) {
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		&clientcmd.ClientConfigLoadingRules{ExplicitPath: kubeconfig},
		&clientcmd.ConfigOverrides{CurrentContext: context},
	).ClientConfig()
}
