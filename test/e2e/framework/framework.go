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
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/klog/v2"
)

const (
	PollInterval = 2 * time.Second
	PollTimeout  = 120 * time.Second
)

var (
	// TestClients holds the shared clients for all tests.
	TestClients *Clients

	karmadaCtx        = "karmada-apiserver"
	karmadaKubeconfig = os.Getenv("HOME") + "/.kube/karmada.config"
	memberKubeconfig  = os.Getenv("HOME") + "/.kube/members.config"
	memberClustersCSV = "member1,member2,member3"
)

func init() {
	flag.StringVar(&karmadaCtx, "karmada-context", karmadaCtx,
		"The kubeconfig context for the Karmada API server")
	flag.StringVar(&karmadaKubeconfig, "karmada-kubeconfig", karmadaKubeconfig,
		"Path to the kubeconfig file for the Karmada control plane")
	flag.StringVar(&memberKubeconfig, "member-kubeconfig", memberKubeconfig,
		"Path to the kubeconfig file for the member clusters")
	flag.StringVar(&memberClustersCSV, "member-clusters", memberClustersCSV,
		"Comma-separated member cluster context names in member kubeconfig")
}

var _ = ginkgo.BeforeSuite(func() {
	var err error
	klog.Infof("Initializing e2e test clients...")

	TestClients, err = NewClients(
		karmadaKubeconfig,
		karmadaCtx,
		memberKubeconfig,
		parseMemberClusters(memberClustersCSV),
	)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred(), "Failed to initialize test clients")

	klog.Infof("E2E test clients initialized successfully")
})

// RandomNamespace creates a unique namespace name for test isolation.
func RandomNamespace(prefix string) string {
	return fmt.Sprintf("%s-%s", prefix, rand.String(5))
}

// CreateNamespace creates a namespace on the Karmada API server.
func CreateNamespace(name string) {
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
	}
	_, err := TestClients.KubeClient.CoreV1().Namespaces().Create(
		context.TODO(), ns, metav1.CreateOptions{})
	if errors.IsAlreadyExists(err) {
		return
	}
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred(), "Failed to create namespace %s", name)
}

// DeleteNamespace deletes a namespace on the Karmada API server.
func DeleteNamespace(name string) {
	err := TestClients.KubeClient.CoreV1().Namespaces().Delete(
		context.TODO(), name, metav1.DeleteOptions{})
	if errors.IsNotFound(err) {
		return
	}
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred(), "Failed to delete namespace %s", name)
}

func parseMemberClusters(csv string) []string {
	var clusters []string
	for _, item := range strings.Split(csv, ",") {
		name := strings.TrimSpace(item)
		if name != "" {
			clusters = append(clusters, name)
		}
	}
	if len(clusters) == 0 {
		return []string{"member1", "member2", "member3"}
	}
	return clusters
}
