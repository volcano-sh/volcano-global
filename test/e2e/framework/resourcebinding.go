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

	"github.com/onsi/gomega"
	workv1alpha2 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// WaitForResourceBindingSuspended waits until a ResourceBinding has spec.suspension.scheduling=true.
func WaitForResourceBindingSuspended(namespace, name string) {
	gomega.Eventually(func() bool {
		rb, err := TestClients.KarmadaClient.WorkV1alpha2().ResourceBindings(namespace).Get(
			context.TODO(), name, metav1.GetOptions{})
		if err != nil {
			return false
		}
		return isSchedulingSuspended(rb)
	}, PollTimeout, PollInterval).Should(gomega.BeTrue(),
		"ResourceBinding %s/%s was not suspended", namespace, name)
}

// WaitForResourceBindingUnsuspended waits until the dispatcher unsuspends the ResourceBinding.
func WaitForResourceBindingUnsuspended(namespace, name string) {
	gomega.Eventually(func() bool {
		rb, err := TestClients.KarmadaClient.WorkV1alpha2().ResourceBindings(namespace).Get(
			context.TODO(), name, metav1.GetOptions{})
		if err != nil {
			return false
		}
		return !isSchedulingSuspended(rb)
	}, PollTimeout, PollInterval).Should(gomega.BeTrue(),
		"ResourceBinding %s/%s was not unsuspended by dispatcher", namespace, name)
}

// WaitForResourceBindingHasAnnotation waits until a ResourceBinding has a specific annotation.
func WaitForResourceBindingHasAnnotation(namespace, name, annotation string) {
	gomega.Eventually(func() bool {
		rb, err := TestClients.KarmadaClient.WorkV1alpha2().ResourceBindings(namespace).Get(
			context.TODO(), name, metav1.GetOptions{})
		if err != nil {
			return false
		}
		_, exists := rb.Annotations[annotation]
		return exists
	}, PollTimeout, PollInterval).Should(gomega.BeTrue(),
		"ResourceBinding %s/%s does not have annotation %s", namespace, name, annotation)
}

// FindResourceBindingByWorkload finds a ResourceBinding that references the given workload.
func FindResourceBindingByWorkload(namespace, apiVersion, kind, workloadName string) *workv1alpha2.ResourceBinding {
	var found *workv1alpha2.ResourceBinding

	gomega.Eventually(func() bool {
		rbList, err := TestClients.KarmadaClient.WorkV1alpha2().ResourceBindings(namespace).List(
			context.TODO(), metav1.ListOptions{})
		if err != nil {
			return false
		}
		for i := range rbList.Items {
			rb := &rbList.Items[i]
			ref := rb.Spec.Resource
			if ref.APIVersion == apiVersion && ref.Kind == kind && ref.Name == workloadName {
				found = rb
				return true
			}
		}
		return false
	}, PollTimeout, PollInterval).Should(gomega.BeTrue(),
		"No ResourceBinding found for %s %s/%s", kind, namespace, workloadName)

	return found
}

func isSchedulingSuspended(rb *workv1alpha2.ResourceBinding) bool {
	if rb.Spec.Suspension == nil {
		return false
	}
	if rb.Spec.Suspension.Scheduling == nil {
		return false
	}
	return *rb.Spec.Suspension.Scheduling
}
