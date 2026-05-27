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
	policyv1alpha1 "github.com/karmada-io/karmada/pkg/apis/policy/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// CreatePropagationPolicy creates a PropagationPolicy on the Karmada API server.
func CreatePropagationPolicy(pp *policyv1alpha1.PropagationPolicy) *policyv1alpha1.PropagationPolicy {
	created, err := TestClients.KarmadaClient.PolicyV1alpha1().PropagationPolicies(pp.Namespace).Create(
		context.TODO(), pp, metav1.CreateOptions{})
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred(),
		"Failed to create PropagationPolicy %s/%s", pp.Namespace, pp.Name)
	return created
}

// DeletePropagationPolicy deletes a PropagationPolicy from the Karmada API server.
func DeletePropagationPolicy(namespace, name string) {
	err := TestClients.KarmadaClient.PolicyV1alpha1().PropagationPolicies(namespace).Delete(
		context.TODO(), name, metav1.DeleteOptions{})
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred(),
		"Failed to delete PropagationPolicy %s/%s", namespace, name)
}

// WaitForPropagationPolicyExists waits until a PropagationPolicy exists.
func WaitForPropagationPolicyExists(namespace, name string) {
	gomega.Eventually(func() error {
		_, err := TestClients.KarmadaClient.PolicyV1alpha1().PropagationPolicies(namespace).Get(
			context.TODO(), name, metav1.GetOptions{})
		return err
	}, PollTimeout, PollInterval).Should(gomega.Succeed(),
		"PropagationPolicy %s/%s was not created", namespace, name)
}
