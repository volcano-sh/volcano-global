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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	trainingv1alpha1 "volcano.sh/apis/pkg/apis/training/v1alpha1"
)

// CreateHyperJob creates a HyperJob on the Karmada API server.
func CreateHyperJob(hj *trainingv1alpha1.HyperJob) *trainingv1alpha1.HyperJob {
	created, err := TestClients.VolcanoClient.TrainingV1alpha1().HyperJobs(hj.Namespace).Create(
		context.TODO(), hj, metav1.CreateOptions{})
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred(),
		"Failed to create HyperJob %s/%s", hj.Namespace, hj.Name)
	return created
}

// GetHyperJob gets a HyperJob from the Karmada API server.
func GetHyperJob(namespace, name string) *trainingv1alpha1.HyperJob {
	hj, err := TestClients.VolcanoClient.TrainingV1alpha1().HyperJobs(namespace).Get(
		context.TODO(), name, metav1.GetOptions{})
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred(),
		"Failed to get HyperJob %s/%s", namespace, name)
	return hj
}

// DeleteHyperJob deletes a HyperJob from the Karmada API server.
func DeleteHyperJob(namespace, name string) {
	err := TestClients.VolcanoClient.TrainingV1alpha1().HyperJobs(namespace).Delete(
		context.TODO(), name, metav1.DeleteOptions{})
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred(),
		"Failed to delete HyperJob %s/%s", namespace, name)
}

// WaitForHyperJobChildVCJobs waits until the expected number of child VCJobs are created.
func WaitForHyperJobChildVCJobs(namespace, hyperjobName string, expectedCount int) {
	gomega.Eventually(func() int {
		jobs, err := TestClients.VolcanoClient.BatchV1alpha1().Jobs(namespace).List(
			context.TODO(), metav1.ListOptions{
				LabelSelector: "volcano.sh/hyperjob-name=" + hyperjobName,
			})
		if err != nil {
			return 0
		}
		return len(jobs.Items)
	}, PollTimeout, PollInterval).Should(gomega.Equal(expectedCount),
		"Expected %d child VCJobs for HyperJob %s/%s", expectedCount, namespace, hyperjobName)
}

// WaitForHyperJobChildPPs waits until the expected number of PropagationPolicies are created.
func WaitForHyperJobChildPPs(namespace, hyperjobName string, expectedCount int) {
	gomega.Eventually(func() int {
		pps, err := TestClients.KarmadaClient.PolicyV1alpha1().PropagationPolicies(namespace).List(
			context.TODO(), metav1.ListOptions{})
		if err != nil {
			return 0
		}
		count := 0
		for _, pp := range pps.Items {
			for _, ref := range pp.OwnerReferences {
				if ref.Name == hyperjobName && ref.Kind == "HyperJob" {
					count++
					break
				}
			}
		}
		return count
	}, PollTimeout, PollInterval).Should(gomega.Equal(expectedCount),
		"Expected %d child PropagationPolicies for HyperJob %s/%s", expectedCount, namespace, hyperjobName)
}
