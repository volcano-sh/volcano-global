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
	batchv1alpha1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"
)

// CreateVCJob creates a Volcano Job on the Karmada API server.
func CreateVCJob(job *batchv1alpha1.Job) *batchv1alpha1.Job {
	created, err := TestClients.VolcanoClient.BatchV1alpha1().Jobs(job.Namespace).Create(
		context.TODO(), job, metav1.CreateOptions{})
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred(), "Failed to create VCJob %s/%s", job.Namespace, job.Name)
	return created
}

// GetVCJob gets a Volcano Job from the Karmada API server.
func GetVCJob(namespace, name string) *batchv1alpha1.Job {
	job, err := TestClients.VolcanoClient.BatchV1alpha1().Jobs(namespace).Get(
		context.TODO(), name, metav1.GetOptions{})
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred(), "Failed to get VCJob %s/%s", namespace, name)
	return job
}

// DeleteVCJob deletes a Volcano Job from the Karmada API server.
func DeleteVCJob(namespace, name string) {
	err := TestClients.VolcanoClient.BatchV1alpha1().Jobs(namespace).Delete(
		context.TODO(), name, metav1.DeleteOptions{})
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred(), "Failed to delete VCJob %s/%s", namespace, name)
}

// WaitForVCJobRunning waits until the VCJob reaches Running state on the Karmada API server.
func WaitForVCJobRunning(namespace, name string) {
	gomega.Eventually(func() batchv1alpha1.JobPhase {
		job, err := TestClients.VolcanoClient.BatchV1alpha1().Jobs(namespace).Get(
			context.TODO(), name, metav1.GetOptions{})
		if err != nil {
			return ""
		}
		return job.Status.State.Phase
	}, PollTimeout, PollInterval).Should(gomega.Equal(batchv1alpha1.Running),
		"VCJob %s/%s did not reach Running state", namespace, name)
}

// WaitForVCJobCompleted waits until the VCJob reaches Completed state.
func WaitForVCJobCompleted(namespace, name string) {
	gomega.Eventually(func() batchv1alpha1.JobPhase {
		job, err := TestClients.VolcanoClient.BatchV1alpha1().Jobs(namespace).Get(
			context.TODO(), name, metav1.GetOptions{})
		if err != nil {
			return ""
		}
		return job.Status.State.Phase
	}, PollTimeout, PollInterval).Should(gomega.Equal(batchv1alpha1.Completed),
		"VCJob %s/%s did not reach Completed state", namespace, name)
}

// WaitForVCJobOnMember waits until a VCJob exists on a specific member cluster.
func WaitForVCJobOnMember(memberCluster, namespace, name string) {
	volcanoClient, ok := TestClients.MemberVolcanoClients[memberCluster]
	gomega.Expect(ok).To(gomega.BeTrue(), "Unknown member cluster %s", memberCluster)

	gomega.Eventually(func() error {
		_, err := volcanoClient.BatchV1alpha1().Jobs(namespace).Get(
			context.TODO(), name, metav1.GetOptions{})
		return err
	}, PollTimeout, PollInterval).Should(gomega.Succeed(),
		"VCJob %s/%s did not appear on %s", namespace, name, memberCluster)
}
