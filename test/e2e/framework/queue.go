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
	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
)

// CreateQueue creates a Queue on the Karmada API server.
func CreateQueue(queue *schedulingv1beta1.Queue) *schedulingv1beta1.Queue {
	created, err := TestClients.VolcanoClient.SchedulingV1beta1().Queues().Create(
		context.TODO(), queue, metav1.CreateOptions{})
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred(), "Failed to create Queue %s", queue.Name)
	return created
}

// DeleteQueue deletes a Queue from the Karmada API server.
func DeleteQueue(name string) {
	err := TestClients.VolcanoClient.SchedulingV1beta1().Queues().Delete(
		context.TODO(), name, metav1.DeleteOptions{})
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred(), "Failed to delete Queue %s", name)
}

// WaitForQueueOpen waits until the Queue reaches Open state on the Karmada API server.
func WaitForQueueOpen(name string) {
	gomega.Eventually(func() schedulingv1beta1.QueueState {
		queue, err := TestClients.VolcanoClient.SchedulingV1beta1().Queues().Get(
			context.TODO(), name, metav1.GetOptions{})
		if err != nil {
			return ""
		}
		return queue.Status.State
	}, PollTimeout, PollInterval).Should(gomega.Equal(schedulingv1beta1.QueueStateOpen),
		"Queue %s did not reach Open state", name)
}

// WaitForQueueOnMember waits until a Queue is propagated to a member cluster.
func WaitForQueueOnMember(memberCluster, name string) {
	volcanoClient, ok := TestClients.MemberVolcanoClients[memberCluster]
	gomega.Expect(ok).To(gomega.BeTrue(), "Unknown member cluster %s", memberCluster)

	gomega.Eventually(func() error {
		_, err := volcanoClient.SchedulingV1beta1().Queues().Get(
			context.TODO(), name, metav1.GetOptions{})
		return err
	}, PollTimeout, PollInterval).Should(gomega.Succeed(),
		"Queue %s did not appear on %s", name, memberCluster)
}
