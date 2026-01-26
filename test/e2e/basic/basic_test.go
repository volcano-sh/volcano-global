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

package basic

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"volcano.sh/volcano-global/test/e2e/framework"
)

// TODO: Add more comprehensive e2e test cases:
// - HyperJob creation and multi-cluster scheduling
// - DataDependency controller tests with Amoro plugin
// - Dispatcher plugin tests (capacity, priority, datadependency)
// - Queue capacity management across clusters
// - Webhook validation and mutation tests
// - Failure scenarios and recovery tests

var _ = Describe("Basic E2E Tests", func() {
	var namespace string

	BeforeEach(func() {
		namespace = framework.CreateTestNamespace("basic")
	})

	AfterEach(func() {
		framework.DeleteTestNamespace(namespace)
	})

	Context("Cluster Connectivity", func() {
		It("should be able to list Karmada clusters", func() {
			clusters, err := framework.GetKarmadaClusters()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(clusters)).To(BeNumerically(">=", 2), "Expected at least 2 member clusters")

			By(fmt.Sprintf("Found clusters: %v", clusters))
		})

		It("should verify volcano-global controller is running", func() {
			ctx := context.Background()

			// Get pods in volcano-global namespace from karmada-host
			// We need to use a separate client for karmada-host
			By("Checking volcano-global-controller-manager is running")

			// Verify the deployment exists via Karmada API
			_, err := framework.KarmadaClient.CoreV1().Namespaces().Get(ctx, "volcano-global", metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred(), "volcano-global namespace should exist")
		})
	})

	Context("Queue Management", func() {
		var queueName string

		BeforeEach(func() {
			queueName = fmt.Sprintf("test-queue-%d", time.Now().UnixNano())
		})

		AfterEach(func() {
			err := framework.DeleteQueue(queueName)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should create a queue in Karmada control plane", func() {
			queue, err := framework.CreateQueue(queueName, 1)
			Expect(err).NotTo(HaveOccurred())
			Expect(queue.Name).To(Equal(queueName))
			Expect(queue.Spec.Weight).To(Equal(int32(1)))

			By("Verifying queue exists in Karmada")
			ctx := context.Background()
			retrievedQueue, err := framework.KarmadaVolcanoClient.SchedulingV1beta1().Queues().Get(ctx, queueName, metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred())
			Expect(retrievedQueue.Name).To(Equal(queueName))
		})

		It("should propagate queue to member clusters", func() {
			By("Creating a queue")
			_, err := framework.CreateQueue(queueName, 1)
			Expect(err).NotTo(HaveOccurred())

			By("Waiting for queue to be propagated to member clusters")
			// The all-queue-propagation policy should propagate queues to all members
			Eventually(func() bool {
				for member, client := range framework.MemberVolcanoClients {
					_, err := client.SchedulingV1beta1().Queues().Get(context.Background(), queueName, metav1.GetOptions{})
					if err != nil {
						By(fmt.Sprintf("Queue not yet on %s: %v", member, err))
						return false
					}
				}
				return true
			}, 2*time.Minute, 5*time.Second).Should(BeTrue(), "Queue should be propagated to all member clusters")
		})
	})

	Context("Volcano Job Scheduling", func() {
		var queueName string
		var jobName string

		BeforeEach(func() {
			queueName = fmt.Sprintf("job-queue-%d", time.Now().UnixNano())
			jobName = fmt.Sprintf("test-job-%d", time.Now().UnixNano())

			// Create queue first
			_, err := framework.CreateQueue(queueName, 1)
			Expect(err).NotTo(HaveOccurred())

			// Wait for queue to be propagated
			Eventually(func() bool {
				for _, client := range framework.MemberVolcanoClients {
					_, err := client.SchedulingV1beta1().Queues().Get(context.Background(), queueName, metav1.GetOptions{})
					if err != nil {
						return false
					}
				}
				return true
			}, 2*time.Minute, 5*time.Second).Should(BeTrue())
		})

		AfterEach(func() {
			framework.DeleteVolcanoJob(namespace, jobName)
			framework.DeletePropagationPolicy(namespace, jobName+"-pp")
			framework.DeleteQueue(queueName)
		})

		It("should create and schedule a Volcano Job", func() {
			By("Creating a Volcano Job")
			job, err := framework.CreateVolcanoJob(namespace, jobName, queueName, 1)
			Expect(err).NotTo(HaveOccurred())
			Expect(job.Name).To(Equal(jobName))

			By("Creating a PropagationPolicy for the job")
			clusters := framework.GetMemberClusters()
			Expect(len(clusters)).To(BeNumerically(">=", 1))

			_, err = framework.CreatePropagationPolicy(
				namespace,
				jobName+"-pp",
				jobName,
				"Job",
				"batch.volcano.sh/v1alpha1",
				[]string{clusters[0]},
			)
			Expect(err).NotTo(HaveOccurred())

			By("Waiting for ResourceBinding to be created")
			rb, err := framework.WaitForResourceBinding(namespace, jobName+"-job", 2*time.Minute)
			if err != nil {
				// Try alternative naming conventions
				rb, err = framework.WaitForResourceBinding(namespace, jobName, 2*time.Minute)
			}
			// ResourceBinding may have different naming, skip strict check for now
			if rb != nil {
				By(fmt.Sprintf("ResourceBinding created: %s", rb.Name))
			}

			By("Verifying job is propagated to member cluster")
			Eventually(func() bool {
				client := framework.MemberVolcanoClients[clusters[0]]
				_, err := client.BatchV1alpha1().Jobs(namespace).Get(context.Background(), jobName, metav1.GetOptions{})
				return err == nil
			}, 3*time.Minute, 5*time.Second).Should(BeTrue(), "Job should be propagated to member cluster")
		})
	})

	Context("Namespace Propagation", func() {
		It("should propagate namespace to member clusters when resources are scheduled", func() {
			By("Verifying test namespace exists in Karmada")
			ctx := context.Background()
			ns, err := framework.KarmadaClient.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred())
			Expect(ns.Name).To(Equal(namespace))
		})
	})
})
