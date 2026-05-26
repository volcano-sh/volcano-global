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

package quota

import (
	"context"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	batchv1alpha1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"

	policyv1alpha1 "github.com/karmada-io/karmada/pkg/apis/policy/v1alpha1"

	"volcano.sh/volcano-global/test/e2e/framework"
)

var _ = ginkgo.Describe("Resource Quota and Priority", func() {
	var ns string

	ginkgo.BeforeEach(func() {
		ns = framework.RandomNamespace("quota-e2e")
		framework.CreateNamespace(ns)
	})

	ginkgo.AfterEach(func() {
		framework.DeleteNamespace(ns)
	})

	ginkgo.Context("Queue Management", func() {
		ginkgo.It("should create a queue and propagate it to all member clusters", func() {
			queueName := "e2e-queue-" + ns
			queue := &schedulingv1beta1.Queue{
				ObjectMeta: metav1.ObjectMeta{
					Name: queueName,
				},
				Spec: schedulingv1beta1.QueueSpec{
					Reclaimable: framework.BoolPtr(true),
					Weight:      1,
				},
			}

			framework.CreateQueue(queue)
			defer framework.DeleteQueue(queueName)

			ginkgo.By("Waiting for queue to be Open on Karmada API server")
			framework.WaitForQueueOpen(queueName)

			ginkgo.By("Verifying queue is propagated to member clusters")
			for cluster := range framework.TestClients.MemberClients {
				framework.WaitForQueueOnMember(cluster, queueName)
			}
		})

		ginkgo.It("should dispatch VCJob when queue has capacity", func() {
			queueName := "e2e-capacity-" + ns
			queue := &schedulingv1beta1.Queue{
				ObjectMeta: metav1.ObjectMeta{
					Name: queueName,
				},
				Spec: schedulingv1beta1.QueueSpec{
					Reclaimable: framework.BoolPtr(true),
					Weight:      1,
					Capability: corev1.ResourceList{
						corev1.ResourceCPU: resource.MustParse("4"),
					},
				},
			}

			framework.CreateQueue(queue)
			defer framework.DeleteQueue(queueName)
			framework.WaitForQueueOpen(queueName)

			ginkgo.By("Creating a VCJob within capacity")
			job := newTestVCJob(ns, "capacity-test", queueName, 1)
			pp := newTestPropagationPolicy(ns, job.Name)

			framework.CreateVCJob(job)
			defer framework.DeleteVCJob(ns, job.Name)
			framework.CreatePropagationPolicy(pp)
			defer framework.DeletePropagationPolicy(ns, pp.Name)

			ginkgo.By("Verifying ResourceBinding is created and unsuspended by the dispatcher")
			rb := framework.FindResourceBindingByWorkload(ns, "batch.volcano.sh/v1alpha1", "Job", job.Name)
			gomega.Expect(rb).ShouldNot(gomega.BeNil())
			framework.WaitForResourceBindingUnsuspended(rb.Namespace, rb.Name)
		})

		ginkgo.It("should dispatch higher priority VCJob before lower priority when capacity is constrained", func() {
			ginkgo.By("Creating a high-priority PriorityClass")
			pc := &schedulingv1.PriorityClass{
				ObjectMeta: metav1.ObjectMeta{Name: "e2e-high-priority-" + ns},
				Value:      1000,
			}
			_, err := framework.TestClients.KubeClient.SchedulingV1().PriorityClasses().Create(
				context.TODO(), pc, metav1.CreateOptions{})
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred(), "Failed to create PriorityClass")
			defer framework.TestClients.KubeClient.SchedulingV1().PriorityClasses().Delete(
				context.TODO(), pc.Name, metav1.DeleteOptions{})

			ginkgo.By("Creating a queue whose capacity fits only one job at a time")
			queueName := "e2e-priority-" + ns
			queue := &schedulingv1beta1.Queue{
				ObjectMeta: metav1.ObjectMeta{
					Name: queueName,
				},
				Spec: schedulingv1beta1.QueueSpec{
					Reclaimable: framework.BoolPtr(true),
					Weight:      1,
					Capability: corev1.ResourceList{
						corev1.ResourceCPU: resource.MustParse("100m"),
					},
				},
			}

			framework.CreateQueue(queue)
			defer framework.DeleteQueue(queueName)
			framework.WaitForQueueOpen(queueName)

			ginkgo.By("Submitting a low-priority VCJob first")
			lowJob := newTestVCJob(ns, "low-priority", queueName, 1)
			lowPP := newTestPropagationPolicy(ns, lowJob.Name)
			framework.CreateVCJob(lowJob)
			defer framework.DeleteVCJob(ns, lowJob.Name)
			framework.CreatePropagationPolicy(lowPP)
			defer framework.DeletePropagationPolicy(ns, lowPP.Name)

			ginkgo.By("Submitting a high-priority VCJob that should preempt the dispatch order")
			highJob := newTestVCJob(ns, "high-priority", queueName, 1)
			highJob.Spec.PriorityClassName = pc.Name
			highPP := newTestPropagationPolicy(ns, highJob.Name)
			framework.CreateVCJob(highJob)
			defer framework.DeleteVCJob(ns, highJob.Name)
			framework.CreatePropagationPolicy(highPP)
			defer framework.DeletePropagationPolicy(ns, highPP.Name)

			rbLow := framework.FindResourceBindingByWorkload(ns, "batch.volcano.sh/v1alpha1", "Job", lowJob.Name)
			rbHigh := framework.FindResourceBindingByWorkload(ns, "batch.volcano.sh/v1alpha1", "Job", highJob.Name)

			ginkgo.By("Verifying the high-priority ResourceBinding is unsuspended first")
			framework.WaitForResourceBindingUnsuspended(rbHigh.Namespace, rbHigh.Name)

			ginkgo.By("Verifying the low-priority ResourceBinding stays suspended while capacity is consumed")
			framework.ConsistentlyResourceBindingSuspended(rbLow.Namespace, rbLow.Name, 10*time.Second)
		})
	})
})

func newTestVCJob(namespace, name, queue string, replicas int32) *batchv1alpha1.Job {
	return &batchv1alpha1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: batchv1alpha1.JobSpec{
			MinAvailable: replicas,
			Queue:        queue,
			Tasks: []batchv1alpha1.TaskSpec{
				{
					Name:     "task",
					Replicas: replicas,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:    "test",
									Image:   "busybox:1.36",
									Command: []string{"sh", "-c", "sleep 60"},
									Resources: corev1.ResourceRequirements{
										Requests: corev1.ResourceList{
											corev1.ResourceCPU: resource.MustParse("100m"),
										},
									},
								},
							},
							RestartPolicy: corev1.RestartPolicyNever,
						},
					},
				},
			},
		},
	}
}

func newTestPropagationPolicy(namespace, jobName string) *policyv1alpha1.PropagationPolicy {
	return &policyv1alpha1.PropagationPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: namespace,
		},
		Spec: policyv1alpha1.PropagationSpec{
			ResourceSelectors: []policyv1alpha1.ResourceSelector{
				{
					APIVersion: "batch.volcano.sh/v1alpha1",
					Kind:       "Job",
					Name:       jobName,
				},
			},
			Placement: policyv1alpha1.Placement{
				ReplicaScheduling: &policyv1alpha1.ReplicaSchedulingStrategy{
					ReplicaSchedulingType:     policyv1alpha1.ReplicaSchedulingTypeDivided,
					ReplicaDivisionPreference: policyv1alpha1.ReplicaDivisionPreferenceAggregated,
				},
			},
		},
	}
}
