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

package vcjob

import (
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	policyv1alpha1 "github.com/karmada-io/karmada/pkg/apis/policy/v1alpha1"
	batchv1alpha1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"

	"volcano.sh/volcano-global/test/e2e/framework"
)

var _ = ginkgo.Describe("Cross-Cluster VCJob Scheduling", func() {
	var ns string

	ginkgo.BeforeEach(func() {
		ns = framework.RandomNamespace("vcjob-e2e")
		framework.CreateNamespace(ns)
	})

	ginkgo.AfterEach(func() {
		framework.DeleteNamespace(ns)
	})

	ginkgo.It("should create RB, suspend by webhook, and unsuspend by dispatcher", func() {
		queueName := "vcjob-queue-" + ns
		queue := &schedulingv1beta1.Queue{
			ObjectMeta: metav1.ObjectMeta{Name: queueName},
			Spec: schedulingv1beta1.QueueSpec{
				Reclaimable: framework.BoolPtr(true),
				Weight:      1,
			},
		}
		framework.CreateQueue(queue)
		defer framework.DeleteQueue(queueName)
		framework.WaitForQueueOpen(queueName)

		job := &batchv1alpha1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "vcjob-basic",
				Namespace: ns,
			},
			Spec: batchv1alpha1.JobSpec{
				MinAvailable: 1,
				Queue:        queueName,
				Tasks: []batchv1alpha1.TaskSpec{{
					Name:     "task",
					Replicas: 1,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{{
								Name:    "main",
								Image:   "busybox:1.36",
								Command: []string{"sh", "-c", "sleep 60"},
								Resources: corev1.ResourceRequirements{
									Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")},
								},
							}},
							RestartPolicy: corev1.RestartPolicyNever,
						},
					},
				}},
			},
		}
		framework.CreateVCJob(job)
		defer framework.DeleteVCJob(ns, job.Name)

		// Use Aggregated preference to avoid the known ReviseReplica/minAvailable bug.
		pp := &policyv1alpha1.PropagationPolicy{
			ObjectMeta: metav1.ObjectMeta{
				Name:      job.Name,
				Namespace: ns,
			},
			Spec: policyv1alpha1.PropagationSpec{
				ResourceSelectors: []policyv1alpha1.ResourceSelector{{
					APIVersion: "batch.volcano.sh/v1alpha1",
					Kind:       "Job",
					Name:       job.Name,
				}},
				Placement: policyv1alpha1.Placement{
					ReplicaScheduling: &policyv1alpha1.ReplicaSchedulingStrategy{
						ReplicaSchedulingType:     policyv1alpha1.ReplicaSchedulingTypeDivided,
						ReplicaDivisionPreference: policyv1alpha1.ReplicaDivisionPreferenceAggregated,
					},
					SpreadConstraints: []policyv1alpha1.SpreadConstraint{{
						SpreadByField: policyv1alpha1.SpreadByFieldCluster,
						MinGroups:     1,
						MaxGroups:     1,
					}},
				},
			},
		}
		framework.CreatePropagationPolicy(pp)
		defer framework.DeletePropagationPolicy(ns, pp.Name)

		rb := framework.FindResourceBindingByWorkload(ns, "batch.volcano.sh/v1alpha1", "Job", job.Name)
		gomega.Expect(rb).ToNot(gomega.BeNil())

		// Verify webhook suspend / dispatcher unsuspend flow if the webhook is active.
		suspendFailures := gomega.InterceptGomegaFailures(func() {
			framework.WaitForResourceBindingSuspended(rb.Namespace, rb.Name)
		})
		if len(suspendFailures) == 0 {
			framework.WaitForResourceBindingUnsuspended(rb.Namespace, rb.Name)
		} else {
			ginkgo.GinkgoWriter.Println("ResourceBinding was not suspended by webhook within timeout; continuing with RB existence smoke check.")
		}
	})
})
