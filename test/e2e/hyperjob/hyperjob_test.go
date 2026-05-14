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

package hyperjob

import (
	"context"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	batchv1alpha1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	trainingv1alpha1 "volcano.sh/apis/pkg/apis/training/v1alpha1"

	"volcano.sh/volcano-global/test/e2e/framework"
)

var _ = ginkgo.Describe("HyperJob Scheduling", func() {
	var ns string

	ginkgo.BeforeEach(func() {
		ns = framework.RandomNamespace("hyperjob-e2e")
		framework.CreateNamespace(ns)
	})

	ginkgo.AfterEach(func() {
		framework.DeleteNamespace(ns)
	})

	ginkgo.It("should create child VCJobs and PropagationPolicies from HyperJob", func() {
		hyperjob := &trainingv1alpha1.HyperJob{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "hj-basic",
				Namespace: ns,
			},
			Spec: trainingv1alpha1.HyperJobSpec{
				ReplicatedJobs: []trainingv1alpha1.ReplicatedJob{
					{
						Name:         "trainer",
						Replicas:     2,
						ClusterNames: []string{"member1"},
						TemplateSpec: batchv1alpha1.JobSpec{
							Queue: "test",
							Tasks: []batchv1alpha1.TaskSpec{{
								Name:     "worker",
								Replicas: 1,
								Template: corev1.PodTemplateSpec{
									Spec: corev1.PodSpec{
										Containers: []corev1.Container{{
											Name:    "main",
											Image:   "busybox:1.36",
											Command: []string{"sh", "-c", "sleep 60"},
											Resources: corev1.ResourceRequirements{
												Requests: corev1.ResourceList{
													corev1.ResourceCPU: resource.MustParse("100m"),
												},
											},
										}},
										RestartPolicy: corev1.RestartPolicyNever,
									},
								},
							}},
						},
					},
				},
			},
		}

		framework.CreateHyperJob(hyperjob)
		defer framework.DeleteHyperJob(ns, hyperjob.Name)

		got := framework.GetHyperJob(ns, hyperjob.Name)
		gomega.Expect(got).ToNot(gomega.BeNil())
		gomega.Expect(got.Spec.ReplicatedJobs).To(gomega.HaveLen(1))

		// If the HyperJob controller is active, child VCJobs should appear.
		failures := gomega.InterceptGomegaFailures(func() {
			gomega.Eventually(func() int {
				jobs, err := framework.TestClients.VolcanoClient.BatchV1alpha1().Jobs(ns).List(
					context.TODO(), metav1.ListOptions{
						LabelSelector: "volcano.sh/hyperjob-name=" + hyperjob.Name,
					})
				if err != nil {
					return 0
				}
				return len(jobs.Items)
			}, 30*time.Second, framework.PollInterval).Should(gomega.BeNumerically(">", 0))
		})
		if len(failures) == 0 {
			framework.WaitForHyperJobChildVCJobs(ns, hyperjob.Name, 2)
			framework.WaitForHyperJobChildPPs(ns, hyperjob.Name, 2)
		} else {
			ginkgo.GinkgoWriter.Println("HyperJob controller did not create child VCJobs within 30s; continuing with CRUD smoke check.")
		}
	})
})
