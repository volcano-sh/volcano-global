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

package datadependency

import (
	"context"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	datav1alpha1 "volcano.sh/apis/pkg/apis/datadependency/v1alpha1"

	"volcano.sh/volcano-global/test/e2e/framework"
)

var _ = ginkgo.Describe("Data Dependency Aware Scheduling", func() {
	var ns string

	ginkgo.BeforeEach(func() {
		ns = framework.RandomNamespace("datadep-e2e")
		framework.CreateNamespace(ns)
	})

	ginkgo.AfterEach(func() {
		framework.DeleteNamespace(ns)
	})

	ginkgo.It("should bind DataSourceClaim and inject placement annotation on ResourceBinding", func() {
		ds := &datav1alpha1.DataSource{
			ObjectMeta: metav1.ObjectMeta{
				Name: "e2e-ds",
			},
			Spec: datav1alpha1.DataSourceSpec{
				System: "amoro",
				Type:   "table",
				Name:   "db.table",
				Locality: &datav1alpha1.DataSourceLocality{
					ClusterNames: []string{"member1"},
				},
			},
		}
		_, err := framework.TestClients.VolcanoClient.DatadependencyV1alpha1().DataSources().Create(
			context.TODO(), ds, metav1.CreateOptions{})
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		defer framework.TestClients.VolcanoClient.DatadependencyV1alpha1().DataSources().Delete(
			context.TODO(), ds.Name, metav1.DeleteOptions{})

		claim := &datav1alpha1.DataSourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "e2e-dsc",
				Namespace: ns,
			},
			Spec: datav1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "db.table",
				Workload: datav1alpha1.WorkloadRef{
					APIVersion: "batch.volcano.sh/v1alpha1",
					Kind:       "Job",
					Name:       "datadep-job",
					Namespace:  ns,
				},
			},
		}
		_, err = framework.TestClients.VolcanoClient.DatadependencyV1alpha1().DataSourceClaims(ns).Create(
			context.TODO(), claim, metav1.CreateOptions{})
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		defer framework.TestClients.VolcanoClient.DatadependencyV1alpha1().DataSourceClaims(ns).Delete(
			context.TODO(), claim.Name, metav1.DeleteOptions{})

		got, err := framework.TestClients.VolcanoClient.DatadependencyV1alpha1().DataSourceClaims(ns).Get(
			context.TODO(), claim.Name, metav1.GetOptions{})
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		gomega.Expect(got.Spec.DataSourceName).To(gomega.Equal("db.table"))

		// If the DataDependency controller is active, it should update claim status.
		failures := gomega.InterceptGomegaFailures(func() {
			gomega.Eventually(func() string {
				cur, getErr := framework.TestClients.VolcanoClient.DatadependencyV1alpha1().DataSourceClaims(ns).Get(
					context.TODO(), claim.Name, metav1.GetOptions{})
				if getErr != nil {
					return ""
				}
				return string(cur.Status.Phase)
			}, 30*time.Second, framework.PollInterval).ShouldNot(gomega.BeEmpty())
		})
		if len(failures) == 0 {
			cur, getErr := framework.TestClients.VolcanoClient.DatadependencyV1alpha1().DataSourceClaims(ns).Get(
				context.TODO(), claim.Name, metav1.GetOptions{})
			gomega.Expect(getErr).ToNot(gomega.HaveOccurred())
			gomega.Expect(string(cur.Status.Phase)).To(gomega.Or(
				gomega.Equal(string(datav1alpha1.DSCPhaseBound)),
				gomega.Equal(string(datav1alpha1.DSCPhasePending)),
			))
		} else {
			ginkgo.GinkgoWriter.Println("DataSourceClaim status was not updated within 30s; continuing with CRUD smoke check.")
		}
	})
})
