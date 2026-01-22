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
	"errors"
	"flag"
	"strings"
	"testing"
	"time"

	clusterv1alpha1 "github.com/karmada-io/karmada/pkg/apis/cluster/v1alpha1"
	policyv1alpha1 "github.com/karmada-io/karmada/pkg/apis/policy/v1alpha1"
	workv1alpha2 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha2"
	karmadafake "github.com/karmada-io/karmada/pkg/generated/clientset/versioned/fake"
	karmadainformerfactory "github.com/karmada-io/karmada/pkg/generated/informers/externalversions"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	"volcano.sh/apis/pkg/apis/datadependency/v1alpha1"
)

func TestReconcileFunction(t *testing.T) {
	// Set klog verbosity to show debug information
	klog.InitFlags(nil)
	flag.Set("v", "5")               // Set verbosity level to 5 to see detailed logs
	flag.Set("logtostderr", "false") // Log to stderr so we can see it in test output

	// Test case 1: Reconcile a new DSC, should add a finalizer
	t.Run("Scenario 1: Reconcile should add finalizer when it's missing", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		dsc := &v1alpha1.DataSourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-dsc",
				Namespace: "default",
			},
			Spec: v1alpha1.DataSourceClaimSpec{
				System: "amoro",
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "second-app",
					Namespace:  "default",
				},
			},
		}

		// 1. Setup
		controller := setupTestController(ctx, dsc)

		// Replace startTestController with Informer Start only
		controller.karmadaInformerFactory.Start(ctx.Done())
		controller.dataInformerFactory.Start(ctx.Done())

		// Wait for sync
		cache.WaitForCacheSync(ctx.Done(),
			controller.dscListerSynced,
			controller.dsListerSynced,
		)

		// Manual Indexer Add
		// Reconcile reads from Lister. Ensure Lister has the object.
		controller.dataInformerFactory.Datadependency().V1alpha1().DataSourceClaims().Informer().GetIndexer().Add(dsc)

		// Get the object key
		key, _ := cache.MetaNamespaceKeyFunc(dsc)

		// 2. Run reconcile (Manual)
		err := controller.Reconcile(key)
		assert.NoError(t, err, "Reconcile should not return an error")

		// 3. Verify
		// Get from Fake Client to verify persistence
		updatedDsc, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSourceClaims(dsc.Namespace).Get(ctx, dsc.Name, metav1.GetOptions{})
		assert.NoError(t, err, "Failed to get updated DSC")
		assert.Contains(t, updatedDsc.Finalizers, DataSourceClaimFinalizer, "Finalizer should be added")
	})

	// Test case 2: Reconcile a DSC with finalizer, should update phase to Pending
	t.Run("Scenario 2: Reconcile should update phase to Pending when finalizer exists", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// 1. Setup Object
		dsc := &v1alpha1.DataSourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "test-dsc",
				Namespace:  "default",
				Finalizers: []string{DataSourceClaimFinalizer},
			},
			Spec: v1alpha1.DataSourceClaimSpec{
				System: "amoro",
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "default",
				},
			},
		}

		// 2. Setup Controller
		// setupTestController puts the DSC into the Fake Client
		controller := setupTestController(ctx, dsc)

		// Start Informers manually (No background workers)
		controller.karmadaInformerFactory.Start(ctx.Done())
		controller.dataInformerFactory.Start(ctx.Done())

		// Wait for sync
		cache.WaitForCacheSync(ctx.Done(),
			controller.dscListerSynced,
		)

		// Add DSC to Indexer manually
		// Reconcile reads from the Lister (cache), so we must ensure the object exists there.
		controller.dataInformerFactory.Datadependency().V1alpha1().DataSourceClaims().Informer().GetIndexer().Add(dsc)

		// Get the object key
		key, _ := cache.MetaNamespaceKeyFunc(dsc)

		// 3. Run Reconcile
		err := controller.Reconcile(key)
		assert.NoError(t, err, "Reconcile should not return an error")

		// 4. Verify result from Fake Client
		updatedDsc, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSourceClaims(dsc.Namespace).Get(ctx, dsc.Name, metav1.GetOptions{})
		assert.NoError(t, err, "Failed to get updated DSC")
		assert.Equal(t, v1alpha1.DSCPhasePending, updatedDsc.Status.Phase, "DSC phase should be Pending")
	})

	// Test case 3: Scenario 3 - Pending → Bound (DynamicBinding)
	t.Run("Scenario 3: Pending DSC should bind to DataSource via DynamicBinding", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// 1. Setup Data
		dsc := &v1alpha1.DataSourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "test-dsc",
				Namespace:  "default",
				Finalizers: []string{DataSourceClaimFinalizer},
			},
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "test-table",
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "default",
				},
			},
			Status: v1alpha1.DataSourceClaimStatus{
				Phase: v1alpha1.DSCPhasePending,
			},
		}

		// 2. Setup Controller & Mock
		mockPluginManager := NewMockPluginManager(func(ctx context.Context, dsc *v1alpha1.DataSourceClaim) ([]string, error) {
			return []string{"cluster1", "cluster2"}, nil
		})

		controller := setupTestController(ctx, dsc)
		controller.pluginManager = mockPluginManager

		// Start Informers manually (No background workers)
		controller.karmadaInformerFactory.Start(ctx.Done())
		controller.dataInformerFactory.Start(ctx.Done())

		// Wait for sync
		cache.WaitForCacheSync(ctx.Done(),
			controller.dscListerSynced,
			controller.dsListerSynced,
		)

		// Add DSC to Indexer manually
		controller.dataInformerFactory.Datadependency().V1alpha1().DataSourceClaims().Informer().GetIndexer().Add(dsc)

		// Get the object key
		key, _ := cache.MetaNamespaceKeyFunc(dsc)

		// 3. Run Reconcile
		// This triggers: handlePending -> findMatchedDS (miss) -> dynamicBinding -> create DS -> staticBinding
		err := controller.Reconcile(key)
		assert.NoError(t, err, "Reconcile should not return an error")

		// 4. Verify DSC Result
		updatedDsc, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSourceClaims(dsc.Namespace).Get(ctx, dsc.Name, metav1.GetOptions{})
		assert.NoError(t, err, "Failed to get updated DSC")
		assert.Equal(t, v1alpha1.DSCPhaseBound, updatedDsc.Status.Phase, "DSC phase should be Bound")
		assert.NotEmpty(t, updatedDsc.Status.BoundDataSource, "BoundDataSource should be set")

		// 5. Verify DS Creation
		createdDS, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSources().Get(ctx, updatedDsc.Status.BoundDataSource, metav1.GetOptions{})
		assert.NoError(t, err, "Failed to get created DataSource")
		assert.Equal(t, "amoro", createdDS.Spec.System, "DataSource system should match DSC")
		assert.Equal(t, "table", createdDS.Spec.Type, "DataSource type should match DSC")
		assert.Equal(t, "test-table", createdDS.Spec.Name, "DataSource name should match DSC")
		assert.Contains(t, createdDS.Spec.Locality.ClusterNames, "cluster1", "DataSource should contain cluster1")
		assert.Contains(t, createdDS.Spec.Locality.ClusterNames, "cluster2", "DataSource should contain cluster2")

		// Verify that DSC is added to DataSource's ClaimRefs
		assert.Len(t, createdDS.Status.ClaimRefs, 1, "DataSource should have one ClaimRef")
		assert.Equal(t, dsc.Name, createdDS.Status.ClaimRefs[0].Name, "ClaimRef should reference the DSC")
		assert.Equal(t, dsc.Namespace, createdDS.Status.ClaimRefs[0].Namespace, "ClaimRef should have correct namespace")
	})

	t.Run("Scenario 4: Bound DSC without ResourceBinding should wait for RB creation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// 1. Setup Data
		dsc := &v1alpha1.DataSourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "test-dsc",
				Namespace:  "default",
				UID:        "test-uid",
				Finalizers: []string{DataSourceClaimFinalizer},
			},
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "test-table",
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "default",
				},
			},
			Status: v1alpha1.DataSourceClaimStatus{
				Phase:           v1alpha1.DSCPhaseBound,
				BoundDataSource: "test-ds",
			},
		}

		ds := &v1alpha1.DataSource{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-ds",
			},
			Spec: v1alpha1.DataSourceSpec{
				System:        "amoro",
				Type:          "table",
				Name:          "test-table",
				ReclaimPolicy: v1alpha1.ReclaimPolicyRetain,
				Locality: &v1alpha1.DataSourceLocality{
					ClusterNames: []string{"cluster1", "cluster2"},
				},
			},
			Status: v1alpha1.DataSourceStatus{
				ClaimRefs: []corev1.ObjectReference{
					{
						Name:      "test-dsc",
						Namespace: "default",
						UID:       "test-uid",
					},
				},
			},
		}

		// 2. Setup Controller
		controller := setupTestController(ctx, dsc)

		// Start Informers manually (No background workers)
		controller.karmadaInformerFactory.Start(ctx.Done())
		controller.dataInformerFactory.Start(ctx.Done())

		// Wait for sync
		cache.WaitForCacheSync(ctx.Done(),
			controller.dscListerSynced,
			controller.dsListerSynced,
			controller.rbListerSynced,
		)

		// Add DSC to Indexer manually
		controller.dataInformerFactory.Datadependency().V1alpha1().DataSourceClaims().Informer().GetIndexer().Add(dsc)

		// 3. Create DS and Sync Indexer
		_, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSources().Create(ctx, ds, metav1.CreateOptions{})
		assert.NoError(t, err, "Failed to create DataSource")

		// Add DS to Indexer manually
		controller.dataInformerFactory.Datadependency().V1alpha1().DataSources().Informer().GetIndexer().Add(ds)

		// Get the object key
		key, _ := cache.MetaNamespaceKeyFunc(dsc)

		// 4. Run Reconcile
		// Logic: handleBound -> checks match (true) -> handlePlacementUpdate -> findAssociatedRB (nil) -> returns nil (Wait)
		err = controller.Reconcile(key)
		assert.NoError(t, err, "Reconcile should not return an error")

		// 5. Verify Results
		// Verify that DSC status remains unchanged (still Bound)
		updatedDsc, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSourceClaims(dsc.Namespace).Get(ctx, dsc.Name, metav1.GetOptions{})
		assert.NoError(t, err, "Failed to get updated DSC")
		assert.Equal(t, v1alpha1.DSCPhaseBound, updatedDsc.Status.Phase, "DSC phase should remain Bound")
		assert.Equal(t, "test-ds", updatedDsc.Status.BoundDataSource, "BoundDataSource should remain unchanged")

		// Verify that no ResourceBinding was created (since user hasn't created workload yet)
		rbList, err := controller.karmadaClient.WorkV1alpha2().ResourceBindings("default").List(ctx, metav1.ListOptions{})
		assert.NoError(t, err, "Failed to list ResourceBindings")
		assert.Empty(t, rbList.Items, "No ResourceBinding should be created")

		// Verify that DataSource status remains unchanged
		updatedDS, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSources().Get(ctx, ds.Name, metav1.GetOptions{})
		assert.NoError(t, err, "Failed to get updated DataSource")
		assert.Len(t, updatedDS.Status.ClaimRefs, 1, "DataSource should still have one ClaimRef")
		assert.Equal(t, dsc.Name, updatedDS.Status.ClaimRefs[0].Name, "ClaimRef should still reference the DSC")
	})

	t.Run("Scenario 6: RB created and discovered by dispatcher, DSC annotation should be updated", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// 1. Setup Objects
		dsc := &v1alpha1.DataSourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "test-dsc",
				Namespace:  "default",
				UID:        "test-uid",
				Finalizers: []string{DataSourceClaimFinalizer},
			},
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "test-table",
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "default",
				},
			},
			Status: v1alpha1.DataSourceClaimStatus{
				Phase:           v1alpha1.DSCPhaseBound,
				BoundDataSource: "test-ds",
			},
		}

		ds := &v1alpha1.DataSource{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-ds",
			},
			Spec: v1alpha1.DataSourceSpec{
				System:        "amoro",
				Type:          "table",
				Name:          "test-table",
				ReclaimPolicy: v1alpha1.ReclaimPolicyRetain,
				Locality: &v1alpha1.DataSourceLocality{
					ClusterNames: []string{"cluster1", "cluster2"},
				},
			},
			Status: v1alpha1.DataSourceStatus{
				ClaimRefs: []corev1.ObjectReference{
					{
						Name:      "test-dsc",
						Namespace: "default",
						UID:       "test-uid",
					},
				},
			},
		}

		// 2. Setup Controller
		controller := setupTestController(ctx, dsc)

		// Start Informers manually
		controller.karmadaInformerFactory.Start(ctx.Done())
		controller.dataInformerFactory.Start(ctx.Done())

		// Wait for sync
		cache.WaitForCacheSync(ctx.Done(),
			controller.dscListerSynced,
			controller.dsListerSynced,
			controller.rbListerSynced,
			controller.cListerSynced,
		)

		// Add DSC to Indexer manually
		controller.dataInformerFactory.Datadependency().V1alpha1().DataSourceClaims().Informer().GetIndexer().Add(dsc)

		// 3. Create Objects & Sync Indexers
		// Create DS
		_, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSources().Create(ctx, ds, metav1.CreateOptions{})
		assert.NoError(t, err)
		// Add DS to Indexer
		controller.dataInformerFactory.Datadependency().V1alpha1().DataSources().Informer().GetIndexer().Add(ds)

		// Create Clusters
		clusterNames := []string{"cluster1", "cluster2", "cluster3"}
		for _, name := range clusterNames {
			c := &clusterv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: name}}
			_, err = controller.karmadaClient.ClusterV1alpha1().Clusters().Create(ctx, c, metav1.CreateOptions{})
			assert.NoError(t, err)
			// Add Cluster to Indexer (Critical for calculation)
			controller.karmadaInformerFactory.Cluster().V1alpha1().Clusters().Informer().GetIndexer().Add(c)
		}

		// Create ResourceBinding
		rb := &workv1alpha2.ResourceBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app-deployment",
				Namespace: "default",
			},
			Spec: workv1alpha2.ResourceBindingSpec{
				Resource: workv1alpha2.ObjectReference{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "default",
				},
			},
		}

		_, err = controller.karmadaClient.WorkV1alpha2().ResourceBindings("default").Create(ctx, rb, metav1.CreateOptions{})
		assert.NoError(t, err)
		// Add RB to Indexer (Critical for findAssociatedRB)
		controller.karmadaInformerFactory.Work().V1alpha2().ResourceBindings().Informer().GetIndexer().Add(rb)

		// Get the object key
		key, _ := cache.MetaNamespaceKeyFunc(dsc)

		// 4. Run Reconcile
		// Logic: handleBound -> match(true) -> handlePlacementUpdate -> findAssociatedRB(found) -> injectPlacementAffinity
		err = controller.Reconcile(key)
		assert.NoError(t, err, "Reconcile should not return an error")

		// 5. Verify Results
		// Verify RB injection
		updatedRB, err := controller.karmadaClient.WorkV1alpha2().ResourceBindings("default").Get(ctx, rb.Name, metav1.GetOptions{})
		assert.NoError(t, err, "Failed to get updated ResourceBinding")
		assert.Equal(t, "true", updatedRB.Annotations[PlacementInjectedAnnotation], "ResourceBinding should have placement injected annotation")

		// Verify Affinity (DS: 1,2. All: 1,2,3. Exclude: 3)
		if assert.NotNil(t, updatedRB.Spec.Placement) && assert.NotNil(t, updatedRB.Spec.Placement.ClusterAffinity) {
			assert.Contains(t, updatedRB.Spec.Placement.ClusterAffinity.ExcludeClusters, "cluster3")
			assert.NotContains(t, updatedRB.Spec.Placement.ClusterAffinity.ExcludeClusters, "cluster1")
			assert.NotContains(t, updatedRB.Spec.Placement.ClusterAffinity.ExcludeClusters, "cluster2")
		}

		// Verify Annotation
		assert.Equal(t, "cluster3", updatedRB.Annotations[ExcludedClustersAnnotation])

		// Verify DSC Status
		updatedDsc, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSourceClaims("default").Get(ctx, dsc.Name, metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Equal(t, v1alpha1.DSCPhaseBound, updatedDsc.Status.Phase)
		assert.Equal(t, "test-ds", updatedDsc.Status.BoundDataSource)
	})

	t.Run("Scenario 8: Second DSC created, matches existing DataSource via staticBinding", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// 1. Setup Objects
		firstDsc := &v1alpha1.DataSourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "first-dsc",
				Namespace:  "default",
				UID:        "first-uid",
				Finalizers: []string{DataSourceClaimFinalizer},
			},
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "shared-table",
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "default",
				},
			},
			Status: v1alpha1.DataSourceClaimStatus{
				Phase:           v1alpha1.DSCPhaseBound,
				BoundDataSource: "shared-ds",
			},
		}

		existingDs := &v1alpha1.DataSource{
			ObjectMeta: metav1.ObjectMeta{
				Name: "shared-ds",
			},
			Spec: v1alpha1.DataSourceSpec{
				System:        "amoro",
				Type:          "table",
				Name:          "shared-table",
				ReclaimPolicy: v1alpha1.ReclaimPolicyRetain,
				Locality: &v1alpha1.DataSourceLocality{
					ClusterNames: []string{"cluster1", "cluster2"},
				},
			},
			Status: v1alpha1.DataSourceStatus{
				ClaimRefs: []corev1.ObjectReference{
					{
						Name:      "first-dsc",
						Namespace: "default",
						UID:       "first-uid",
					},
				},
			},
		}

		// 2. Setup Controller
		controller := setupTestController(ctx, firstDsc)

		// Start ONLY Informers
		controller.karmadaInformerFactory.Start(ctx.Done())
		controller.dataInformerFactory.Start(ctx.Done())

		// Wait for cache sync
		cache.WaitForCacheSync(ctx.Done(),
			controller.dscListerSynced,
			controller.dsListerSynced,
			controller.rbListerSynced,
			controller.cListerSynced,
		)

		// 3. Create Objects in Client AND Indexer
		// Create DS
		_, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSources().Create(ctx, existingDs, metav1.CreateOptions{})
		assert.NoError(t, err)
		// Update Indexer
		controller.dataInformerFactory.Datadependency().V1alpha1().DataSources().Informer().GetIndexer().Add(existingDs)

		// Create Clusters
		clusterNames := []string{"cluster1", "cluster2", "cluster3"}
		for _, name := range clusterNames {
			c := &clusterv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: name}}
			_, err = controller.karmadaClient.ClusterV1alpha1().Clusters().Create(ctx, c, metav1.CreateOptions{})
			assert.NoError(t, err)
			// Update Indexer (Critical for triggerRescheduling list)
			controller.karmadaInformerFactory.Cluster().V1alpha1().Clusters().Informer().GetIndexer().Add(c)
		}

		// 4. Create Second DSC
		secondDsc := &v1alpha1.DataSourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "second-dsc",
				Namespace:  "default",
				UID:        "second-uid",
				Finalizers: []string{DataSourceClaimFinalizer},
			},
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "shared-table",
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "second-app",
					Namespace:  "default",
				},
			},
			Status: v1alpha1.DataSourceClaimStatus{
				Phase: v1alpha1.DSCPhasePending,
			},
		}

		_, err = controller.datadependencyClient.DatadependencyV1alpha1().DataSourceClaims("default").Create(ctx, secondDsc, metav1.CreateOptions{})
		assert.NoError(t, err)
		// Update Indexer
		controller.dataInformerFactory.Datadependency().V1alpha1().DataSourceClaims().Informer().GetIndexer().Add(secondDsc)

		// 5. Manual Reconcile (Step 1: Binding)
		key, _ := cache.MetaNamespaceKeyFunc(secondDsc)
		err = controller.Reconcile(key)
		assert.NoError(t, err)

		// Verify Binding
		updatedSecondDsc, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSourceClaims("default").Get(ctx, secondDsc.Name, metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Equal(t, v1alpha1.DSCPhaseBound, updatedSecondDsc.Status.Phase)
		assert.Equal(t, "shared-ds", updatedSecondDsc.Status.BoundDataSource)

		// Sync updated DSC state to Indexer for next steps
		controller.dataInformerFactory.Datadependency().V1alpha1().DataSourceClaims().Informer().GetIndexer().Update(updatedSecondDsc)

		// Verify DS Refs
		updatedDs, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSources().Get(ctx, existingDs.Name, metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Len(t, updatedDs.Status.ClaimRefs, 2)

		// Sync updated DS state to Indexer
		controller.dataInformerFactory.Datadependency().V1alpha1().DataSources().Informer().GetIndexer().Update(updatedDs)

		// 6. Create ResourceBinding
		rb := &workv1alpha2.ResourceBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "second-app-deployment",
				Namespace: "default",
			},
			Spec: workv1alpha2.ResourceBindingSpec{
				Resource: workv1alpha2.ObjectReference{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "second-app",
					Namespace:  "default",
				},
			},
		}

		_, err = controller.karmadaClient.WorkV1alpha2().ResourceBindings("default").Create(ctx, rb, metav1.CreateOptions{})
		assert.NoError(t, err)
		// Update Indexer (Remove wait.Poll)
		controller.karmadaInformerFactory.Work().V1alpha2().ResourceBindings().Informer().GetIndexer().Add(rb)

		// 7. Manual Reconcile (Step 2: Injection)
		err = controller.Reconcile(key)
		assert.NoError(t, err)

		// Verify Injection
		updatedRB, err := controller.karmadaClient.WorkV1alpha2().ResourceBindings("default").Get(ctx, rb.Name, metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Equal(t, "true", updatedRB.Annotations[PlacementInjectedAnnotation])

		// Verify Affinity (Total: 1,2,3. Locality: 1,2. Exclude: 3)
		if assert.NotNil(t, updatedRB.Spec.Placement) && assert.NotNil(t, updatedRB.Spec.Placement.ClusterAffinity) {
			assert.Contains(t, updatedRB.Spec.Placement.ClusterAffinity.ExcludeClusters, "cluster3")
			assert.NotContains(t, updatedRB.Spec.Placement.ClusterAffinity.ExcludeClusters, "cluster1")
			assert.NotContains(t, updatedRB.Spec.Placement.ClusterAffinity.ExcludeClusters, "cluster2")
		}

		assert.Equal(t, "cluster3", updatedRB.Annotations[ExcludedClustersAnnotation])
	})

	t.Run("Scenario 9.1: Modified data source does not exist, DSC remains Pending", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// 1. Setup Objects
		dsc := &v1alpha1.DataSourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "test-dsc",
				Namespace:  "default",
				UID:        "test-uid",
				Finalizers: []string{DataSourceClaimFinalizer},
			},
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "original-table", // Original data source name
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "default",
				},
			},
			Status: v1alpha1.DataSourceClaimStatus{
				Phase:           v1alpha1.DSCPhaseBound,
				BoundDataSource: "original-ds",
			},
		}

		originalDs := &v1alpha1.DataSource{
			ObjectMeta: metav1.ObjectMeta{
				Name: "original-ds",
			},
			Spec: v1alpha1.DataSourceSpec{
				System:        "amoro",
				Type:          "table",
				Name:          "original-table",
				ReclaimPolicy: v1alpha1.ReclaimPolicyRetain,
				Locality: &v1alpha1.DataSourceLocality{
					ClusterNames: []string{"cluster1", "cluster2"},
				},
			},
			Status: v1alpha1.DataSourceStatus{
				ClaimRefs: []corev1.ObjectReference{
					{
						Name:      "test-dsc",
						Namespace: "default",
						UID:       "test-uid",
					},
				},
			},
		}

		// 2. Setup Controller
		controller := setupTestController(ctx, dsc)

		// Setup Mock Plugin (return empty list -> dynamic binding fails)
		controller.pluginManager = NewMockPluginManager(func(ctx context.Context, dsc *v1alpha1.DataSourceClaim) ([]string, error) {
			return []string{}, nil
		})

		// Start Only Informers
		controller.karmadaInformerFactory.Start(ctx.Done())
		controller.dataInformerFactory.Start(ctx.Done())

		cache.WaitForCacheSync(ctx.Done(),
			controller.dscListerSynced,
			controller.dsListerSynced,
			controller.rbListerSynced,
			controller.cListerSynced,
		)

		// 3. Create Objects in Client AND Indexer
		_, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSources().Create(ctx, originalDs, metav1.CreateOptions{})
		assert.NoError(t, err)
		// Add DS to Indexer
		controller.dataInformerFactory.Datadependency().V1alpha1().DataSources().Informer().GetIndexer().Add(originalDs)

		// Create Clusters
		clusterNames := []string{"cluster1", "cluster2", "cluster3"}
		for _, name := range clusterNames {
			c := &clusterv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: name}}
			_, err = controller.karmadaClient.ClusterV1alpha1().Clusters().Create(ctx, c, metav1.CreateOptions{})
			assert.NoError(t, err)
			// Add Cluster to Indexer
			controller.karmadaInformerFactory.Cluster().V1alpha1().Clusters().Informer().GetIndexer().Add(c)
		}

		// Create RB
		rb := &workv1alpha2.ResourceBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app-deployment",
				Namespace: "default",
				Annotations: map[string]string{
					PlacementInjectedAnnotation: "true",
					ExcludedClustersAnnotation:  "cluster3",
				},
			},
			Spec: workv1alpha2.ResourceBindingSpec{
				Resource: workv1alpha2.ObjectReference{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "default",
				},
				Placement: &policyv1alpha1.Placement{
					ClusterAffinity: &policyv1alpha1.ClusterAffinity{
						ExcludeClusters: []string{"cluster3"},
					},
				},
			},
		}
		_, err = controller.karmadaClient.WorkV1alpha2().ResourceBindings("default").Create(ctx, rb, metav1.CreateOptions{})
		assert.NoError(t, err)
		// Add RB to Indexer
		controller.karmadaInformerFactory.Work().V1alpha2().ResourceBindings().Informer().GetIndexer().Add(rb)

		// 4. Modify DSC (Trigger Test)
		dscToUpdate := dsc.DeepCopy()
		dscToUpdate.Spec.DataSourceName = "non-existent-table" // Change to non-existent data source
		_, err = controller.datadependencyClient.DatadependencyV1alpha1().DataSourceClaims("default").Update(ctx, dscToUpdate, metav1.UpdateOptions{})
		assert.NoError(t, err)

		// Sync updated DSC to Indexer
		controller.dataInformerFactory.Datadependency().V1alpha1().DataSourceClaims().Informer().GetIndexer().Update(dscToUpdate)

		// 5. Manual Reconcile (Step 1: Unbinding)
		key, _ := cache.MetaNamespaceKeyFunc(dscToUpdate)
		err = controller.Reconcile(key)
		assert.NoError(t, err)

		// Verify Unbinding State
		updatedDsc, _ := controller.datadependencyClient.DatadependencyV1alpha1().DataSourceClaims("default").Get(ctx, dsc.Name, metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Equal(t, v1alpha1.DSCPhasePending, updatedDsc.Status.Phase)
		assert.Empty(t, updatedDsc.Status.BoundDataSource)

		// Sync updated DSC state (Pending) to Indexer for next step
		controller.dataInformerFactory.Datadependency().V1alpha1().DataSourceClaims().Informer().GetIndexer().Update(updatedDsc)

		// Verify Original DS (ClaimRef removed)
		updatedOriginalDs, _ := controller.datadependencyClient.DatadependencyV1alpha1().DataSources().Get(ctx, originalDs.Name, metav1.GetOptions{})
		assert.Empty(t, updatedOriginalDs.Status.ClaimRefs)

		// Verify RB (Annotation removed)
		updatedRB, _ := controller.karmadaClient.WorkV1alpha2().ResourceBindings("default").Get(ctx, rb.Name, metav1.GetOptions{})
		assert.NotEqual(t, "true", updatedRB.Annotations[PlacementInjectedAnnotation])

		// 6. Manual Reconcile (Step 2: Rebinding attempt)
		// Status is Pending -> findMatchedDS (fail) -> dynamicBinding (fail mock) -> Remains Pending
		err = controller.Reconcile(key)
		assert.NoError(t, err)

		// Verify Final State
		finalDsc, _ := controller.datadependencyClient.DatadependencyV1alpha1().DataSourceClaims("default").Get(ctx, dsc.Name, metav1.GetOptions{})
		assert.Equal(t, v1alpha1.DSCPhasePending, finalDsc.Status.Phase)
		assert.Empty(t, finalDsc.Status.BoundDataSource)
	})

	t.Run("Scenario 9.2: Modified data source exists, DSC rebinds and RB is rescheduled to new clusters", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// 1. Setup Objects (DSC, OriginalDS, NewDS)
		dsc := &v1alpha1.DataSourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "test-dsc",
				Namespace:  "default",
				UID:        "test-uid",
				Finalizers: []string{DataSourceClaimFinalizer},
			},
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "original-table",
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Namespace:  "default",
					Name:       "test-app",
				},
			},
			Status: v1alpha1.DataSourceClaimStatus{
				Phase:           v1alpha1.DSCPhaseBound,
				BoundDataSource: "original-ds",
			},
		}

		originalDs := &v1alpha1.DataSource{
			ObjectMeta: metav1.ObjectMeta{
				Name: "original-ds",
			},
			Spec: v1alpha1.DataSourceSpec{
				System:        "amoro",
				Type:          "table",
				Name:          "original-table",
				ReclaimPolicy: v1alpha1.ReclaimPolicyRetain,
				Locality: &v1alpha1.DataSourceLocality{
					ClusterNames: []string{"cluster1", "cluster2"},
				},
			},
			Status: v1alpha1.DataSourceStatus{
				ClaimRefs: []corev1.ObjectReference{
					{
						Name:      "test-dsc",
						Namespace: "default",
						UID:       "test-uid",
					},
				},
			},
		}

		newDs := &v1alpha1.DataSource{
			ObjectMeta: metav1.ObjectMeta{
				Name: "new-ds",
			},
			Spec: v1alpha1.DataSourceSpec{
				System:        "amoro",
				Type:          "table",
				Name:          "new-table",
				ReclaimPolicy: v1alpha1.ReclaimPolicyRetain,
				Locality: &v1alpha1.DataSourceLocality{
					ClusterNames: []string{"cluster3", "cluster4"},
				},
			},
			Status: v1alpha1.DataSourceStatus{
				ClaimRefs: []corev1.ObjectReference{},
			},
		}

		// 2. Setup Controller
		controller := setupTestController(ctx, dsc)

		// Start Informers (No Workers)
		controller.karmadaInformerFactory.Start(ctx.Done())
		controller.dataInformerFactory.Start(ctx.Done())

		cache.WaitForCacheSync(ctx.Done(),
			controller.dscListerSynced,
			controller.dsListerSynced,
			controller.rbListerSynced,
			controller.cListerSynced,
		)

		// 3. Create Objects & Manually Sync Indexer
		// DataSources
		_, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSources().Create(ctx, originalDs, metav1.CreateOptions{})
		assert.NoError(t, err)
		controller.dataInformerFactory.Datadependency().V1alpha1().DataSources().Informer().GetIndexer().Add(originalDs)

		_, err = controller.datadependencyClient.DatadependencyV1alpha1().DataSources().Create(ctx, newDs, metav1.CreateOptions{})
		assert.NoError(t, err)
		controller.dataInformerFactory.Datadependency().V1alpha1().DataSources().Informer().GetIndexer().Add(newDs)

		// Clusters
		clusterNames := []string{"cluster1", "cluster2", "cluster3", "cluster4", "cluster5"}
		for _, name := range clusterNames {
			cluster := &clusterv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: name}}
			_, err = controller.karmadaClient.ClusterV1alpha1().Clusters().Create(ctx, cluster, metav1.CreateOptions{})
			assert.NoError(t, err)
			controller.karmadaInformerFactory.Cluster().V1alpha1().Clusters().Informer().GetIndexer().Add(cluster)
		}

		// ResourceBinding
		rb := &workv1alpha2.ResourceBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app-deployment",
				Namespace: "default",
				Annotations: map[string]string{
					PlacementInjectedAnnotation: "true",
					ExcludedClustersAnnotation:  "cluster3,cluster4,cluster5",
				},
			},
			Spec: workv1alpha2.ResourceBindingSpec{
				Resource: workv1alpha2.ObjectReference{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "default",
				},
				Placement: &policyv1alpha1.Placement{
					ClusterAffinity: &policyv1alpha1.ClusterAffinity{
						ExcludeClusters: []string{"cluster3", "cluster4", "cluster5"},
					},
				},
			},
		}
		_, err = controller.karmadaClient.WorkV1alpha2().ResourceBindings("default").Create(ctx, rb, metav1.CreateOptions{})
		assert.NoError(t, err)
		controller.karmadaInformerFactory.Work().V1alpha2().ResourceBindings().Informer().GetIndexer().Add(rb)

		// 4. Modify DSC Spec
		dscToUpdate := dsc.DeepCopy()
		dscToUpdate.Spec.DataSourceName = "new-table"
		_, err = controller.datadependencyClient.DatadependencyV1alpha1().DataSourceClaims("default").Update(ctx, dscToUpdate, metav1.UpdateOptions{})
		assert.NoError(t, err)

		// [Manual Sync] Update Indexer with new Spec immediately
		err = controller.dataInformerFactory.Datadependency().V1alpha1().DataSourceClaims().Informer().GetIndexer().Update(dscToUpdate)
		assert.NoError(t, err)

		key, _ := cache.MetaNamespaceKeyFunc(dscToUpdate)

		// -----------------------------------------------------------
		// Step A: Trigger Unbinding
		// -----------------------------------------------------------
		err = controller.Reconcile(key)
		assert.NoError(t, err)

		// Get latest from Client
		updatedDSC, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSourceClaims("default").Get(ctx, dsc.Name, metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Equal(t, v1alpha1.DSCPhasePending, updatedDSC.Status.Phase)
		assert.Empty(t, updatedDSC.Status.BoundDataSource)

		// [Manual Sync] Force Indexer to Pending state for next Reconcile
		err = controller.dataInformerFactory.Datadependency().V1alpha1().DataSourceClaims().Informer().GetIndexer().Update(updatedDSC)
		assert.NoError(t, err)

		// Update Original DS in Indexer (ClaimRefs removed)
		updatedOriginalDs, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSources().Get(ctx, originalDs.Name, metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Empty(t, updatedOriginalDs.Status.ClaimRefs)
		controller.dataInformerFactory.Datadependency().V1alpha1().DataSources().Informer().GetIndexer().Update(updatedOriginalDs)

		// -----------------------------------------------------------
		// Step B: Trigger Rebinding
		// -----------------------------------------------------------
		// Reconcile reads "Pending" from Indexer -> findMatchedDS -> Bind
		err = controller.Reconcile(key)
		assert.NoError(t, err)

		// Get latest from Client
		updatedDSC, err = controller.datadependencyClient.DatadependencyV1alpha1().DataSourceClaims("default").Get(ctx, dsc.Name, metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Equal(t, v1alpha1.DSCPhaseBound, updatedDSC.Status.Phase)
		assert.Equal(t, "new-ds", updatedDSC.Status.BoundDataSource)

		// [Manual Sync] Force Indexer to Bound state for next Reconcile
		err = controller.dataInformerFactory.Datadependency().V1alpha1().DataSourceClaims().Informer().GetIndexer().Update(updatedDSC)
		assert.NoError(t, err)

		// Update New DS in Indexer (ClaimRef added)
		updatedNewDs, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSources().Get(ctx, newDs.Name, metav1.GetOptions{})
		assert.NoError(t, err)
		if assert.Len(t, updatedNewDs.Status.ClaimRefs, 1) {
			assert.Equal(t, "test-dsc", updatedNewDs.Status.ClaimRefs[0].Name)
		}
		controller.dataInformerFactory.Datadependency().V1alpha1().DataSources().Informer().GetIndexer().Update(updatedNewDs)

		// -----------------------------------------------------------
		// Step C: Trigger Placement Injection
		// -----------------------------------------------------------
		// Reconcile reads "Bound" from Indexer -> handlePlacementUpdate -> inject
		err = controller.Reconcile(key)
		assert.NoError(t, err)

		// 5. Verification
		updatedRB, err := controller.karmadaClient.WorkV1alpha2().ResourceBindings("default").Get(ctx, rb.Name, metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Equal(t, "true", updatedRB.Annotations[PlacementInjectedAnnotation])

		actualExcluded := updatedRB.Spec.Placement.ClusterAffinity.ExcludeClusters
		expectedExcluded := []string{"cluster1", "cluster2", "cluster5"}

		for _, expected := range expectedExcluded {
			assert.Contains(t, actualExcluded, expected)
		}
		assert.NotContains(t, actualExcluded, "cluster3")
		assert.NotContains(t, actualExcluded, "cluster4")
	})

	// Test case 10: DSC deletion should trigger handleDeletion and cleanup
	t.Run("Scenario 10: DSC deletion should trigger handleDeletion and cleanup", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// 1. Setup Objects
		dsc := &v1alpha1.DataSourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "test-dsc-to-delete",
				Namespace:         "default",
				UID:               "test-dsc-to-delete-uid",
				Finalizers:        []string{DataSourceClaimFinalizer},
				DeletionTimestamp: &metav1.Time{Time: time.Now()}, // Mark for deletion
			},
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "dc1.db1.orders",
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "locality-test-app",
					Namespace:  "default",
				},
			},
			Status: v1alpha1.DataSourceClaimStatus{
				Phase:           v1alpha1.DSCPhaseBound,
				BoundDataSource: "test-ds-for-deletion",
			},
		}

		ds := &v1alpha1.DataSource{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-ds-for-deletion",
			},
			Spec: v1alpha1.DataSourceSpec{
				System:        "amoro",
				Type:          "table",
				Name:          "dc1.db1.orders",
				ReclaimPolicy: v1alpha1.ReclaimPolicyRetain, // Use Retain to test cleanup without DS deletion
				Locality: &v1alpha1.DataSourceLocality{
					ClusterNames: []string{"cluster1", "cluster2"},
				},
			},
			Status: v1alpha1.DataSourceStatus{
				ClaimRefs: []corev1.ObjectReference{
					{
						Kind:      "DataSourceClaim",
						Name:      dsc.Name,
						Namespace: dsc.Namespace,
						UID:       dsc.UID,
					},
				},
				BoundClaims: 1,
			},
		}

		// 2. Setup Controller
		// setupTestController creates the DSC in the fake client
		controller := setupTestController(ctx, dsc)

		// Start Informers manually (No background workers)
		controller.karmadaInformerFactory.Start(ctx.Done())
		controller.dataInformerFactory.Start(ctx.Done())

		// Wait for sync
		cache.WaitForCacheSync(ctx.Done(),
			controller.dscListerSynced,
			controller.dsListerSynced,
		)

		// Add DSC to Indexer manually
		controller.dataInformerFactory.Datadependency().V1alpha1().DataSourceClaims().Informer().GetIndexer().Add(dsc)

		// 3. Create DS and Sync Indexer
		_, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSources().Create(ctx, ds, metav1.CreateOptions{})
		assert.NoError(t, err, "Failed to create DataSource")

		// Add DS to Indexer manually
		controller.dataInformerFactory.Datadependency().V1alpha1().DataSources().Informer().GetIndexer().Add(ds)

		// Get the object key
		key, _ := cache.MetaNamespaceKeyFunc(dsc)

		// 4. Run Reconcile
		// Logic: handleDeletion -> checks finalizer -> removes ref from DS -> removes finalizer from DSC
		err = controller.Reconcile(key)
		assert.NoError(t, err, "Reconcile should not return an error during deletion")

		// 5. Verify Results
		// Verify that the claim reference was removed from DataSource
		updatedDS, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSources().Get(ctx, ds.Name, metav1.GetOptions{})
		assert.NoError(t, err, "Failed to get updated DataSource")
		assert.Equal(t, 0, len(updatedDS.Status.ClaimRefs), "ClaimRefs should be empty after DSC deletion")
		assert.Equal(t, int32(0), updatedDS.Status.BoundClaims, "BoundClaims should be 0 after DSC deletion")

		// Verify that the finalizer was removed from DSC
		updatedDSC, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSourceClaims(dsc.Namespace).Get(ctx, dsc.Name, metav1.GetOptions{})
		assert.NoError(t, err, "Failed to get updated DSC")
		assert.NotContains(t, updatedDSC.Finalizers, DataSourceClaimFinalizer, "Finalizer should be removed after cleanup")

		// Verify DataSource still exists (ReclaimPolicy is Retain)
		_, err = controller.datadependencyClient.DatadependencyV1alpha1().DataSources().Get(ctx, ds.Name, metav1.GetOptions{})
		assert.NoError(t, err, "DataSource should still exist with ReclaimPolicy Retain")
	})

	// Test case 11: DS Locality change should trigger DSC requeue and RB rescheduling
	t.Run("Scenario 11: DS Locality change should trigger DSC requeue and RB rescheduling", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Create a DSC that is bound to a DataSource
		dsc := &v1alpha1.DataSourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "test-dsc-locality-change",
				Namespace:  "default",
				UID:        "test-dsc-locality-change-uid",
				Finalizers: []string{DataSourceClaimFinalizer},
			},
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "dc1.db1.orders",
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "locality-test-app",
					Namespace:  "default",
				},
			},
			Status: v1alpha1.DataSourceClaimStatus{
				Phase:           v1alpha1.DSCPhaseBound,
				BoundDataSource: "test-ds-locality-change",
			},
		}

		// Create a DataSource that is bound to this DSC
		// Initial Locality: cluster-loc-1, cluster-loc-2
		originalDS := &v1alpha1.DataSource{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-ds-locality-change",
			},
			Spec: v1alpha1.DataSourceSpec{
				System: "amoro",
				Type:   "table",
				Name:   "dc1.db1.orders",
				Locality: &v1alpha1.DataSourceLocality{
					ClusterNames: []string{"cluster-loc-1", "cluster-loc-2"},
				},
			},
			Status: v1alpha1.DataSourceStatus{
				ClaimRefs: []corev1.ObjectReference{
					{
						Kind:      "DataSourceClaim",
						Name:      dsc.Name,
						Namespace: dsc.Namespace,
						UID:       dsc.UID,
					},
				},
				BoundClaims: 1,
			},
		}

		// Create clusters for testing
		cluster1 := &clusterv1alpha1.Cluster{
			ObjectMeta: metav1.ObjectMeta{Name: "cluster-loc-1"},
		}
		cluster2 := &clusterv1alpha1.Cluster{
			ObjectMeta: metav1.ObjectMeta{Name: "cluster-loc-2"},
		}
		cluster3 := &clusterv1alpha1.Cluster{
			ObjectMeta: metav1.ObjectMeta{Name: "cluster-loc-3"},
		}

		// Create a ResourceBinding that has been injected with placement affinity
		// Initial Exclusion: cluster-loc-3 (because DS is on 1 & 2)
		rb := &workv1alpha2.ResourceBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "locality-test-app-deployment",
				Namespace: "default",
				Annotations: map[string]string{
					PlacementInjectedAnnotation: "true",
					ExcludedClustersAnnotation:  "cluster-loc-3",
				},
			},
			Spec: workv1alpha2.ResourceBindingSpec{
				Resource: workv1alpha2.ObjectReference{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "locality-test-app",
					Namespace:  "default",
				},
				Placement: &policyv1alpha1.Placement{
					ClusterAffinity: &policyv1alpha1.ClusterAffinity{
						ExcludeClusters: []string{"cluster-loc-3"},
					},
				},
			},
		}

		controller := setupTestController(ctx, dsc)

		// IMPORTANT: Do NOT call startTestController.
		// We only start informers to sync cache but avoid starting background workers
		// to prevent race conditions when reading from the queue manually.
		controller.karmadaInformerFactory.Start(ctx.Done())
		controller.dataInformerFactory.Start(ctx.Done())

		// Wait for cache sync
		cache.WaitForCacheSync(ctx.Done(),
			controller.dscListerSynced,
			controller.dsListerSynced,
			controller.rbListerSynced,
			controller.cListerSynced,
		)

		// Create clusters in fake client
		controller.karmadaClient.ClusterV1alpha1().Clusters().Create(ctx, cluster1, metav1.CreateOptions{})
		controller.karmadaClient.ClusterV1alpha1().Clusters().Create(ctx, cluster2, metav1.CreateOptions{})
		controller.karmadaClient.ClusterV1alpha1().Clusters().Create(ctx, cluster3, metav1.CreateOptions{})

		// Manually add clusters to the Informer Indexer to ensure c.cLister.List() returns them immediately
		// without waiting for the async watch event.
		controller.karmadaInformerFactory.Cluster().V1alpha1().Clusters().Informer().GetIndexer().Add(cluster1)
		controller.karmadaInformerFactory.Cluster().V1alpha1().Clusters().Informer().GetIndexer().Add(cluster2)
		controller.karmadaInformerFactory.Cluster().V1alpha1().Clusters().Informer().GetIndexer().Add(cluster3)

		// Create the DataSource and ResourceBinding in the fake client
		_, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSources().Create(ctx, originalDS, metav1.CreateOptions{})
		assert.NoError(t, err, "Failed to create DataSource")

		_, err = controller.karmadaClient.WorkV1alpha2().ResourceBindings(rb.Namespace).Create(ctx, rb, metav1.CreateOptions{})
		assert.NoError(t, err, "Failed to create ResourceBinding")

		// Add RB to indexer to ensure triggerRescheduling finds it
		controller.karmadaInformerFactory.Work().V1alpha2().ResourceBindings().Informer().GetIndexer().Add(rb)

		// Add initial DS to indexer
		controller.dataInformerFactory.Datadependency().V1alpha1().DataSources().Informer().GetIndexer().Add(originalDS)

		// Simulate DS Locality change
		// New Locality: cluster-loc-2, cluster-loc-3
		// Expected Exclusion: cluster-loc-1
		updatedDS := originalDS.DeepCopy()
		updatedDS.Spec.Locality.ClusterNames = []string{"cluster-loc-2", "cluster-loc-3"}

		// Update the DataSource in the fake client
		_, err = controller.datadependencyClient.DatadependencyV1alpha1().DataSources().Update(ctx, updatedDS, metav1.UpdateOptions{})
		assert.NoError(t, err, "Failed to update DataSource")

		// Manually update the DS in the Informer Indexer.
		// Since we are not running the full async loop, the Lister used inside Reconcile
		// will still see 'originalDS' unless we update the cache explicitly.
		controller.dataInformerFactory.Datadependency().V1alpha1().DataSources().Informer().GetIndexer().Update(updatedDS)

		// Trigger the updateDataSource event handler manually
		controller.updateDataSource(originalDS, updatedDS)

		// Verify that the DSC was enqueued due to DS Locality change
		assert.Greater(t, controller.queue.Len(), 0, "Queue should contain DSC due to DS Locality change")

		// Get the DSC key from queue
		item, shutdown := controller.queue.Get()
		assert.False(t, shutdown, "Queue should not be shut down")
		expectedKey := "default/test-dsc-locality-change"
		assert.Equal(t, expectedKey, item, "Queue should contain the correct DSC key")
		controller.queue.Done(item)

		// Run reconcile - should trigger handleBound -> triggerRescheduling
		err = controller.Reconcile(expectedKey)
		assert.NoError(t, err, "Reconcile should not return an error")
		// Verify that the ResourceBinding was updated with new placement affinity
		updatedRB, err := controller.karmadaClient.WorkV1alpha2().ResourceBindings(rb.Namespace).Get(ctx, rb.Name, metav1.GetOptions{})
		assert.NoError(t, err, "Failed to get updated ResourceBinding")

		// The ExcludeClusters should now exclude cluster-loc-1 (since DS now includes cluster-loc-2 and cluster-loc-3)
		assert.Contains(t, updatedRB.Spec.Placement.ClusterAffinity.ExcludeClusters, "cluster-loc-1", "ResourceBinding should exclude cluster-loc-1 after DS Locality change")
		assert.NotContains(t, updatedRB.Spec.Placement.ClusterAffinity.ExcludeClusters, "cluster-loc-2", "ResourceBinding should not exclude cluster-loc-2 after DS Locality change")
		assert.NotContains(t, updatedRB.Spec.Placement.ClusterAffinity.ExcludeClusters, "cluster-loc-3", "ResourceBinding should not exclude cluster-loc-3 after DS Locality change")

		// Verify that the ExcludedClustersAnnotation was updated
		assert.Equal(t, "cluster-loc-1", updatedRB.Annotations[ExcludedClustersAnnotation], "ExcludedClustersAnnotation should be updated")

		// Verify DSC status remains bound
		updatedDSC, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSourceClaims(dsc.Namespace).Get(ctx, dsc.Name, metav1.GetOptions{})
		assert.NoError(t, err, "Failed to get updated DSC")
		assert.Equal(t, v1alpha1.DSCPhaseBound, updatedDSC.Status.Phase, "DSC should remain bound after DS Locality change")
		assert.Equal(t, "test-ds-locality-change", updatedDSC.Status.BoundDataSource, "DSC should remain bound to the same DataSource")
	})

	// Test case 12: DS deletion should trigger DSC requeue and unbinding
	t.Run("Scenario 12: DS deletion should trigger DSC requeue and unbinding", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// 1. Setup Data
		dsc := &v1alpha1.DataSourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "test-dsc-ds-deletion",
				Namespace:  "default",
				UID:        "test-dsc-ds-deletion-uid",
				Finalizers: []string{DataSourceClaimFinalizer},
			},
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "dc1.db1.orders",
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "default",
				},
			},
			Status: v1alpha1.DataSourceClaimStatus{
				Phase:           v1alpha1.DSCPhaseBound,
				BoundDataSource: "test-ds-to-delete",
			},
		}

		originalDS := &v1alpha1.DataSource{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "test-ds-to-delete",
				Finalizers: []string{DataSourceFinalizer},
			},
			Spec: v1alpha1.DataSourceSpec{
				System: "amoro",
				Type:   "table",
				Name:   "dc1.db1.orders",
				Locality: &v1alpha1.DataSourceLocality{
					ClusterNames: []string{"cluster1", "cluster2"},
				},
			},
			Status: v1alpha1.DataSourceStatus{
				ClaimRefs: []corev1.ObjectReference{
					{
						Kind:      "DataSourceClaim",
						Name:      dsc.Name,
						Namespace: dsc.Namespace,
						UID:       dsc.UID,
					},
				},
				BoundClaims: 1,
			},
		}

		// 2. Setup Controller
		controller := setupTestController(ctx, dsc)

		// Start Informers manually
		controller.karmadaInformerFactory.Start(ctx.Done())
		controller.dataInformerFactory.Start(ctx.Done()) // Check your variable name (dataInformerFactory vs datadependencyInformerFactory)

		cache.WaitForCacheSync(ctx.Done(),
			controller.dscListerSynced,
			controller.dsListerSynced,
		)

		// Add DSC to Indexer
		controller.dataInformerFactory.Datadependency().V1alpha1().DataSourceClaims().Informer().GetIndexer().Add(dsc)

		// 3. Create DS and Sync Indexer
		_, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSources().Create(ctx, originalDS, metav1.CreateOptions{})
		assert.NoError(t, err, "Failed to create DataSource")
		// Add DS to Indexer
		controller.dataInformerFactory.Datadependency().V1alpha1().DataSources().Informer().GetIndexer().Add(originalDS)

		// 4. Simulate DS deletion
		dsToDelete := originalDS.DeepCopy()
		now := metav1.Now()
		dsToDelete.DeletionTimestamp = &now

		_, err = controller.datadependencyClient.DatadependencyV1alpha1().DataSources().Update(ctx, dsToDelete, metav1.UpdateOptions{})
		assert.NoError(t, err, "Failed to update DataSource with DeletionTimestamp")

		// Manually update the Indexer with the deleted DS
		// Reconcile -> handleBound -> finds DS from Cache.
		// If Cache isn't updated, it won't see DeletionTimestamp and won't trigger unbinding.
		controller.dataInformerFactory.Datadependency().V1alpha1().DataSources().Informer().GetIndexer().Update(dsToDelete)

		// 5. Run Reconcile (Sync Phase)
		// We pass the key manually. We don't need the queue.
		expectedKey := "default/test-dsc-ds-deletion"
		err = controller.Reconcile(expectedKey)
		assert.NoError(t, err, "Reconcile should not return an error")

		// 6. Verify Intermediate State (Sync Phase Result)
		updatedDSC, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSourceClaims(dsc.Namespace).Get(ctx, dsc.Name, metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Equal(t, v1alpha1.DSCPhasePending, updatedDSC.Status.Phase, "DSC should be reset to Pending")
		assert.Empty(t, updatedDSC.Status.BoundDataSource)

		updatedDS, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSources().Get(ctx, originalDS.Name, metav1.GetOptions{})
		assert.NoError(t, err)

		// ClaimRefs SHOULD be empty now.
		assert.Empty(t, updatedDS.Status.ClaimRefs, "ClaimRefs should be emptied even if DS is deleting")

		// Finalizer SHOULD still exist here.
		assert.Contains(t, updatedDS.Finalizers, DataSourceFinalizer, "Finalizer should persist until async update handler runs")

		// 7. Run Async Phase (Simulate updateDataSource)
		// dsToDelete is state 'before' claim removal (has refs, has TS)
		// updatedDS is state 'after' (no refs, has TS)
		controller.updateDataSource(dsToDelete, updatedDS)

		// 8. Verify Final State
		finalDS, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSources().Get(ctx, originalDS.Name, metav1.GetOptions{})
		assert.NoError(t, err)

		// Now the Finalizer should be gone
		assert.Empty(t, finalDS.Finalizers, "DataSource finalizer should be removed after updateDataSource handles the zero-ref state")

		// Helper verification
		// Verify that doesDataSourceMatchClaim returns false for DS with DeletionTimestamp
		matchResult := doesDataSourceMatchClaim(dsc, finalDS)
		assert.False(t, matchResult, "doesDataSourceMatchClaim should return false for DS with DeletionTimestamp")
	})
}

func TestHandleDeletion(t *testing.T) {
	t.Run("Finalizer already removed - should return immediately", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Create a DSC without the finalizer
		dsc := &v1alpha1.DataSourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-dsc",
				Namespace: "default",
				UID:       "test-uid",
				// No finalizer present
			},
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "test-system",
				DataSourceType: "test-type",
				DataSourceName: "test-name",
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "default",
				},
			},
			Status: v1alpha1.DataSourceClaimStatus{
				Phase:           v1alpha1.DSCPhaseBound,
				BoundDataSource: "test-ds",
			},
		}

		controller := setupTestController(ctx, dsc)

		// Call handleDeletion
		err := controller.handleDeletion(dsc)

		// Should return nil without any errors
		assert.NoError(t, err)
	})

	t.Run("Bound DataSource cleanup - should remove claim reference", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Create a DSC with finalizer and bound DataSource
		dsc := &v1alpha1.DataSourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "test-dsc",
				Namespace:  "default",
				UID:        "test-uid",
				Finalizers: []string{DataSourceClaimFinalizer},
			},
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "test-system",
				DataSourceType: "test-type",
				DataSourceName: "test-name",
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "default",
				},
			},
			Status: v1alpha1.DataSourceClaimStatus{
				Phase:           v1alpha1.DSCPhaseBound,
				BoundDataSource: "test-ds",
			},
		}

		// Create a DataSource with the claim reference
		ds := &v1alpha1.DataSource{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-ds",
			},
			Spec: v1alpha1.DataSourceSpec{
				System:        "test-system",
				Type:          "test-type",
				Name:          "test-name",
				ReclaimPolicy: v1alpha1.ReclaimPolicyRetain,
			},
			Status: v1alpha1.DataSourceStatus{
				ClaimRefs: []corev1.ObjectReference{
					{
						Name:      dsc.Name,
						Namespace: dsc.Namespace,
						UID:       dsc.UID,
					},
				},
				BoundClaims: 1,
			},
		}

		controller := setupTestController(ctx, dsc)

		// Create the DataSource in the fake client
		_, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSources().Create(ctx, ds, metav1.CreateOptions{})
		assert.NoError(t, err)

		// Call handleDeletion
		err = controller.handleDeletion(dsc)
		assert.NoError(t, err)

		// Verify that the claim reference was removed from DataSource
		updatedDS, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSources().Get(ctx, ds.Name, metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Equal(t, 0, len(updatedDS.Status.ClaimRefs))
		assert.Equal(t, int32(0), updatedDS.Status.BoundClaims)

		// Verify that the finalizer was removed from DSC
		updatedDSC, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSourceClaims(dsc.Namespace).Get(ctx, dsc.Name, metav1.GetOptions{})
		assert.NoError(t, err)
		assert.NotContains(t, updatedDSC.Finalizers, DataSourceClaimFinalizer)
	})

	t.Run("Bound DataSource not found - should warn but not block", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Create a DSC with finalizer and bound DataSource that doesn't exist
		dsc := &v1alpha1.DataSourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "test-dsc",
				Namespace:  "default",
				UID:        "test-uid",
				Finalizers: []string{DataSourceClaimFinalizer},
			},
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "test-system",
				DataSourceType: "test-type",
				DataSourceName: "test-name",
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "default",
				},
			},
			Status: v1alpha1.DataSourceClaimStatus{
				Phase:           v1alpha1.DSCPhaseBound,
				BoundDataSource: "non-existent-ds",
			},
		}

		controller := setupTestController(ctx, dsc)

		// Call handleDeletion (DataSource doesn't exist)
		err := controller.handleDeletion(dsc)
		assert.NoError(t, err)

		// Verify that the finalizer was still removed from DSC
		updatedDSC, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSourceClaims(dsc.Namespace).Get(ctx, dsc.Name, metav1.GetOptions{})
		assert.NoError(t, err)
		assert.NotContains(t, updatedDSC.Finalizers, DataSourceClaimFinalizer)
	})

	t.Run("ReclaimPolicy Delete - should delete DataSource when no references", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Create a DSC with finalizer and bound DataSource
		dsc := &v1alpha1.DataSourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "test-dsc",
				Namespace:  "default",
				UID:        "test-uid",
				Finalizers: []string{DataSourceClaimFinalizer},
			},
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "test-system",
				DataSourceType: "test-type",
				DataSourceName: "test-name",
			},
			Status: v1alpha1.DataSourceClaimStatus{
				Phase:           v1alpha1.DSCPhaseBound,
				BoundDataSource: "test-ds-delete",
			},
		}

		// Create a DataSource with ReclaimPolicy Delete
		ds := &v1alpha1.DataSource{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-ds-delete",
			},
			Spec: v1alpha1.DataSourceSpec{
				System:        "test-system",
				Type:          "test-type",
				Name:          "test-name",
				ReclaimPolicy: v1alpha1.ReclaimPolicyDelete,
			},
			Status: v1alpha1.DataSourceStatus{
				ClaimRefs: []corev1.ObjectReference{
					{
						Name:      dsc.Name,
						Namespace: dsc.Namespace,
						UID:       dsc.UID,
					},
				},
				BoundClaims: 1,
			},
		}

		controller := setupTestController(ctx, dsc)

		// Create the DataSource in the fake client
		_, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSources().Create(ctx, ds, metav1.CreateOptions{})
		assert.NoError(t, err)

		// Call handleDeletion
		err = controller.handleDeletion(dsc)
		assert.NoError(t, err)

		// Verify that the DataSource was deleted
		_, err = controller.datadependencyClient.DatadependencyV1alpha1().DataSources().Get(ctx, ds.Name, metav1.GetOptions{})
		assert.Error(t, err)
		assert.True(t, apierrors.IsNotFound(err))

		// Verify that the finalizer was removed from DSC
		updatedDSC, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSourceClaims(dsc.Namespace).Get(ctx, dsc.Name, metav1.GetOptions{})
		assert.NoError(t, err)
		assert.NotContains(t, updatedDSC.Finalizers, DataSourceClaimFinalizer)
	})

	t.Run("No bound DataSource - should only remove finalizer", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Create a DSC with finalizer but no bound DataSource
		dsc := &v1alpha1.DataSourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "test-dsc",
				Namespace:  "default",
				UID:        "test-uid",
				Finalizers: []string{DataSourceClaimFinalizer},
			},
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "test-system",
				DataSourceType: "test-type",
				DataSourceName: "test-name",
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "default",
				},
			},
			Status: v1alpha1.DataSourceClaimStatus{
				Phase: v1alpha1.DSCPhasePending,
				// No BoundDataSource
			},
		}

		controller := setupTestController(ctx, dsc)

		// Call handleDeletion
		err := controller.handleDeletion(dsc)
		assert.NoError(t, err)

		// Verify that the finalizer was removed from DSC
		updatedDSC, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSourceClaims(dsc.Namespace).Get(ctx, dsc.Name, metav1.GetOptions{})
		assert.NoError(t, err)
		assert.NotContains(t, updatedDSC.Finalizers, DataSourceClaimFinalizer)
	})
}

func TestHandlePending(t *testing.T) {
	t.Run("Static match found - should call staticBinding", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// 1. Setup Objects
		dsc := &v1alpha1.DataSourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-dsc",
				Namespace: "default",
				UID:       "dsc-uid-123",
			},
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "test-table",
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "default",
				},
				Attributes: map[string]string{
					"catalog": "test-catalog",
				},
			},
			Status: v1alpha1.DataSourceClaimStatus{
				Phase: v1alpha1.DSCPhasePending,
			},
		}

		ds := &v1alpha1.DataSource{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-ds",
			},
			Spec: v1alpha1.DataSourceSpec{
				System: "amoro",
				Type:   "table",
				Name:   "test-table",
				Attributes: map[string]string{
					"catalog": "test-catalog",
				},
			},
		}

		// 2. Setup Controller
		controller := setupTestController(ctx, dsc)

		// Start Informers manually, DO NOT start background workers
		controller.karmadaInformerFactory.Start(ctx.Done())
		controller.dataInformerFactory.Start(ctx.Done())

		// Wait for sync
		cache.WaitForCacheSync(ctx.Done(),
			controller.dscListerSynced,
			controller.dsListerSynced,
		)

		// 3. Create Objects in Client AND Indexer
		// Create DS
		_, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSources().Create(ctx, ds, metav1.CreateOptions{})
		assert.NoError(t, err, "Failed to create DataSource in fake client")

		// Manually add DS to Indexer so findMatchedDS can see it immediately
		controller.dataInformerFactory.Datadependency().V1alpha1().DataSources().Informer().GetIndexer().Add(ds)

		// Manually add DSC to Indexer (good practice)
		controller.dataInformerFactory.Datadependency().V1alpha1().DataSourceClaims().Informer().GetIndexer().Add(dsc)

		// 4. Run Logic (handlePending)
		// It will call findMatchedDS -> list from cache -> found -> staticBinding
		err = controller.handlePending(dsc)
		assert.NoError(t, err)

		// 5. Verify Results
		updatedDSC, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSourceClaims(dsc.Namespace).Get(ctx, dsc.Name, metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Equal(t, v1alpha1.DSCPhaseBound, updatedDSC.Status.Phase)
		assert.Equal(t, ds.Name, updatedDSC.Status.BoundDataSource)

		// Verify conditions
		foundBoundCondition := false
		for _, condition := range updatedDSC.Status.Conditions {
			if condition.Type == "Bound" && condition.Status == metav1.ConditionTrue && condition.Reason == "BindingSuccessful" {
				foundBoundCondition = true
				break
			}
		}
		assert.True(t, foundBoundCondition, "Expected to find a 'Bound' condition")
	})

	t.Run("No static match - should call dynamicBinding", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// 1. Setup Objects
		dsc := &v1alpha1.DataSourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-dsc",
				Namespace: "default",
				UID:       "dsc-uid-123",
			},
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "test-table",
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "default",
				},
				Attributes: map[string]string{
					"catalog": "test-catalog",
				},
			},
			Status: v1alpha1.DataSourceClaimStatus{
				Phase: v1alpha1.DSCPhasePending,
			},
		}

		// Non-matching DS
		ds := &v1alpha1.DataSource{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-ds",
			},
			Spec: v1alpha1.DataSourceSpec{
				System: "different-system", // Mismatch
				Type:   "table",
				Name:   "different-table",
			},
		}

		// 2. Setup Controller & Mock
		mockPluginManager := NewMockPluginManager(func(ctx context.Context, dsc *v1alpha1.DataSourceClaim) ([]string, error) {
			return []string{"cluster1", "cluster2"}, nil
		})

		controller := setupTestController(ctx, dsc)
		controller.pluginManager = mockPluginManager

		// Start Informers manually
		controller.karmadaInformerFactory.Start(ctx.Done())
		controller.dataInformerFactory.Start(ctx.Done())

		// Wait for sync
		cache.WaitForCacheSync(ctx.Done(),
			controller.dscListerSynced,
			controller.dsListerSynced,
		)

		// 3. Create Objects in Client AND Indexer
		// Create non-matching DS
		_, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSources().Create(ctx, ds, metav1.CreateOptions{})
		assert.NoError(t, err, "Failed to create DataSource in fake client")

		// Manually add non-matching DS to Indexer
		// This ensures findMatchedDS sees it but correctly ignores it due to mismatch
		controller.dataInformerFactory.Datadependency().V1alpha1().DataSources().Informer().GetIndexer().Add(ds)

		// Manually add DSC to Indexer
		controller.dataInformerFactory.Datadependency().V1alpha1().DataSourceClaims().Informer().GetIndexer().Add(dsc)

		// 4. Run Logic (handlePending)
		// It will call findMatchedDS -> list cache -> no match -> dynamicBinding
		err = controller.handlePending(dsc)
		assert.NoError(t, err)

		// 5. Verify Results
		dsList, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSources().List(ctx, metav1.ListOptions{})
		assert.NoError(t, err)
		assert.Len(t, dsList.Items, 2) // Original DS + Created DS

		// Identify new DS
		var newDS *v1alpha1.DataSource
		for _, item := range dsList.Items {
			if item.Name != "test-ds" {
				newDS = &item
				break
			}
		}
		assert.NotNil(t, newDS, "Expected a new DataSource to be created")
		assert.Equal(t, dsc.Spec.System, newDS.Spec.System)
		assert.Equal(t, dsc.Spec.DataSourceType, newDS.Spec.Type)
		assert.Equal(t, dsc.Spec.DataSourceName, newDS.Spec.Name)
		assert.Equal(t, dsc.Spec.Attributes, newDS.Spec.Attributes)

		// Verify DSC Binding
		updatedDSC, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSourceClaims(dsc.Namespace).Get(ctx, dsc.Name, metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Equal(t, v1alpha1.DSCPhaseBound, updatedDSC.Status.Phase)
		assert.Equal(t, newDS.Name, updatedDSC.Status.BoundDataSource)
	})
}

func TestHandleBound(t *testing.T) {
	t.Run("should call handlePlacementUpdate when binding is still valid", func(t *testing.T) {
		ctx := context.Background()

		// Create test DSC (bound state)
		dsc := &v1alpha1.DataSourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-dsc",
				Namespace: "default",
				UID:       "test-dsc-uid",
			},
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "dc1.db1.orders",
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Namespace:  "default",
					Name:       "test-app",
				},
			},
			Status: v1alpha1.DataSourceClaimStatus{
				Phase:           v1alpha1.DSCPhaseBound,
				BoundDataSource: "test-ds",
			},
		}

		// Create test DataSource that matches the DSC
		ds := &v1alpha1.DataSource{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-ds",
			},
			Spec: v1alpha1.DataSourceSpec{
				System: "amoro",
				Type:   "table",
				Name:   "dc1.db1.orders",
				Locality: &v1alpha1.DataSourceLocality{
					ClusterNames: []string{"cluster1", "cluster2"},
				},
			},
			Status: v1alpha1.DataSourceStatus{
				ClaimRefs: []corev1.ObjectReference{
					{
						Kind:      "DataSourceClaim",
						Name:      "test-dsc",
						Namespace: "default",
						UID:       "test-dsc-uid",
					},
				},
			},
		}

		// Setup test controller
		controller := setupTestController(ctx, dsc)

		// Add DataSource to the fake client
		controller.datadependencyClient.DatadependencyV1alpha1().DataSources().Create(ctx, ds, metav1.CreateOptions{})

		// Call handleBound
		err := controller.handleBound(dsc)
		assert.NoError(t, err)

		// Since no ResourceBinding exists, handlePlacementUpdate should return nil
		// We verify this by checking that no error occurred and DSC remains bound
		updatedDSC, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSourceClaims("default").Get(ctx, "test-dsc", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Equal(t, v1alpha1.DSCPhaseBound, updatedDSC.Status.Phase)
		assert.Equal(t, "test-ds", updatedDSC.Status.BoundDataSource)
	})

	t.Run("should call handleUnbinding when binding is no longer valid", func(t *testing.T) {
		ctx := context.Background()

		// Create test DSC (bound state)
		dsc := &v1alpha1.DataSourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-dsc",
				Namespace: "default",
				UID:       "test-dsc-uid",
			},
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "dc1.db1.orders",
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Namespace:  "default",
					Name:       "test-app",
				},
			},
			Status: v1alpha1.DataSourceClaimStatus{
				Phase:           v1alpha1.DSCPhaseBound,
				BoundDataSource: "test-ds",
			},
		}

		// Create test DataSource that does NOT match the DSC (different system)
		ds := &v1alpha1.DataSource{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-ds",
			},
			Spec: v1alpha1.DataSourceSpec{
				System: "different-system", // This makes the binding invalid
				Type:   "table",
				Name:   "dc1.db1.orders",
				Locality: &v1alpha1.DataSourceLocality{
					ClusterNames: []string{"cluster1", "cluster2"},
				},
			},
			Status: v1alpha1.DataSourceStatus{
				ClaimRefs: []corev1.ObjectReference{
					{
						Kind:      "DataSourceClaim",
						Name:      "test-dsc",
						Namespace: "default",
						UID:       "test-dsc-uid",
					},
				},
			},
		}

		// Setup test controller
		controller := setupTestController(ctx, dsc)

		// Add DataSource to the fake client
		controller.datadependencyClient.DatadependencyV1alpha1().DataSources().Create(ctx, ds, metav1.CreateOptions{})

		// Call handleBound
		err := controller.handleBound(dsc)
		assert.NoError(t, err)

		// Verify DSC status was reset to Pending (unbinding occurred)
		updatedDSC, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSourceClaims("default").Get(ctx, "test-dsc", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Equal(t, v1alpha1.DSCPhasePending, updatedDSC.Status.Phase)
		assert.Empty(t, updatedDSC.Status.BoundDataSource)

		// Verify DataSource claim ref was removed
		updatedDS, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSources().Get(ctx, "test-ds", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Empty(t, updatedDS.Status.ClaimRefs)
	})

	t.Run("should handle DataSource not found error gracefully", func(t *testing.T) {
		ctx := context.Background()

		// Create test DSC (bound state)
		dsc := &v1alpha1.DataSourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-dsc",
				Namespace: "default",
				UID:       "test-dsc-uid",
			},
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "dc1.db1.orders",
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Namespace:  "default",
					Name:       "test-app",
				},
			},
			Status: v1alpha1.DataSourceClaimStatus{
				Phase:           v1alpha1.DSCPhaseBound,
				BoundDataSource: "non-existent-ds", // DataSource doesn't exist
			},
		}

		// Setup test controller
		controller := setupTestController(ctx, dsc)

		// Call handleBound - should return error since DataSource doesn't exist
		err := controller.handleBound(dsc)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("should call handleUnbinding when DataSource has DeletionTimestamp", func(t *testing.T) {
		ctx := context.Background()
		now := metav1.Now()

		// 1. Setup Data
		dsc := &v1alpha1.DataSourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-dsc",
				Namespace: "default",
				UID:       "test-dsc-uid",
			},
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "dc1.db1.orders",
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Namespace:  "default",
					Name:       "test-app",
				},
			},
			Status: v1alpha1.DataSourceClaimStatus{
				Phase:           v1alpha1.DSCPhaseBound,
				BoundDataSource: "test-ds",
			},
		}

		ds := &v1alpha1.DataSource{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "test-ds",
				DeletionTimestamp: &now,                          // DataSource is being deleted
				Finalizers:        []string{DataSourceFinalizer}, // Add finalizer
			},
			Spec: v1alpha1.DataSourceSpec{
				System: "amoro",
				Type:   "table",
				Name:   "dc1.db1.orders",
				Locality: &v1alpha1.DataSourceLocality{
					ClusterNames: []string{"cluster1", "cluster2"},
				},
			},
			Status: v1alpha1.DataSourceStatus{
				ClaimRefs: []corev1.ObjectReference{
					{
						Kind:      "DataSourceClaim",
						Name:      "test-dsc",
						Namespace: "default",
						UID:       "test-dsc-uid",
					},
				},
			},
		}

		// 2. Setup Controller
		controller := setupTestController(ctx, dsc)
		// Add DataSource to the fake client
		controller.datadependencyClient.DatadependencyV1alpha1().DataSources().Create(ctx, ds, metav1.CreateOptions{})

		// 3. Action: Call handleBound
		// This triggers: doesDataSourceMatchClaim(false) -> handleUnbinding -> removeClaimRefFromDS
		err := controller.handleBound(dsc)
		assert.NoError(t, err)

		// 4. Verify DSC Status (Reset to Pending)
		updatedDSC, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSourceClaims("default").Get(ctx, "test-dsc", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Equal(t, v1alpha1.DSCPhasePending, updatedDSC.Status.Phase)
		assert.Empty(t, updatedDSC.Status.BoundDataSource)

		// 5. Verify DS Status (Sync Phase)
		updatedDS, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSources().Get(ctx, "test-ds", metav1.GetOptions{})
		assert.NoError(t, err)

		// ClaimRefs MUST be empty.
		// logic: handleUnbinding calls removeClaimRefFromDS, which removes the ref.
		assert.Empty(t, updatedDS.Status.ClaimRefs, "ClaimRefs should be removed to acknowledge unbinding")

		// Finalizer MUST still exist.
		// logic: removeClaimRefFromDS no longer touches finalizers.
		assert.Contains(t, updatedDS.Finalizers, DataSourceFinalizer, "Finalizer should persist until async update handler runs")

		// 6. Action: Simulate Async Update (updateDataSource)
		// logic: K8s sends event -> updateDataSource sees (DeletionTimestamp != nil AND refs == 0) -> removes finalizer
		controller.updateDataSource(ds, updatedDS)

		// 7. Verify DS Status (Final Phase)
		finalDS, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSources().Get(ctx, "test-ds", metav1.GetOptions{})
		assert.NoError(t, err)

		// Finalizer should be gone now
		assert.NotContains(t, finalDS.Finalizers, DataSourceFinalizer, "Finalizer should be removed by updateDataSource logic")
	})
}

func TestAttributesEqual(t *testing.T) {
	t.Run("Both maps are nil", func(t *testing.T) {
		result := attributesEqual(nil, nil)
		assert.True(t, result, "Two nil maps should be equal")
	})

	t.Run("One map is nil, other is not", func(t *testing.T) {
		a := map[string]string{"key": "value"}
		result1 := attributesEqual(nil, a)
		result2 := attributesEqual(a, nil)
		assert.False(t, result1, "nil and non-nil maps should not be equal")
		assert.False(t, result2, "non-nil and nil maps should not be equal")
	})

	t.Run("Both maps are empty", func(t *testing.T) {
		a := make(map[string]string)
		b := make(map[string]string)
		result := attributesEqual(a, b)
		assert.True(t, result, "Two empty maps should be equal")
	})

	t.Run("Maps with different lengths", func(t *testing.T) {
		a := map[string]string{"key1": "value1"}
		b := map[string]string{"key1": "value1", "key2": "value2"}
		result := attributesEqual(a, b)
		assert.False(t, result, "Maps with different lengths should not be equal")
	})

	t.Run("Maps with same key-value pairs", func(t *testing.T) {
		a := map[string]string{"key1": "value1", "key2": "value2"}
		b := map[string]string{"key2": "value2", "key1": "value1"}
		result := attributesEqual(a, b)
		assert.True(t, result, "Maps with same key-value pairs should be equal")
	})

	t.Run("Maps with different values for same keys", func(t *testing.T) {
		a := map[string]string{"key1": "value1", "key2": "value2"}
		b := map[string]string{"key1": "value1", "key2": "different"}
		result := attributesEqual(a, b)
		assert.False(t, result, "Maps with different values should not be equal")
	})

	t.Run("Maps with different keys", func(t *testing.T) {
		a := map[string]string{"key1": "value1"}
		b := map[string]string{"key2": "value1"}
		result := attributesEqual(a, b)
		assert.False(t, result, "Maps with different keys should not be equal")
	})
}

func TestDoesDataSourceMatchClaim(t *testing.T) {
	t.Run("Perfect match", func(t *testing.T) {
		dsc := &v1alpha1.DataSourceClaim{
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "dc1.db1.orders",
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "default",
				},
				Attributes: map[string]string{
					"property1": "value1",
					"property2": "value2",
				},
			},
		}

		ds := &v1alpha1.DataSource{
			Spec: v1alpha1.DataSourceSpec{
				System: "amoro",
				Type:   "table",
				Name:   "dc1.db1.orders",
				Attributes: map[string]string{
					"property1": "value1",
					"property2": "value2",
				},
			},
		}

		result := doesDataSourceMatchClaim(dsc, ds)
		assert.True(t, result, "DataSource should match DataSourceClaim")
	})

	t.Run("System mismatch", func(t *testing.T) {
		dsc := &v1alpha1.DataSourceClaim{
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "dc1.db1.orders",
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "default",
				},
			},
		}

		ds := &v1alpha1.DataSource{
			Spec: v1alpha1.DataSourceSpec{
				System: "hive",
				Type:   "table",
				Name:   "dc1.db1.orders",
			},
		}

		result := doesDataSourceMatchClaim(dsc, ds)
		assert.False(t, result, "DataSource with different system should not match")
	})

	t.Run("Type mismatch", func(t *testing.T) {
		dsc := &v1alpha1.DataSourceClaim{
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "dc1.db1.orders",
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "default",
				},
			},
		}

		ds := &v1alpha1.DataSource{
			Spec: v1alpha1.DataSourceSpec{
				System: "amoro",
				Type:   "view",
				Name:   "dc1.db1.orders",
			},
		}

		result := doesDataSourceMatchClaim(dsc, ds)
		assert.False(t, result, "DataSource with different type should not match")
	})

	t.Run("Name mismatch", func(t *testing.T) {
		dsc := &v1alpha1.DataSourceClaim{
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "dc1.db1.orders",
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "default",
				},
			},
		}

		ds := &v1alpha1.DataSource{
			Spec: v1alpha1.DataSourceSpec{
				System: "amoro",
				Type:   "table",
				Name:   "dc1.db1.customers",
			},
		}

		result := doesDataSourceMatchClaim(dsc, ds)
		assert.False(t, result, "DataSource with different name should not match")
	})

	t.Run("Attributes mismatch", func(t *testing.T) {
		dsc := &v1alpha1.DataSourceClaim{
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "dc1.db1.orders",
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "default",
				},
				Attributes: map[string]string{
					"property1": "value1",
				},
			},
		}

		ds := &v1alpha1.DataSource{
			Spec: v1alpha1.DataSourceSpec{
				System: "amoro",
				Type:   "table",
				Name:   "dc1.db1.orders",
				Attributes: map[string]string{
					"property1": "value2",
				},
			},
		}

		result := doesDataSourceMatchClaim(dsc, ds)
		assert.False(t, result, "DataSource with different attributes should not match")
	})

	t.Run("Match with nil attributes", func(t *testing.T) {
		dsc := &v1alpha1.DataSourceClaim{
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "dc1.db1.orders",
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "default",
				},
			},
		}

		ds := &v1alpha1.DataSource{
			Spec: v1alpha1.DataSourceSpec{
				System: "amoro",
				Type:   "table",
				Name:   "dc1.db1.orders",
			},
		}

		result := doesDataSourceMatchClaim(dsc, ds)
		assert.True(t, result, "DataSource should match when both have nil attributes")
	})

	t.Run("DataSource with DeletionTimestamp should not match", func(t *testing.T) {
		now := metav1.Now()
		dsc := &v1alpha1.DataSourceClaim{
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "dc1.db1.orders",
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "default",
				},
			},
		}

		ds := &v1alpha1.DataSource{
			ObjectMeta: metav1.ObjectMeta{
				DeletionTimestamp: &now, // DataSource is being deleted
			},
			Spec: v1alpha1.DataSourceSpec{
				System: "amoro",
				Type:   "table",
				Name:   "dc1.db1.orders",
			},
		}

		result := doesDataSourceMatchClaim(dsc, ds)
		assert.False(t, result, "DataSource with DeletionTimestamp should not match even if specs are identical")
	})
}

func TestFindMatchingDataSource(t *testing.T) {
	t.Run("Find matching DataSource", func(t *testing.T) {
		dsc := &v1alpha1.DataSourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-claim",
				Namespace: "default",
			},
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "dc1.db1.orders",
				Attributes: map[string]string{
					"property1": "value1",
				},
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "default",
				},
			},
		}

		dsList := []*v1alpha1.DataSource{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name: "ds1",
				},
				Spec: v1alpha1.DataSourceSpec{
					System: "hive",
					Type:   "table",
					Name:   "customers",
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name: "ds2",
				},
				Spec: v1alpha1.DataSourceSpec{
					System: "amoro",
					Type:   "table",
					Name:   "dc1.db1.orders",
					Attributes: map[string]string{
						"property1": "value1",
					},
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name: "ds3",
				},
				Spec: v1alpha1.DataSourceSpec{
					System: "amoro",
					Type:   "table",
					Name:   "dc1.db1.products",
				},
			},
		}

		result, err := findMatchingDataSource(dsc, dsList)
		assert.NoError(t, err, "findMatchingDataSource should not return an error")
		assert.NotNil(t, result, "Should find a matching DataSource")
		assert.Equal(t, "ds2", result.Name, "Should return the correct matching DataSource")
	})

	t.Run("No matching DataSource found", func(t *testing.T) {
		dsc := &v1alpha1.DataSourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-claim",
				Namespace: "default",
			},
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "dc1.db1.nonexistent",
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "default",
				},
			},
		}

		dsList := []*v1alpha1.DataSource{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name: "ds1",
				},
				Spec: v1alpha1.DataSourceSpec{
					System: "hive",
					Type:   "table",
					Name:   "customers",
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name: "ds2",
				},
				Spec: v1alpha1.DataSourceSpec{
					System: "amoro",
					Type:   "table",
					Name:   "dc1.db1.orders",
				},
			},
		}

		result, err := findMatchingDataSource(dsc, dsList)
		assert.NoError(t, err, "findMatchingDataSource should not return an error")
		assert.Nil(t, result, "Should not find any matching DataSource")
	})

	t.Run("Empty DataSource list", func(t *testing.T) {
		dsc := &v1alpha1.DataSourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-claim",
				Namespace: "default",
			},
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "dc1.db1.orders",
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "default",
				},
			},
		}

		var dsList []*v1alpha1.DataSource

		result, err := findMatchingDataSource(dsc, dsList)
		assert.NoError(t, err, "findMatchingDataSource should not return an error")
		assert.Nil(t, result, "Should not find any matching DataSource in empty list")
	})

	t.Run("Multiple matches - returns first", func(t *testing.T) {
		dsc := &v1alpha1.DataSourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-claim",
				Namespace: "default",
			},
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "dc1.db1.orders",
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "default",
				},
			},
		}

		dsList := []*v1alpha1.DataSource{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name: "ds1",
				},
				Spec: v1alpha1.DataSourceSpec{
					System: "amoro",
					Type:   "table",
					Name:   "dc1.db1.orders",
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name: "ds2",
				},
				Spec: v1alpha1.DataSourceSpec{
					System: "amoro",
					Type:   "table",
					Name:   "dc1.db1.orders",
				},
			},
		}

		result, err := findMatchingDataSource(dsc, dsList)
		assert.NoError(t, err, "findMatchingDataSource should not return an error")
		assert.NotNil(t, result, "Should find a matching DataSource")
		assert.Equal(t, "ds1", result.Name, "Should return the first matching DataSource")
	})
}

func TestStaticBinding(t *testing.T) {
	t.Run("Successful binding", func(t *testing.T) {
		ctx := context.Background()

		// Create test DSC
		dsc := &v1alpha1.DataSourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-claim",
				Namespace: "default",
				UID:       "test-uid-123",
			},
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "dc1.db1.orders",
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "default",
				},
			},
			Status: v1alpha1.DataSourceClaimStatus{
				Phase: v1alpha1.DSCPhasePending,
			},
		}

		// Create test DS
		ds := &v1alpha1.DataSource{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-datasource",
			},
			Spec: v1alpha1.DataSourceSpec{
				System: "amoro",
				Type:   "table",
				Name:   "dc1.db1.orders",
			},
		}

		controller := setupTestController(ctx, dsc)

		// Create a proper scheme with our custom types registered
		testScheme := runtime.NewScheme()
		v1alpha1.AddToScheme(testScheme)
		controller.Scheme = testScheme

		// Add the DataSource to the fake client to avoid "not found" warnings
		_, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSources().Create(ctx, ds, metav1.CreateOptions{})
		assert.NoError(t, err, "Failed to create DataSource in fake client")

		// Perform static binding - should succeed
		err = controller.staticBinding(dsc, ds)

		// Verify no error occurred
		assert.NoError(t, err, "staticBinding should succeed")

		// Verify DSC status was updated
		updatedDSC, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSourceClaims(dsc.Namespace).Get(ctx, dsc.Name, metav1.GetOptions{})
		assert.NoError(t, err, "Failed to get updated DSC")
		assert.Equal(t, v1alpha1.DSCPhaseBound, updatedDSC.Status.Phase, "DSC should be in Bound phase")
		assert.Equal(t, ds.Name, updatedDSC.Status.BoundDataSource, "BoundDataSource should be set")

		// Verify Bound condition is set
		foundBoundCondition := false
		for _, condition := range updatedDSC.Status.Conditions {
			if condition.Type == "Bound" {
				foundBoundCondition = true
				assert.Equal(t, metav1.ConditionTrue, condition.Status, "Bound condition should be True")
				break
			}
		}
		assert.True(t, foundBoundCondition, "Bound condition should be present")
	})

	t.Run("DSC status update failure", func(t *testing.T) {
		ctx := context.Background()

		// Create test DSC that exists in the fake client
		dsc := &v1alpha1.DataSourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-claim",
				Namespace: "default",
				UID:       "test-uid-123",
			},
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "dc1.db1.orders",
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "default",
				},
			},
			Status: v1alpha1.DataSourceClaimStatus{
				Phase: v1alpha1.DSCPhasePending,
			},
		}

		// Create test DS
		ds := &v1alpha1.DataSource{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-datasource",
			},
			Spec: v1alpha1.DataSourceSpec{
				System: "amoro",
				Type:   "table",
				Name:   "dc1.db1.orders",
			},
		}

		controller := setupTestController(ctx, dsc)

		// Delete the DSC from the fake client to trigger a NotFound error during status update
		err := controller.datadependencyClient.DatadependencyV1alpha1().DataSourceClaims(dsc.Namespace).Delete(ctx, dsc.Name, metav1.DeleteOptions{})
		assert.NoError(t, err, "Failed to delete DSC from fake client")

		// Perform static binding - should succeed because NotFound errors are treated as success in updateDSCstatus
		err = controller.staticBinding(dsc, ds)
		assert.NoError(t, err, "staticBinding should succeed even when DSC is not found (treated as success)")
	})

	t.Run("ClaimRef generation failure - binding still succeeds", func(t *testing.T) {
		ctx := context.Background()

		// Create test DSC with nil scheme to trigger ClaimRef generation failure
		dsc := &v1alpha1.DataSourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-claim",
				Namespace: "default",
				UID:       "test-uid-123",
			},
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "dc1.db1.orders",
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "default",
				},
			},
			Status: v1alpha1.DataSourceClaimStatus{
				Phase: v1alpha1.DSCPhasePending,
			},
		}

		// Create test DS
		ds := &v1alpha1.DataSource{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-datasource",
			},
			Spec: v1alpha1.DataSourceSpec{
				System: "amoro",
				Type:   "table",
				Name:   "dc1.db1.orders",
			},
		}

		controller := setupTestController(ctx, dsc)

		// Set scheme to empty scheme to trigger ClaimRef generation failure
		originalScheme := controller.Scheme
		controller.Scheme = runtime.NewScheme() // Empty scheme without DataSourceClaim type
		defer func() { controller.Scheme = originalScheme }()

		// Add the DataSource to the fake client
		_, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSources().Create(ctx, ds, metav1.CreateOptions{})
		assert.NoError(t, err, "Failed to create test DataSource")

		// Perform static binding - should succeed despite ClaimRef failure
		err = controller.staticBinding(dsc, ds)
		assert.NoError(t, err, "staticBinding should succeed even when ClaimRef generation fails")

		// Verify DSC status was still updated
		updatedDSC, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSourceClaims(dsc.Namespace).Get(ctx, dsc.Name, metav1.GetOptions{})
		assert.NoError(t, err, "Failed to get updated DSC")
		assert.Equal(t, v1alpha1.DSCPhaseBound, updatedDSC.Status.Phase, "DSC should be in Bound phase")
		assert.Equal(t, ds.Name, updatedDSC.Status.BoundDataSource, "DSC should reference the bound DataSource")
	})

	t.Run("DS back-reference update failure - binding still succeeds", func(t *testing.T) {
		ctx := context.Background()

		// Create test DSC
		dsc := &v1alpha1.DataSourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-claim",
				Namespace: "default",
				UID:       "test-uid-123",
			},
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "dc1.db1.orders",
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "default",
				},
			},
			Status: v1alpha1.DataSourceClaimStatus{
				Phase: v1alpha1.DSCPhasePending,
			},
		}

		// Create test DS with name that doesn't exist in fake client to trigger update failure
		ds := &v1alpha1.DataSource{
			ObjectMeta: metav1.ObjectMeta{
				Name: "nonexistent-datasource",
			},
			Spec: v1alpha1.DataSourceSpec{
				System: "amoro",
				Type:   "table",
				Name:   "dc1.db1.orders",
			},
		}

		controller := setupTestController(ctx, dsc)

		// Create a proper scheme with our custom types registered
		testScheme := runtime.NewScheme()
		v1alpha1.AddToScheme(testScheme)
		controller.Scheme = testScheme

		// Perform static binding - should succeed despite DS back-reference failure
		err := controller.staticBinding(dsc, ds)
		assert.NoError(t, err, "staticBinding should succeed even when DS back-reference update fails")

		// Verify DSC status was still updated
		updatedDSC, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSourceClaims(dsc.Namespace).Get(ctx, dsc.Name, metav1.GetOptions{})
		assert.NoError(t, err, "Failed to get updated DSC")
		assert.Equal(t, v1alpha1.DSCPhaseBound, updatedDSC.Status.Phase, "DSC should be in Bound phase")
		assert.Equal(t, ds.Name, updatedDSC.Status.BoundDataSource, "DSC should reference the bound DataSource")
	})

	t.Run("Verify event recording", func(t *testing.T) {
		ctx := context.Background()

		// Create test DSC
		dsc := &v1alpha1.DataSourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-claim",
				Namespace: "default",
				UID:       "test-uid-123",
			},
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "dc1.db1.orders",
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "default",
				},
			},
			Status: v1alpha1.DataSourceClaimStatus{
				Phase: v1alpha1.DSCPhasePending,
			},
		}

		// Create test DS
		ds := &v1alpha1.DataSource{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-datasource",
			},
			Spec: v1alpha1.DataSourceSpec{
				System: "amoro",
				Type:   "table",
				Name:   "dc1.db1.orders",
			},
		}

		controller := setupTestController(ctx, dsc)

		// Create a proper scheme with our custom types registered
		testScheme := runtime.NewScheme()
		v1alpha1.AddToScheme(testScheme)
		controller.Scheme = testScheme

		// Add the DataSource to the fake client
		_, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSources().Create(ctx, ds, metav1.CreateOptions{})
		assert.NoError(t, err, "Failed to create test DataSource")

		// Perform static binding
		err = controller.staticBinding(dsc, ds)
		assert.NoError(t, err, "staticBinding should succeed")

		// Verify event was recorded (we can't easily check the actual event content with fake recorder,
		// but we can verify the function completed successfully which means the event recording didn't fail)
		assert.NoError(t, err, "Event recording should not cause staticBinding to fail")
	})
}

func TestDynamicBinding(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("Successful dynamic binding", func(t *testing.T) {
		// Create test DSC
		dsc := &v1alpha1.DataSourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-claim",
				Namespace: "default",
				Labels: map[string]string{
					"app": "test-app",
				},
				Annotations: map[string]string{
					AssociatedRBsAnnotation: "test-rb",
				},
			},
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "dc1.db1.orders",
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "default",
				},
			},
		}

		// Create mock plugin manager that returns specific clusters
		mockPluginManager := NewMockPluginManager(func(ctx context.Context, dsc *v1alpha1.DataSourceClaim) ([]string, error) {
			return []string{"cluster-1", "cluster-2"}, nil
		})

		// Setup controller with mock plugin manager
		testScheme := runtime.NewScheme()
		v1alpha1.AddToScheme(testScheme)
		controller := setupTestController(ctx, dsc)
		controller.pluginManager = mockPluginManager
		controller.Scheme = testScheme

		// Call dynamicBinding
		err := controller.dynamicBinding(dsc)
		assert.NoError(t, err, "Dynamic binding should succeed with valid clusters")

		// Verify DataSource was created
		dsList, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSources().List(ctx, metav1.ListOptions{})
		assert.NoError(t, err, "Should be able to list DataSources")
		assert.Len(t, dsList.Items, 1, "Should have created one DataSource")

		// Verify DataSource properties
		ds := dsList.Items[0]
		assert.Equal(t, "amoro-table-dc1.db1.orders", ds.Name, "DataSource name should be correct")
		assert.Equal(t, "amoro", ds.Spec.System, "DataSource system should match")
		assert.Equal(t, "table", ds.Spec.Type, "DataSource type should match")
		assert.Equal(t, "dc1.db1.orders", ds.Spec.Name, "DataSource name should match")
		assert.Equal(t, []string{"cluster-1", "cluster-2"}, ds.Spec.Locality.ClusterNames, "DataSource clusters should match")

		// Verify DataSourceClaim status and binding
		updatedDSC, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSourceClaims(dsc.Namespace).Get(ctx, dsc.Name, metav1.GetOptions{})
		assert.NoError(t, err, "Should be able to get updated DataSourceClaim")

		// Check DSC phase
		assert.Equal(t, v1alpha1.DSCPhaseBound, updatedDSC.Status.Phase, "DSC should be in Bound phase")

		// Check bound DataSource reference
		assert.Equal(t, ds.Name, updatedDSC.Status.BoundDataSource, "DSC should reference the created DataSource")

		// Check binding condition
		assert.NotEmpty(t, updatedDSC.Status.Conditions, "DSC should have status conditions")

		// Find the "Bound" condition
		var boundCondition *metav1.Condition
		for i := range updatedDSC.Status.Conditions {
			if updatedDSC.Status.Conditions[i].Type == "Bound" {
				boundCondition = &updatedDSC.Status.Conditions[i]
				break
			}
		}
		assert.NotNil(t, boundCondition, "DSC should have a 'Bound' condition")
		assert.Equal(t, metav1.ConditionTrue, boundCondition.Status, "Bound condition should be True")
		assert.Equal(t, "BindingSuccessful", boundCondition.Reason, "Bound condition reason should be BindingSuccessful")
		assert.Contains(t, boundCondition.Message, ds.Name, "Bound condition message should contain DataSource name")
	})

	t.Run("Plugin SelectClusters failure", func(t *testing.T) {
		// Create test DSC
		dsc := &v1alpha1.DataSourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-claim-2",
				Namespace: "default",
				Labels: map[string]string{
					"app": "test-app",
				},
				Annotations: map[string]string{
					AssociatedRBsAnnotation: "test-rb",
				},
			},
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "dc1.db1.orders",
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "default",
				},
			},
		}

		// Create mock plugin manager that returns error
		mockPluginManager := NewMockPluginManager(func(ctx context.Context, dsc *v1alpha1.DataSourceClaim) ([]string, error) {
			return nil, errors.New("plugin connection failed")
		})

		// Setup controller with mock plugin manager
		controller := setupTestController(ctx, dsc)
		controller.pluginManager = mockPluginManager

		// Call dynamicBinding
		err := controller.dynamicBinding(dsc)
		assert.Error(t, err, "Dynamic binding should fail when plugin fails")
		assert.Contains(t, err.Error(), "plugin connection failed", "Error should contain plugin error message")

		// Verify no DataSource was created
		dsList, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSources().List(ctx, metav1.ListOptions{})
		assert.NoError(t, err, "Should be able to list DataSources")
		assert.Len(t, dsList.Items, 0, "Should not have created any DataSource")
	})

	t.Run("No clusters selected - keep pending", func(t *testing.T) {
		// Create test DSC
		dsc := &v1alpha1.DataSourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-claim-3",
				Namespace: "default",
				Labels: map[string]string{
					"app": "test-app",
				},
				Annotations: map[string]string{
					AssociatedRBsAnnotation: "test-rb",
				},
			},
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "dc1.db1.orders",
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "default",
				},
			},
			Status: v1alpha1.DataSourceClaimStatus{
				Phase: v1alpha1.DSCPhasePending,
			},
		}

		// Create mock plugin manager that returns empty clusters
		mockPluginManager := NewMockPluginManager(func(ctx context.Context, dsc *v1alpha1.DataSourceClaim) ([]string, error) {
			return []string{}, nil // No clusters available
		})

		// Setup controller with mock plugin manager
		controller := setupTestController(ctx, dsc)
		controller.pluginManager = mockPluginManager

		// Call dynamicBinding
		err := controller.dynamicBinding(dsc)
		assert.NoError(t, err, "Dynamic binding should not error when no clusters available")

		// Verify no DataSource was created (DSC should remain pending)
		dsList, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSources().List(ctx, metav1.ListOptions{})
		assert.NoError(t, err, "Should be able to list DataSources")
		assert.Len(t, dsList.Items, 0, "Should not have created any DataSource when no clusters available")

		// Verify DSC remains in Pending state
		updatedDSC, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSourceClaims(dsc.Namespace).Get(ctx, dsc.Name, metav1.GetOptions{})
		assert.NoError(t, err, "Should be able to get updated DataSourceClaim")

		// Check DSC phase remains Pending
		assert.Equal(t, v1alpha1.DSCPhasePending, updatedDSC.Status.Phase, "DSC should remain in Pending phase when no clusters available")

		// Check no bound DataSource reference
		assert.Empty(t, updatedDSC.Status.BoundDataSource, "DSC should not have bound DataSource when no clusters available")

		// Check that no "Bound" condition exists
		var boundCondition *metav1.Condition
		for i := range updatedDSC.Status.Conditions {
			if updatedDSC.Status.Conditions[i].Type == "Bound" {
				boundCondition = &updatedDSC.Status.Conditions[i]
				break
			}
		}
		if boundCondition != nil {
			assert.Equal(t, metav1.ConditionFalse, boundCondition.Status, "Bound condition should be False when no clusters available")
		}
	})

	t.Run("DataSource creation with clusters", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Create test DSC
		dsc := &v1alpha1.DataSourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-claim-4",
				Namespace: "default",
				Labels: map[string]string{
					"app": "test-app",
				},
				Annotations: map[string]string{
					AssociatedRBsAnnotation: "test-rb",
				},
			},
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "dc1.db1.orders",
				Attributes: map[string]string{
					"format": "iceberg",
					"region": "us-west-2",
				},
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "default",
				},
			},
		}

		// Create mock plugin manager that returns single cluster
		mockPluginManager := NewMockPluginManager(func(ctx context.Context, dsc *v1alpha1.DataSourceClaim) ([]string, error) {
			return []string{"production-cluster"}, nil
		})

		// Setup controller with mock plugin manager
		testScheme := runtime.NewScheme()
		v1alpha1.AddToScheme(testScheme)
		controller := setupTestController(ctx, dsc)
		controller.pluginManager = mockPluginManager
		controller.Scheme = testScheme

		// Call dynamicBinding
		err := controller.dynamicBinding(dsc)
		assert.NoError(t, err, "Dynamic binding should succeed")

		// Verify DataSource was created with correct attributes
		dsList, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSources().List(ctx, metav1.ListOptions{})
		assert.NoError(t, err, "Should be able to list DataSources")
		assert.Len(t, dsList.Items, 1, "Should have created one DataSource")

		// Verify DataSource properties including attributes
		ds := dsList.Items[0]
		assert.Equal(t, "amoro-table-dc1.db1.orders-56600459", ds.Name, "DataSource name should be correct")
		assert.Equal(t, []string{"production-cluster"}, ds.Spec.Locality.ClusterNames, "DataSource should have correct cluster")
		assert.Equal(t, map[string]string{"format": "iceberg", "region": "us-west-2"}, ds.Spec.Attributes, "DataSource should preserve attributes")

	})
}

func TestFindAssociatedRB(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("Empty workloadRef - should return error", func(t *testing.T) {
		// Create DSC with empty workloadRef (all fields are zero values)
		dsc := &v1alpha1.DataSourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-claim",
				Namespace: "test-namespace",
			},
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "dc1.db1.orders",
				// Workload field is zero value (empty WorkloadRef)
			},
		}

		controller := setupTestController(ctx, dsc)

		// Call findAssociatedRB - should return error due to empty required fields in workloadRef
		rb, err := controller.findAssociatedRB(dsc)
		assert.Error(t, err, "Should return error when workloadRef required fields are empty")
		assert.Nil(t, rb, "Should return nil when workloadRef is invalid")
		assert.Contains(t, err.Error(), "APIVersion is required but empty", "Error should mention missing APIVersion")
	})

	t.Run("No matching ResourceBinding - should return nil", func(t *testing.T) {
		// Create DSC with valid workloadRef
		dsc := &v1alpha1.DataSourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-claim",
				Namespace: "test-namespace",
			},
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "dc1.db1.orders",
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-deployment",
					Namespace:  "test-namespace",
				},
			},
		}

		controller := setupTestController(ctx, dsc)

		// Call findAssociatedRB (no RBs created, so should return nil)
		rb, err := controller.findAssociatedRB(dsc)
		assert.NoError(t, err, "Should not return error")
		assert.Nil(t, rb, "Should return nil when no RB matches")
	})

	t.Run("Single matching ResourceBinding - should return matching RB", func(t *testing.T) {
		// Create DSC with valid workloadRef
		dsc := &v1alpha1.DataSourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-claim",
				Namespace: "test-namespace",
			},
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "dc1.db1.orders",
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-deployment",
					Namespace:  "test-namespace",
				},
			},
		}

		controller := setupTestController(ctx, dsc)

		// Create a matching ResourceBinding with the same workload reference
		rb := &workv1alpha2.ResourceBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-rb",
				Namespace: "test-namespace",
			},
			Spec: workv1alpha2.ResourceBindingSpec{
				Resource: workv1alpha2.ObjectReference{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-deployment",
					Namespace:  "test-namespace",
				},
			},
		}

		_, err := controller.karmadaClient.WorkV1alpha2().ResourceBindings("test-namespace").Create(ctx, rb, metav1.CreateOptions{})
		assert.NoError(t, err, "Should create ResourceBinding successfully")

		// Manually add RB to Indexer (No wait.Poll needed)
		controller.karmadaInformerFactory.Work().V1alpha2().ResourceBindings().Informer().GetIndexer().Add(rb)

		// Call findAssociatedRB
		foundRB, err := controller.findAssociatedRB(dsc)
		assert.NoError(t, err, "Should not return error")
		assert.NotNil(t, foundRB, "Should return the matching RB")
		assert.Equal(t, "test-rb", foundRB.Name, "Should return the correct RB")
	})

	t.Run("ResourceBinding in different namespace - should not match", func(t *testing.T) {
		// Create DSC with valid workloadRef
		dsc := &v1alpha1.DataSourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-claim",
				Namespace: "test-namespace",
			},
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "dc1.db1.orders",
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-deployment",
					Namespace:  "test-namespace",
				},
			},
		}

		controller := setupTestController(ctx, dsc)

		// Create a ResourceBinding with same workload reference but different namespace
		rb := &workv1alpha2.ResourceBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-rb",
				Namespace: "different-namespace", // Different namespace
			},
			Spec: workv1alpha2.ResourceBindingSpec{
				Resource: workv1alpha2.ObjectReference{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-deployment",
					Namespace:  "test-namespace",
				},
			},
		}

		_, err := controller.karmadaClient.WorkV1alpha2().ResourceBindings("different-namespace").Create(ctx, rb, metav1.CreateOptions{})
		assert.NoError(t, err, "Should create ResourceBinding successfully")

		// Manually add RB to Indexer
		controller.karmadaInformerFactory.Work().V1alpha2().ResourceBindings().Informer().GetIndexer().Add(rb)

		// Call findAssociatedRB - should not find RB in different namespace
		foundRB, err := controller.findAssociatedRB(dsc)
		assert.NoError(t, err, "Should not return error")
		assert.Nil(t, foundRB, "Should not return RB from different namespace")
	})

	t.Run("Multiple ResourceBindings - only one matches by workloadRef", func(t *testing.T) {
		// Create DSC with valid workloadRef
		dsc := &v1alpha1.DataSourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-claim",
				Namespace: "test-namespace",
			},
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "dc1.db1.orders",
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "shared-deployment",
					Namespace:  "test-namespace",
				},
			},
		}

		controller := setupTestController(ctx, dsc)

		// Create multiple ResourceBindings
		rb1 := &workv1alpha2.ResourceBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-rb-1",
				Namespace: "test-namespace",
			},
			Spec: workv1alpha2.ResourceBindingSpec{
				Resource: workv1alpha2.ObjectReference{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "shared-deployment",
					Namespace:  "test-namespace",
				},
			},
		}

		rb2 := &workv1alpha2.ResourceBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-rb-2",
				Namespace: "test-namespace",
			},
			Spec: workv1alpha2.ResourceBindingSpec{
				Resource: workv1alpha2.ObjectReference{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "shared-deployment-2", // Different workload name
					Namespace:  "test-namespace",
				},
			},
		}

		_, err := controller.karmadaClient.WorkV1alpha2().ResourceBindings("test-namespace").Create(ctx, rb1, metav1.CreateOptions{})
		assert.NoError(t, err, "Should create ResourceBinding 1 successfully")

		_, err = controller.karmadaClient.WorkV1alpha2().ResourceBindings("test-namespace").Create(ctx, rb2, metav1.CreateOptions{})
		assert.NoError(t, err, "Should create ResourceBinding 2 successfully")

		// Manually add both RBs to Indexer
		controller.karmadaInformerFactory.Work().V1alpha2().ResourceBindings().Informer().GetIndexer().Add(rb1)
		controller.karmadaInformerFactory.Work().V1alpha2().ResourceBindings().Informer().GetIndexer().Add(rb2)

		// Call findAssociatedRB - should return the only matching RB
		foundRB, err := controller.findAssociatedRB(dsc)
		assert.NoError(t, err, "Should not return error")
		assert.NotNil(t, foundRB, "Should return a matching RB")

		// Verify the correct RB is returned (should be the matching one)
		assert.Equal(t, "test-rb-1", foundRB.Name, "Should return the matching RB")
	})
}

func TestInjectPlacementAffinity(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("Empty dsClusterNames - should not modify ClusterAffinity", func(t *testing.T) {
		// Create DSC
		dsc := &v1alpha1.DataSourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-claim",
				Namespace: "test-namespace",
			},
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "dc1.db1.orders",
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "test-namespace",
				},
			},
		}

		controller := setupTestController(ctx, dsc)

		// Create a ResourceBinding without placement
		rb := &workv1alpha2.ResourceBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app-deployment",
				Namespace: "test-namespace",
			},
			Spec: workv1alpha2.ResourceBindingSpec{
				Resource: workv1alpha2.ObjectReference{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "test-namespace",
				},
			},
		}

		_, err := controller.karmadaClient.WorkV1alpha2().ResourceBindings("test-namespace").Create(ctx, rb, metav1.CreateOptions{})
		assert.NoError(t, err, "Should create ResourceBinding successfully")

		// Call injectPlacementAffinity with empty dsClusterNames
		err = controller.injectPlacementAffinity(rb, []string{})
		assert.NoError(t, err, "Should not return error")

		// Verify the ResourceBinding was updated with annotation but no ClusterAffinity changes
		updatedRB, err := controller.karmadaClient.WorkV1alpha2().ResourceBindings("test-namespace").Get(ctx, "test-app-deployment", metav1.GetOptions{})
		assert.NoError(t, err, "Should get updated ResourceBinding")
		assert.Equal(t, "true", updatedRB.Annotations[PlacementInjectedAnnotation], "Should have placement injected annotation")

		// Should have Placement but no ClusterAffinity
		assert.NotNil(t, updatedRB.Spec.Placement, "Should have Placement")
		assert.Nil(t, updatedRB.Spec.Placement.ClusterAffinity, "Should not have ClusterAffinity")
	})

	t.Run("Valid dsClusterNames - should set ExcludeClusters", func(t *testing.T) {
		// Create DSC
		dsc := &v1alpha1.DataSourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-claim",
				Namespace: "test-namespace",
			},
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "dc1.db1.orders",
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "test-namespace",
				},
			},
		}

		controller := setupTestController(ctx, dsc)

		// Create some clusters
		cluster1 := &clusterv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster-1"}}
		cluster2 := &clusterv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster-2"}}
		cluster3 := &clusterv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster-3"}}

		_, err := controller.karmadaClient.ClusterV1alpha1().Clusters().Create(ctx, cluster1, metav1.CreateOptions{})
		assert.NoError(t, err)
		_, err = controller.karmadaClient.ClusterV1alpha1().Clusters().Create(ctx, cluster2, metav1.CreateOptions{})
		assert.NoError(t, err)
		_, err = controller.karmadaClient.ClusterV1alpha1().Clusters().Create(ctx, cluster3, metav1.CreateOptions{})
		assert.NoError(t, err)

		// Manually add clusters to Indexer (Critical for injectPlacementAffinity)
		controller.karmadaInformerFactory.Cluster().V1alpha1().Clusters().Informer().GetIndexer().Add(cluster1)
		controller.karmadaInformerFactory.Cluster().V1alpha1().Clusters().Informer().GetIndexer().Add(cluster2)
		controller.karmadaInformerFactory.Cluster().V1alpha1().Clusters().Informer().GetIndexer().Add(cluster3)

		// Create a ResourceBinding
		rb := &workv1alpha2.ResourceBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app-deployment",
				Namespace: "test-namespace",
			},
			Spec: workv1alpha2.ResourceBindingSpec{
				Resource: workv1alpha2.ObjectReference{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "test-namespace",
				},
			},
		}

		_, err = controller.karmadaClient.WorkV1alpha2().ResourceBindings("test-namespace").Create(ctx, rb, metav1.CreateOptions{})
		assert.NoError(t, err)

		// Call injectPlacementAffinity with cluster-1 and cluster-2 as DS clusters
		dsClusterNames := []string{"cluster-1", "cluster-2"}
		err = controller.injectPlacementAffinity(rb, dsClusterNames)
		assert.NoError(t, err)

		// Verify the ResourceBinding was updated
		updatedRB, err := controller.karmadaClient.WorkV1alpha2().ResourceBindings("test-namespace").Get(ctx, "test-app-deployment", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Equal(t, "true", updatedRB.Annotations[PlacementInjectedAnnotation])
		assert.Equal(t, "cluster-3", updatedRB.Annotations[ExcludedClustersAnnotation])

		// Should exclude cluster-3 (all clusters - DS clusters)
		assert.NotNil(t, updatedRB.Spec.Placement)
		assert.NotNil(t, updatedRB.Spec.Placement.ClusterAffinity)
		assert.Equal(t, []string{"cluster-3"}, updatedRB.Spec.Placement.ClusterAffinity.ExcludeClusters)
	})

	t.Run("Existing user-defined ExcludeClusters - should merge", func(t *testing.T) {
		// Create DSC
		dsc := &v1alpha1.DataSourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-claim",
				Namespace: "test-namespace",
			},
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "dc1.db1.orders",
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "test-namespace",
				},
			},
		}

		controller := setupTestController(ctx, dsc)

		// Create some clusters
		cluster1 := &clusterv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster-1"}}
		cluster2 := &clusterv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster-2"}}
		cluster3 := &clusterv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster-3"}}
		cluster4 := &clusterv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster-4"}}

		_, err := controller.karmadaClient.ClusterV1alpha1().Clusters().Create(ctx, cluster1, metav1.CreateOptions{})
		assert.NoError(t, err)
		_, err = controller.karmadaClient.ClusterV1alpha1().Clusters().Create(ctx, cluster2, metav1.CreateOptions{})
		assert.NoError(t, err)
		_, err = controller.karmadaClient.ClusterV1alpha1().Clusters().Create(ctx, cluster3, metav1.CreateOptions{})
		assert.NoError(t, err)
		_, err = controller.karmadaClient.ClusterV1alpha1().Clusters().Create(ctx, cluster4, metav1.CreateOptions{})
		assert.NoError(t, err)

		// Manually add clusters to Indexer
		controller.karmadaInformerFactory.Cluster().V1alpha1().Clusters().Informer().GetIndexer().Add(cluster1)
		controller.karmadaInformerFactory.Cluster().V1alpha1().Clusters().Informer().GetIndexer().Add(cluster2)
		controller.karmadaInformerFactory.Cluster().V1alpha1().Clusters().Informer().GetIndexer().Add(cluster3)
		controller.karmadaInformerFactory.Cluster().V1alpha1().Clusters().Informer().GetIndexer().Add(cluster4)

		// Create a ResourceBinding with existing user-defined exclusions
		rb := &workv1alpha2.ResourceBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app-deployment",
				Namespace: "test-namespace",
			},
			Spec: workv1alpha2.ResourceBindingSpec{
				Resource: workv1alpha2.ObjectReference{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "test-namespace",
				},
				Placement: &policyv1alpha1.Placement{
					ClusterAffinity: &policyv1alpha1.ClusterAffinity{
						ExcludeClusters: []string{"cluster-4"}, // User-defined exclusion
					},
				},
			},
		}

		_, err = controller.karmadaClient.WorkV1alpha2().ResourceBindings("test-namespace").Create(ctx, rb, metav1.CreateOptions{})
		assert.NoError(t, err)

		// Call injectPlacementAffinity with cluster-1 as DS cluster
		// This should exclude cluster-2, cluster-3 (all clusters - DS clusters)
		dsClusterNames := []string{"cluster-1"}
		err = controller.injectPlacementAffinity(rb, dsClusterNames)
		assert.NoError(t, err)

		// Verify the ResourceBinding was updated
		updatedRB, err := controller.karmadaClient.WorkV1alpha2().ResourceBindings("test-namespace").Get(ctx, "test-app-deployment", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Equal(t, "true", updatedRB.Annotations[PlacementInjectedAnnotation])

		// Should merge user-defined exclusions with our exclusions
		excludedClusters := updatedRB.Spec.Placement.ClusterAffinity.ExcludeClusters
		assert.Contains(t, excludedClusters, "cluster-4", "Should keep user-defined exclusion")
		assert.Contains(t, excludedClusters, "cluster-2", "Should add our exclusion")
		assert.Contains(t, excludedClusters, "cluster-3", "Should add our exclusion")
		assert.Len(t, excludedClusters, 3, "Should have 3 excluded clusters total")
	})

	t.Run("ResourceBinding not found - should return error", func(t *testing.T) {
		// Create DSC
		dsc := &v1alpha1.DataSourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-claim",
				Namespace: "test-namespace",
			},
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "dc1.db1.orders",
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "test-namespace",
				},
			},
		}

		controller := setupTestController(ctx, dsc)

		// Create a ResourceBinding reference but don't actually create it
		rb := &workv1alpha2.ResourceBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app-deployment",
				Namespace: "test-namespace",
			},
			Spec: workv1alpha2.ResourceBindingSpec{
				Resource: workv1alpha2.ObjectReference{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "test-namespace",
				},
			},
		}

		// Call injectPlacementAffinity on non-existent RB
		err := controller.injectPlacementAffinity(rb, []string{"cluster-1"})
		assert.Error(t, err, "Should return error for non-existent ResourceBinding")
		assert.True(t, apierrors.IsNotFound(err), "Should be a NotFound error")
	})

	t.Run("Duplicate clusters in exclusions - should remove duplicates", func(t *testing.T) {
		// Create DSC
		dsc := &v1alpha1.DataSourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-claim",
				Namespace: "test-namespace",
			},
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "dc1.db1.orders",
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "test-namespace",
				},
			},
		}

		controller := setupTestController(ctx, dsc)

		// Create some clusters
		cluster1 := &clusterv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster-1"}}
		cluster2 := &clusterv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster-2"}}

		_, err := controller.karmadaClient.ClusterV1alpha1().Clusters().Create(ctx, cluster1, metav1.CreateOptions{})
		assert.NoError(t, err)
		_, err = controller.karmadaClient.ClusterV1alpha1().Clusters().Create(ctx, cluster2, metav1.CreateOptions{})
		assert.NoError(t, err)

		// Manually add clusters to Indexer
		controller.karmadaInformerFactory.Cluster().V1alpha1().Clusters().Informer().GetIndexer().Add(cluster1)
		controller.karmadaInformerFactory.Cluster().V1alpha1().Clusters().Informer().GetIndexer().Add(cluster2)

		// Create a ResourceBinding with existing exclusion that will be duplicated
		rb := &workv1alpha2.ResourceBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app-deployment",
				Namespace: "test-namespace",
			},
			Spec: workv1alpha2.ResourceBindingSpec{
				Resource: workv1alpha2.ObjectReference{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "test-namespace",
				},
				Placement: &policyv1alpha1.Placement{
					ClusterAffinity: &policyv1alpha1.ClusterAffinity{
						ExcludeClusters: []string{"cluster-2"}, // This will be duplicated
					},
				},
			},
		}

		_, err = controller.karmadaClient.WorkV1alpha2().ResourceBindings("test-namespace").Create(ctx, rb, metav1.CreateOptions{})
		assert.NoError(t, err)

		// Call injectPlacementAffinity with cluster-1 as DS cluster
		// This should exclude cluster-2, but cluster-2 is already in user exclusions
		dsClusterNames := []string{"cluster-1"}
		err = controller.injectPlacementAffinity(rb, dsClusterNames)
		assert.NoError(t, err)

		// Verify the ResourceBinding was updated
		updatedRB, err := controller.karmadaClient.WorkV1alpha2().ResourceBindings("test-namespace").Get(ctx, "test-app-deployment", metav1.GetOptions{})
		assert.NoError(t, err)

		// Should have only one instance of cluster-2 (no duplicates)
		excludedClusters := updatedRB.Spec.Placement.ClusterAffinity.ExcludeClusters
		assert.Equal(t, []string{"cluster-2"}, excludedClusters, "Should have only one instance of cluster-2")
	})
}

func TestTriggerRescheduling(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = workv1alpha2.AddToScheme(scheme)
	_ = clusterv1alpha1.AddToScheme(scheme)
	_ = policyv1alpha1.AddToScheme(scheme)

	t.Run("should update placement affinity with valid DS cluster names", func(t *testing.T) {
		// Create test clusters
		cluster1 := &clusterv1alpha1.Cluster{
			ObjectMeta: metav1.ObjectMeta{Name: "cluster1"},
		}
		cluster2 := &clusterv1alpha1.Cluster{
			ObjectMeta: metav1.ObjectMeta{Name: "cluster2"},
		}
		cluster3 := &clusterv1alpha1.Cluster{
			ObjectMeta: metav1.ObjectMeta{Name: "cluster3"},
		}

		// Create test ResourceBinding that has already been injected
		// Previously DS was on cluster1 only, so cluster2 and cluster3 were excluded
		rb := &workv1alpha2.ResourceBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app-deployment",
				Namespace: "default",
				Annotations: map[string]string{
					PlacementInjectedAnnotation: "true",
					ExcludedClustersAnnotation:  "cluster2,cluster3", // Previously excluded cluster2,cluster3
				},
			},
			Spec: workv1alpha2.ResourceBindingSpec{
				Clusters: []workv1alpha2.TargetCluster{
					{Name: "cluster1"},
				},
				Placement: &policyv1alpha1.Placement{
					ClusterAffinity: &policyv1alpha1.ClusterAffinity{
						ExcludeClusters: []string{"cluster2", "cluster3"}, // Previously excluded cluster2,cluster3
					},
				},
			},
		}

		karmadaClient := karmadafake.NewSimpleClientset(rb)
		clusterInformerFactory := karmadainformerfactory.NewSharedInformerFactory(karmadaClient, 0)
		clusterInformer := clusterInformerFactory.Cluster().V1alpha1().Clusters()

		// Add clusters to informer
		clusterInformer.Informer().GetStore().Add(cluster1)
		clusterInformer.Informer().GetStore().Add(cluster2)
		clusterInformer.Informer().GetStore().Add(cluster3)

		controller := &DataDependencyController{
			karmadaClient: karmadaClient,
			cLister:       clusterInformer.Lister(),
		}

		// Now DS expands to cluster1 and cluster2, so only cluster3 should be excluded
		dsClusterNames := []string{"cluster1", "cluster2"}
		err := controller.triggerRescheduling(rb, dsClusterNames)
		assert.NoError(t, err)

		// Verify the ResourceBinding was updated
		updatedRB, err := karmadaClient.WorkV1alpha2().ResourceBindings("default").Get(context.TODO(), "test-app-deployment", metav1.GetOptions{})
		assert.NoError(t, err)

		// Verify clusters field was cleared (triggers rescheduling)
		assert.Nil(t, updatedRB.Spec.Clusters)

		// Verify placement affinity was updated to exclude only cluster3 (the only non-DS cluster)
		assert.NotNil(t, updatedRB.Spec.Placement)
		assert.NotNil(t, updatedRB.Spec.Placement.ClusterAffinity)
		assert.Equal(t, []string{"cluster3"}, updatedRB.Spec.Placement.ClusterAffinity.ExcludeClusters)

		// Verify annotation was updated to reflect new controller exclusions
		assert.Equal(t, "cluster3", updatedRB.Annotations[ExcludedClustersAnnotation])
	})

	t.Run("should clear previous exclusions when DS cluster names is empty", func(t *testing.T) {
		// Create test ResourceBinding with existing exclusions
		rb := &workv1alpha2.ResourceBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app-deployment",
				Namespace: "default",
				Annotations: map[string]string{
					ExcludedClustersAnnotation:  "cluster2,cluster3",
					PlacementInjectedAnnotation: "true",
				},
			},
			Spec: workv1alpha2.ResourceBindingSpec{
				Placement: &policyv1alpha1.Placement{
					ClusterAffinity: &policyv1alpha1.ClusterAffinity{
						ExcludeClusters: []string{"cluster2", "cluster3"},
					},
				},
				Clusters: []workv1alpha2.TargetCluster{
					{Name: "cluster1"},
				},
			},
		}

		karmadaClient := karmadafake.NewSimpleClientset(rb)
		controller := &DataDependencyController{
			karmadaClient: karmadaClient,
		}

		// Call with empty DS cluster names
		err := controller.triggerRescheduling(rb, []string{})
		assert.NoError(t, err)

		// Verify the ResourceBinding was updated
		updatedRB, err := karmadaClient.WorkV1alpha2().ResourceBindings("default").Get(context.TODO(), "test-app-deployment", metav1.GetOptions{})
		assert.NoError(t, err)

		// Verify clusters field was cleared
		assert.Nil(t, updatedRB.Spec.Clusters)

		// Verify exclusions were cleared
		assert.Empty(t, updatedRB.Spec.Placement.ClusterAffinity.ExcludeClusters)

		// Verify annotations were cleared
		_, exists := updatedRB.Annotations[ExcludedClustersAnnotation]
		assert.False(t, exists)
		_, exists = updatedRB.Annotations[PlacementInjectedAnnotation]
		assert.False(t, exists)
	})

	t.Run("should preserve user-defined exclusions", func(t *testing.T) {
		// Create test clusters
		cluster1 := &clusterv1alpha1.Cluster{
			ObjectMeta: metav1.ObjectMeta{Name: "cluster1"},
		}
		cluster2 := &clusterv1alpha1.Cluster{
			ObjectMeta: metav1.ObjectMeta{Name: "cluster2"},
		}
		cluster3 := &clusterv1alpha1.Cluster{
			ObjectMeta: metav1.ObjectMeta{Name: "cluster3"},
		}
		cluster4 := &clusterv1alpha1.Cluster{
			ObjectMeta: metav1.ObjectMeta{Name: "cluster4"},
		}

		// Create test ResourceBinding with existing user-defined exclusions
		rb := &workv1alpha2.ResourceBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app-deployment",
				Namespace: "default",
				Annotations: map[string]string{
					ExcludedClustersAnnotation: "cluster3", // Previously excluded by controller
				},
			},
			Spec: workv1alpha2.ResourceBindingSpec{
				Placement: &policyv1alpha1.Placement{
					ClusterAffinity: &policyv1alpha1.ClusterAffinity{
						ExcludeClusters: []string{"cluster4", "cluster3"}, // cluster4 is user-defined, cluster3 is controller-added
					},
				},
			},
		}

		karmadaClient := karmadafake.NewSimpleClientset(rb)
		clusterInformerFactory := karmadainformerfactory.NewSharedInformerFactory(karmadaClient, 0)
		clusterInformer := clusterInformerFactory.Cluster().V1alpha1().Clusters()

		// Add clusters to informer
		clusterInformer.Informer().GetStore().Add(cluster1)
		clusterInformer.Informer().GetStore().Add(cluster2)
		clusterInformer.Informer().GetStore().Add(cluster3)
		clusterInformer.Informer().GetStore().Add(cluster4)

		controller := &DataDependencyController{
			karmadaClient: karmadaClient,
			cLister:       clusterInformer.Lister(),
		}

		dsClusterNames := []string{"cluster1", "cluster2"}
		err := controller.triggerRescheduling(rb, dsClusterNames)
		assert.NoError(t, err)

		// Verify the ResourceBinding was updated
		updatedRB, err := karmadaClient.WorkV1alpha2().ResourceBindings("default").Get(context.TODO(), "test-app-deployment", metav1.GetOptions{})
		assert.NoError(t, err)

		// Verify clusters field was cleared
		assert.Nil(t, updatedRB.Spec.Clusters)

		// Verify exclusions: should have user-defined cluster4 + new controller exclusions cluster3
		expectedExclusions := []string{"cluster4", "cluster3"}
		assert.ElementsMatch(t, expectedExclusions, updatedRB.Spec.Placement.ClusterAffinity.ExcludeClusters)

		// Verify annotation was updated (should contain new controller exclusions)
		assert.Equal(t, "cluster3,cluster4", updatedRB.Annotations[ExcludedClustersAnnotation])
	})

	t.Run("should handle ResourceBinding not found", func(t *testing.T) {
		rb := &workv1alpha2.ResourceBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "non-existent-app-deployment",
				Namespace: "default",
			},
		}

		karmadaClient := karmadafake.NewSimpleClientset()
		controller := &DataDependencyController{
			karmadaClient: karmadaClient,
		}

		err := controller.triggerRescheduling(rb, []string{"cluster1"})
		assert.Error(t, err)
		assert.True(t, apierrors.IsNotFound(err))
	})

	t.Run("should handle cluster listing error", func(t *testing.T) {
		rb := &workv1alpha2.ResourceBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app-deployment",
				Namespace: "default",
			},
		}

		karmadaClient := karmadafake.NewSimpleClientset(rb)

		// Create empty cluster informer factory to simulate empty cluster list
		clusterInformerFactory := karmadainformerfactory.NewSharedInformerFactory(karmadaClient, 0)
		clusterInformer := clusterInformerFactory.Cluster().V1alpha1().Clusters()

		controller := &DataDependencyController{
			karmadaClient: karmadaClient,
			cLister:       clusterInformer.Lister(),
		}

		// This should work fine with empty cluster list
		err := controller.triggerRescheduling(rb, []string{"cluster1"})
		assert.NoError(t, err)

		// Verify the ResourceBinding was updated
		updatedRB, err := karmadaClient.WorkV1alpha2().ResourceBindings("default").Get(context.TODO(), "test-app-deployment", metav1.GetOptions{})
		assert.NoError(t, err)

		// Verify clusters field was cleared
		assert.Nil(t, updatedRB.Spec.Clusters)
	})

	t.Run("should remove duplicates in excluded clusters", func(t *testing.T) {
		// Create test clusters
		cluster1 := &clusterv1alpha1.Cluster{
			ObjectMeta: metav1.ObjectMeta{Name: "cluster1"},
		}
		cluster2 := &clusterv1alpha1.Cluster{
			ObjectMeta: metav1.ObjectMeta{Name: "cluster2"},
		}
		cluster3 := &clusterv1alpha1.Cluster{
			ObjectMeta: metav1.ObjectMeta{Name: "cluster3"},
		}

		// Create test ResourceBinding with existing exclusions that will create duplicates
		rb := &workv1alpha2.ResourceBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app-deployment",
				Namespace: "default",
				Annotations: map[string]string{
					ExcludedClustersAnnotation: "cluster2",
				},
			},
			Spec: workv1alpha2.ResourceBindingSpec{
				Placement: &policyv1alpha1.Placement{
					ClusterAffinity: &policyv1alpha1.ClusterAffinity{
						ExcludeClusters: []string{"cluster2", "cluster3"}, // cluster2 will be duplicate
					},
				},
			},
		}

		karmadaClient := karmadafake.NewSimpleClientset(rb)
		clusterInformerFactory := karmadainformerfactory.NewSharedInformerFactory(karmadaClient, 0)
		clusterInformer := clusterInformerFactory.Cluster().V1alpha1().Clusters()

		// Add clusters to informer
		clusterInformer.Informer().GetStore().Add(cluster1)
		clusterInformer.Informer().GetStore().Add(cluster2)
		clusterInformer.Informer().GetStore().Add(cluster3)

		controller := &DataDependencyController{
			karmadaClient: karmadaClient,
			cLister:       clusterInformer.Lister(),
		}

		dsClusterNames := []string{"cluster1"}
		err := controller.triggerRescheduling(rb, dsClusterNames)
		assert.NoError(t, err)

		// Verify the ResourceBinding was updated
		updatedRB, err := karmadaClient.WorkV1alpha2().ResourceBindings("default").Get(context.TODO(), "test-app-deployment", metav1.GetOptions{})
		assert.NoError(t, err)

		// Verify no duplicates in exclusions
		excludedClusters := updatedRB.Spec.Placement.ClusterAffinity.ExcludeClusters
		uniqueSet := make(map[string]bool)
		for _, cluster := range excludedClusters {
			assert.False(t, uniqueSet[cluster], "Duplicate cluster found: %s", cluster)
			uniqueSet[cluster] = true
		}

		// Should contain cluster3 (user-defined) and cluster2 (new controller exclusion)
		expectedExclusions := []string{"cluster3", "cluster2"}
		assert.ElementsMatch(t, expectedExclusions, excludedClusters)
	})
}

func TestHandlePlacementUpdate(t *testing.T) {
	t.Run("should return nil when no ResourceBinding found", func(t *testing.T) {
		ctx := context.Background()

		// Create test DSC with WorkloadRef (no labels/annotations)
		dsc := &v1alpha1.DataSourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-dsc",
				Namespace: "default",
			},
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "dc1.db1.orders",
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "default",
				},
			},
			Status: v1alpha1.DataSourceClaimStatus{
				Phase:           v1alpha1.DSCPhaseBound,
				BoundDataSource: "test-ds",
			},
		}

		// Create test DataSource
		ds := &v1alpha1.DataSource{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-ds",
			},
			Spec: v1alpha1.DataSourceSpec{
				System: "amoro",
				Type:   "table",
				Name:   "dc1.db1.orders",
				Locality: &v1alpha1.DataSourceLocality{
					ClusterNames: []string{"cluster1", "cluster2"},
				},
			},
		}

		// Setup controller
		controller := setupTestController(ctx, dsc)

		// Call handlePlacementUpdate - should return nil since no RB found
		err := controller.handlePlacementUpdate(dsc, ds)
		assert.NoError(t, err)
	})

	t.Run("should inject placement affinity for uninjected ResourceBinding", func(t *testing.T) {
		ctx := context.Background()

		// 1. Setup Objects (DSC, DS, RB)
		dsc := &v1alpha1.DataSourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-dsc",
				Namespace: "default",
			},
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "dc1.db1.orders",
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "default",
				},
			},
			Status: v1alpha1.DataSourceClaimStatus{
				Phase:           v1alpha1.DSCPhaseBound,
				BoundDataSource: "test-ds",
			},
		}

		ds := &v1alpha1.DataSource{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-ds",
			},
			Spec: v1alpha1.DataSourceSpec{
				System: "amoro",
				Type:   "table",
				Name:   "dc1.db1.orders",
				Locality: &v1alpha1.DataSourceLocality{
					ClusterNames: []string{"cluster1", "cluster2"},
				},
			},
		}

		rb := &workv1alpha2.ResourceBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app-deployment",
				Namespace: "default",
			},
			Spec: workv1alpha2.ResourceBindingSpec{
				Resource: workv1alpha2.ObjectReference{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "default",
				},
				Placement: &policyv1alpha1.Placement{
					ClusterAffinity: &policyv1alpha1.ClusterAffinity{
						ClusterNames: []string{"cluster1", "cluster2"},
					},
				},
			},
		}

		// 2. Setup Controller
		controller := setupTestController(ctx, dsc)

		// Start Informers manually
		controller.karmadaInformerFactory.Start(ctx.Done())
		controller.dataInformerFactory.Start(ctx.Done())

		// Wait for cache sync
		cache.WaitForCacheSync(ctx.Done(),
			controller.dscListerSynced,
			controller.dsListerSynced,
			controller.rbListerSynced,
			controller.cListerSynced)

		// 3. Create Clusters and Sync Indexer
		clusters := []*clusterv1alpha1.Cluster{
			{ObjectMeta: metav1.ObjectMeta{Name: "cluster1"}},
			{ObjectMeta: metav1.ObjectMeta{Name: "cluster2"}},
			{ObjectMeta: metav1.ObjectMeta{Name: "cluster3"}},
		}

		for _, cluster := range clusters {
			_, err := controller.karmadaClient.ClusterV1alpha1().Clusters().Create(ctx, cluster, metav1.CreateOptions{})
			assert.NoError(t, err)
			// Add Cluster to Indexer (Critical for calculation)
			controller.karmadaInformerFactory.Cluster().V1alpha1().Clusters().Informer().GetIndexer().Add(cluster)
		}

		// 4. Create DS/RB and Sync Indexer
		_, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSources().Create(ctx, ds, metav1.CreateOptions{})
		assert.NoError(t, err)
		// Add DS to Indexer
		controller.dataInformerFactory.Datadependency().V1alpha1().DataSources().Informer().GetIndexer().Add(ds)

		_, err = controller.karmadaClient.WorkV1alpha2().ResourceBindings("default").Create(ctx, rb, metav1.CreateOptions{})
		assert.NoError(t, err)
		// Add RB to Indexer (Critical for findAssociatedRB)
		controller.karmadaInformerFactory.Work().V1alpha2().ResourceBindings().Informer().GetIndexer().Add(rb)

		// 5. Execute Logic
		err = controller.handlePlacementUpdate(dsc, ds)
		assert.NoError(t, err)

		// 6. Verify Result
		updatedRB, err := controller.karmadaClient.WorkV1alpha2().ResourceBindings("default").Get(context.TODO(), "test-app-deployment", metav1.GetOptions{})
		assert.NoError(t, err)

		// Verify placement injection annotation
		assert.Equal(t, "true", updatedRB.Annotations[PlacementInjectedAnnotation])

		// Verify placement affinity
		// DS Locality: 1, 2
		// All Clusters: 1, 2, 3
		// Expected Exclude: 3
		assert.NotNil(t, updatedRB.Spec.Placement)
		assert.NotNil(t, updatedRB.Spec.Placement.ClusterAffinity)
		assert.Equal(t, []string{"cluster3"}, updatedRB.Spec.Placement.ClusterAffinity.ExcludeClusters)

		// Verify excluded clusters annotation
		assert.Equal(t, "cluster3", updatedRB.Annotations[ExcludedClustersAnnotation])
	})

	t.Run("should trigger rescheduling for already injected ResourceBinding", func(t *testing.T) {
		ctx := context.Background()

		// 1. Setup Objects (DSC, DS, RB)
		dsc := &v1alpha1.DataSourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-dsc",
				Namespace: "default",
			},
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "dc1.db1.orders",
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "default",
				},
			},
			Status: v1alpha1.DataSourceClaimStatus{
				Phase:           v1alpha1.DSCPhaseBound,
				BoundDataSource: "test-ds",
			},
		}

		ds := &v1alpha1.DataSource{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-ds",
			},
			Spec: v1alpha1.DataSourceSpec{
				System: "amoro",
				Type:   "table",
				Name:   "dc1.db1.orders",
				Locality: &v1alpha1.DataSourceLocality{
					ClusterNames: []string{"cluster1", "cluster2"}, // DS now on cluster1,cluster2
				},
			},
		}

		rb := &workv1alpha2.ResourceBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app-deployment",
				Namespace: "default",
				Annotations: map[string]string{
					PlacementInjectedAnnotation: "true",
					ExcludedClustersAnnotation:  "cluster2,cluster3", // Previously excluded
				},
			},
			Spec: workv1alpha2.ResourceBindingSpec{
				Resource: workv1alpha2.ObjectReference{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "default",
				},
				Clusters: []workv1alpha2.TargetCluster{
					{Name: "cluster1"},
				},
				Placement: &policyv1alpha1.Placement{
					ClusterAffinity: &policyv1alpha1.ClusterAffinity{
						ExcludeClusters: []string{"cluster2", "cluster3"},
					},
				},
			},
		}

		// 2. Setup Controller
		controller := setupTestController(ctx, dsc)

		// Manually start Informers
		controller.karmadaInformerFactory.Start(ctx.Done())
		controller.dataInformerFactory.Start(ctx.Done())

		cache.WaitForCacheSync(ctx.Done(),
			controller.dscListerSynced,
			controller.dsListerSynced,
			controller.rbListerSynced,
			controller.cListerSynced)

		// 3. Create Clusters and Sync Indexer
		clusters := []*clusterv1alpha1.Cluster{
			{ObjectMeta: metav1.ObjectMeta{Name: "cluster1"}},
			{ObjectMeta: metav1.ObjectMeta{Name: "cluster2"}},
			{ObjectMeta: metav1.ObjectMeta{Name: "cluster3"}},
		}

		for _, cluster := range clusters {
			_, err := controller.karmadaClient.ClusterV1alpha1().Clusters().Create(ctx, cluster, metav1.CreateOptions{})
			assert.NoError(t, err)
			// Manually add clusters to Indexer (Critical for triggerRescheduling calculation)
			controller.karmadaInformerFactory.Cluster().V1alpha1().Clusters().Informer().GetIndexer().Add(cluster)
		}

		// 4. Create DS/RB and Sync Indexer
		_, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSources().Create(ctx, ds, metav1.CreateOptions{})
		assert.NoError(t, err)
		// Manually add DS to Indexer
		controller.dataInformerFactory.Datadependency().V1alpha1().DataSources().Informer().GetIndexer().Add(ds)

		_, err = controller.karmadaClient.WorkV1alpha2().ResourceBindings("default").Create(ctx, rb, metav1.CreateOptions{})
		assert.NoError(t, err)
		// Manually add RB to Indexer (Critical for findAssociatedRB)
		controller.karmadaInformerFactory.Work().V1alpha2().ResourceBindings().Informer().GetIndexer().Add(rb)

		// 5. Execute Logic
		err = controller.handlePlacementUpdate(dsc, ds)
		assert.NoError(t, err)

		// 6. Verify Result
		updatedRB, err := controller.karmadaClient.WorkV1alpha2().ResourceBindings("default").Get(context.TODO(), "test-app-deployment", metav1.GetOptions{})
		assert.NoError(t, err)

		// Verify clusters field was cleared (triggers rescheduling)
		assert.Nil(t, updatedRB.Spec.Clusters)

		// Verify placement affinity
		// All Clusters: 1, 2, 3
		// DS Locality:  1, 2
		// Expected Exclude: 3
		assert.NotNil(t, updatedRB.Spec.Placement)
		assert.NotNil(t, updatedRB.Spec.Placement.ClusterAffinity)
		assert.Equal(t, []string{"cluster3"}, updatedRB.Spec.Placement.ClusterAffinity.ExcludeClusters)

		// Verify excluded clusters annotation was updated
		assert.Equal(t, "cluster3", updatedRB.Annotations[ExcludedClustersAnnotation])
	})

	t.Run("should return nil when no ResourceBinding found in annotation", func(t *testing.T) {
		ctx := context.Background()

		// Create test DSC with valid WorkloadRef but no matching RB
		dsc := &v1alpha1.DataSourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-dsc",
				Namespace: "default",
			},
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "dc1.db1.orders",
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "default",
				},
			},
			Status: v1alpha1.DataSourceClaimStatus{
				Phase:           v1alpha1.DSCPhaseBound,
				BoundDataSource: "test-ds",
			},
		}

		// Create test DataSource
		ds := &v1alpha1.DataSource{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-ds",
			},
			Spec: v1alpha1.DataSourceSpec{
				System: "amoro",
				Type:   "table",
				Name:   "dc1.db1.orders",
				Locality: &v1alpha1.DataSourceLocality{
					ClusterNames: []string{"cluster1", "cluster2"},
				},
			},
		}

		// Setup controller
		controller := setupTestController(ctx, dsc)

		// Call handlePlacementUpdate - should return nil when no ResourceBinding found
		// (findAssociatedRBs will return empty list because the referenced RB doesn't exist)
		err := controller.handlePlacementUpdate(dsc, ds)
		assert.NoError(t, err) // Should not return error, just log and continue
	})
}

func TestGenerateDataSourceName(t *testing.T) {
	t.Run("DSC without attributes", func(t *testing.T) {
		dsc := &v1alpha1.DataSourceClaim{
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "dc1.db1.orders",
			},
		}

		name := generateDataSourceName(dsc)
		expected := "amoro-table-dc1.db1.orders"
		assert.Equal(t, expected, name, "Name should match expected format without attributes")
	})

	t.Run("DSC with attributes", func(t *testing.T) {
		dsc := &v1alpha1.DataSourceClaim{
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "dc1.db1.orders",
				Attributes: map[string]string{
					"property1": "value1",
					"property2": "value2",
				},
			},
		}

		name := generateDataSourceName(dsc)
		// Should have base name plus hash
		assert.True(t, strings.HasPrefix(name, "amoro-table-dc1.db1.orders-"), "Name should start with base name and dash")
		assert.True(t, len(name) > len("amoro-table-dc1.db1.orders-"), "Name should include hash suffix")

		// Hash should be 8 characters
		hashPart := strings.TrimPrefix(name, "amoro-table-dc1.db1.orders-")
		assert.Equal(t, 8, len(hashPart), "Hash should be 8 characters long")
	})

	t.Run("Different attributes generate different names", func(t *testing.T) {
		dsc1 := &v1alpha1.DataSourceClaim{
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "dc1.db1.orders",
				Attributes: map[string]string{
					"property1": "value1",
				},
			},
		}

		dsc2 := &v1alpha1.DataSourceClaim{
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "dc1.db1.orders",
				Attributes: map[string]string{
					"property1": "value2", // Different value
				},
			},
		}

		name1 := generateDataSourceName(dsc1)
		name2 := generateDataSourceName(dsc2)
		assert.NotEqual(t, name1, name2, "Different attributes should generate different names")
	})

	t.Run("Same attributes in different order generate same name", func(t *testing.T) {
		dsc1 := &v1alpha1.DataSourceClaim{
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "dc1.db1.orders",
				Attributes: map[string]string{
					"property1": "value1",
					"property2": "value2",
				},
			},
		}

		dsc2 := &v1alpha1.DataSourceClaim{
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "dc1.db1.orders",
				Attributes: map[string]string{
					"property2": "value2", // Different order
					"property1": "value1",
				},
			},
		}

		name1 := generateDataSourceName(dsc1)
		name2 := generateDataSourceName(dsc2)
		assert.Equal(t, name1, name2, "Same attributes in different order should generate same name")
	})
}

func TestHandleUnbinding(t *testing.T) {
	t.Run("should successfully unbind DSC from DS when DS is not being deleted", func(t *testing.T) {
		ctx := context.Background()

		// 1. Setup Objects (DSC, DS, RB)
		dsc := &v1alpha1.DataSourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-dsc",
				Namespace: "default",
				UID:       "test-dsc-uid",
			},
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "dc1.db1.orders",
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "default",
				},
			},
			Status: v1alpha1.DataSourceClaimStatus{
				Phase:           v1alpha1.DSCPhaseBound,
				BoundDataSource: "test-ds",
			},
		}

		ds := &v1alpha1.DataSource{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "test-ds",
				Finalizers: []string{DataSourceFinalizer},
			},
			Spec: v1alpha1.DataSourceSpec{
				System: "amoro",
				Type:   "table",
				Name:   "dc2.db1.orders",
				Locality: &v1alpha1.DataSourceLocality{
					ClusterNames: []string{"cluster1", "cluster2"},
				},
			},
			Status: v1alpha1.DataSourceStatus{
				ClaimRefs: []corev1.ObjectReference{
					{
						Kind:      "DataSourceClaim",
						Name:      "test-dsc",
						Namespace: "default",
						UID:       "test-dsc-uid",
					},
				},
			},
		}

		rb := &workv1alpha2.ResourceBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-rb",
				Namespace: "default",
				Annotations: map[string]string{
					PlacementInjectedAnnotation: "true",
					ExcludedClustersAnnotation:  "cluster3",
				},
			},
			Spec: workv1alpha2.ResourceBindingSpec{
				Resource: workv1alpha2.ObjectReference{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "default",
				},
				Clusters: []workv1alpha2.TargetCluster{
					{Name: "cluster1", Replicas: 1},
					{Name: "cluster2", Replicas: 1},
				},
				Placement: &policyv1alpha1.Placement{
					ClusterAffinity: &policyv1alpha1.ClusterAffinity{
						ExcludeClusters: []string{"cluster3"},
					},
				},
			},
		}

		cluster1 := &clusterv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster1"}}
		cluster2 := &clusterv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster2"}}
		cluster3 := &clusterv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster3"}}

		// 2. Setup Controller
		controller := setupTestController(ctx, dsc)

		// Manually start Informers (No background workers)
		controller.karmadaInformerFactory.Start(ctx.Done())
		controller.dataInformerFactory.Start(ctx.Done())

		cache.WaitForCacheSync(ctx.Done(),
			controller.karmadaInformerFactory.Cluster().V1alpha1().Clusters().Informer().HasSynced,
			controller.karmadaInformerFactory.Work().V1alpha2().ResourceBindings().Informer().HasSynced,
		)

		// 3. Create Objects AND Sync Indexers
		// Create DS
		controller.datadependencyClient.DatadependencyV1alpha1().DataSources().Create(ctx, ds, metav1.CreateOptions{})
		// Manually add DS to Indexer
		controller.dataInformerFactory.Datadependency().V1alpha1().DataSources().Informer().GetIndexer().Add(ds)

		// Create RB
		controller.karmadaClient.WorkV1alpha2().ResourceBindings("default").Create(ctx, rb, metav1.CreateOptions{})
		// Manually add RB to Indexer (CRITICAL: findAssociatedRB relies on this)
		controller.karmadaInformerFactory.Work().V1alpha2().ResourceBindings().Informer().GetIndexer().Add(rb)

		// Create Clusters
		controller.karmadaClient.ClusterV1alpha1().Clusters().Create(ctx, cluster1, metav1.CreateOptions{})
		controller.karmadaClient.ClusterV1alpha1().Clusters().Create(ctx, cluster2, metav1.CreateOptions{})
		controller.karmadaClient.ClusterV1alpha1().Clusters().Create(ctx, cluster3, metav1.CreateOptions{})
		// Manually add Clusters to Indexer
		controller.karmadaInformerFactory.Cluster().V1alpha1().Clusters().Informer().GetIndexer().Add(cluster1)
		controller.karmadaInformerFactory.Cluster().V1alpha1().Clusters().Informer().GetIndexer().Add(cluster2)
		controller.karmadaInformerFactory.Cluster().V1alpha1().Clusters().Informer().GetIndexer().Add(cluster3)

		// 4. Run Logic
		err := controller.handleUnbinding(dsc, ds)
		assert.NoError(t, err)

		// 5. Verify Results
		// Verify DSC status was reset
		updatedDSC, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSourceClaims("default").Get(ctx, "test-dsc", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Equal(t, v1alpha1.DSCPhasePending, updatedDSC.Status.Phase)
		assert.Empty(t, updatedDSC.Status.BoundDataSource)

		// Verify ResourceBinding placement was cleared
		updatedRB, err := controller.karmadaClient.WorkV1alpha2().ResourceBindings("default").Get(ctx, "test-rb", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.NotContains(t, updatedRB.Annotations, PlacementInjectedAnnotation)
		assert.NotContains(t, updatedRB.Annotations, ExcludedClustersAnnotation)
		assert.Nil(t, updatedRB.Spec.Clusters)

		// Verify ExcludeClusters was cleared
		if updatedRB.Spec.Placement != nil && updatedRB.Spec.Placement.ClusterAffinity != nil {
			assert.Empty(t, updatedRB.Spec.Placement.ClusterAffinity.ExcludeClusters)
		}

		// Verify DataSource claim ref was removed
		updatedDS, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSources().Get(ctx, "test-ds", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Empty(t, updatedDS.Status.ClaimRefs)
	})

	t.Run("should handle DS deletion and cleanup finalizer", func(t *testing.T) {
		ctx := context.Background()

		// 1. Setup: Create DSC, DS (Deleting), RB, Clusters
		dsc := &v1alpha1.DataSourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-dsc",
				Namespace: "default",
				UID:       "test-dsc-uid",
			},
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "dc1.db1.orders",
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "default",
				},
			},
			Status: v1alpha1.DataSourceClaimStatus{
				Phase:           v1alpha1.DSCPhaseBound,
				BoundDataSource: "test-ds",
			},
		}

		now := metav1.Now()
		ds := &v1alpha1.DataSource{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "test-ds",
				DeletionTimestamp: &now, // Simulate deletion
				Finalizers:        []string{DataSourceFinalizer},
			},
			Spec: v1alpha1.DataSourceSpec{
				System: "amoro",
				Type:   "table",
				Name:   "dc2.db1.orders",
				Locality: &v1alpha1.DataSourceLocality{
					ClusterNames: []string{"cluster1", "cluster2"},
				},
			},
			Status: v1alpha1.DataSourceStatus{
				ClaimRefs: []corev1.ObjectReference{
					{
						Kind:      "DataSourceClaim",
						Name:      "test-dsc",
						Namespace: "default",
						UID:       "test-dsc-uid",
					},
				},
			},
		}

		rb := &workv1alpha2.ResourceBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app-deployment", // Ensure this matches logic in findAssociatedRB
				Namespace: "default",
				Annotations: map[string]string{
					PlacementInjectedAnnotation: "true",
					ExcludedClustersAnnotation:  "cluster3",
				},
			},
			Spec: workv1alpha2.ResourceBindingSpec{
				Resource: workv1alpha2.ObjectReference{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "default",
				},
				Clusters: []workv1alpha2.TargetCluster{
					{Name: "cluster1", Replicas: 1},
					{Name: "cluster2", Replicas: 1},
				},
				Placement: &policyv1alpha1.Placement{
					ClusterAffinity: &policyv1alpha1.ClusterAffinity{
						ExcludeClusters: []string{"cluster3"},
					},
				},
			},
		}

		cluster1 := &clusterv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster1"}}
		cluster2 := &clusterv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster2"}}
		cluster3 := &clusterv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster3"}}

		// 2. Setup Controller
		controller := setupTestController(ctx, dsc)

		// Start Informers manually (No background workers)
		// We avoid startTestController to prevent race conditions in the test
		controller.karmadaInformerFactory.Start(ctx.Done())
		controller.dataInformerFactory.Start(ctx.Done())

		cache.WaitForCacheSync(ctx.Done(),
			controller.karmadaInformerFactory.Cluster().V1alpha1().Clusters().Informer().HasSynced,
			controller.karmadaInformerFactory.Work().V1alpha2().ResourceBindings().Informer().HasSynced,
		)

		// 3. Create Objects AND Sync to Indexer
		// Create DS
		controller.datadependencyClient.DatadependencyV1alpha1().DataSources().Create(ctx, ds, metav1.CreateOptions{})
		// Add DS to Indexer manually to ensure lister visibility
		controller.dataInformerFactory.Datadependency().V1alpha1().DataSources().Informer().GetIndexer().Add(ds)

		// Create RB
		controller.karmadaClient.WorkV1alpha2().ResourceBindings("default").Create(ctx, rb, metav1.CreateOptions{})
		// Add RB to Indexer (Critical for handleUnbinding -> findAssociatedRB)
		controller.karmadaInformerFactory.Work().V1alpha2().ResourceBindings().Informer().GetIndexer().Add(rb)

		// Create Clusters
		controller.karmadaClient.ClusterV1alpha1().Clusters().Create(ctx, cluster1, metav1.CreateOptions{})
		controller.karmadaClient.ClusterV1alpha1().Clusters().Create(ctx, cluster2, metav1.CreateOptions{})
		controller.karmadaClient.ClusterV1alpha1().Clusters().Create(ctx, cluster3, metav1.CreateOptions{})
		// Add Clusters to Indexer
		controller.karmadaInformerFactory.Cluster().V1alpha1().Clusters().Informer().GetIndexer().Add(cluster1)
		controller.karmadaInformerFactory.Cluster().V1alpha1().Clusters().Informer().GetIndexer().Add(cluster2)
		controller.karmadaInformerFactory.Cluster().V1alpha1().Clusters().Informer().GetIndexer().Add(cluster3)

		// 4. Action: Call handleUnbinding
		err := controller.handleUnbinding(dsc, ds)
		assert.NoError(t, err)

		// 5. Verification Phase 1: Check Intermediate State
		updatedDSC, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSourceClaims("default").Get(ctx, "test-dsc", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Equal(t, v1alpha1.DSCPhasePending, updatedDSC.Status.Phase)

		updatedDS, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSources().Get(ctx, "test-ds", metav1.GetOptions{})
		assert.NoError(t, err)

		// ClaimRefs should be empty
		assert.Empty(t, updatedDS.Status.ClaimRefs)
		// Finalizer should STILL be present here (because updateDataSource logic hasn't run yet)
		assert.Contains(t, updatedDS.Finalizers, DataSourceFinalizer)

		// 6. Action Phase 2: Simulate Async Event (updateDataSource)
		// We manually trigger the event handler to verify the cleanup logic (removing finalizer)
		controller.updateDataSource(ds, updatedDS)

		// 7. Verification Phase 2: Check Final State
		finalDS, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSources().Get(ctx, "test-ds", metav1.GetOptions{})
		assert.NoError(t, err)
		// Now the finalizer should be gone
		assert.NotContains(t, finalDS.Finalizers, DataSourceFinalizer)

		// 8. Verify ResourceBinding cleanup
		updatedRB, err := controller.karmadaClient.WorkV1alpha2().ResourceBindings("default").Get(ctx, "test-app-deployment", metav1.GetOptions{})
		assert.NoError(t, err)
		// This assertion previously failed intermittently because findAssociatedRB returned nil due to cache lag
		assert.NotContains(t, updatedRB.Annotations, PlacementInjectedAnnotation)
		if updatedRB.Spec.Placement != nil && updatedRB.Spec.Placement.ClusterAffinity != nil {
			assert.Empty(t, updatedRB.Spec.Placement.ClusterAffinity.ExcludeClusters)
		}
	})

	t.Run("should handle findAssociatedRB failure gracefully", func(t *testing.T) {
		ctx := context.Background()

		// Create test DSC with invalid WorkloadRef to simulate findAssociatedRB failure
		dsc := &v1alpha1.DataSourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-dsc",
				Namespace: "default",
				UID:       "test-dsc-uid",
			},
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "dc1.db1.orders",
				// Intentionally leave WorkloadRef empty to trigger index key generation error
				Workload: v1alpha1.WorkloadRef{},
			},
			Status: v1alpha1.DataSourceClaimStatus{
				Phase:           v1alpha1.DSCPhaseBound,
				BoundDataSource: "test-ds",
			},
		}

		// Create test DataSource
		ds := &v1alpha1.DataSource{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-ds",
			},
			Spec: v1alpha1.DataSourceSpec{
				System: "amoro",
				Type:   "table",
				Name:   "dc1.db1.orders",
			},
		}

		// Setup test controller
		controller := setupTestController(ctx, dsc)

		// Add DataSource to the fake client
		controller.datadependencyClient.DatadependencyV1alpha1().DataSources().Create(ctx, ds, metav1.CreateOptions{})

		// Call handleUnbinding - should not fail even if findAssociatedRB fails
		err := controller.handleUnbinding(dsc, ds)
		assert.NoError(t, err)

		// Verify DSC status was still reset despite the error
		updatedDSC, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSourceClaims("default").Get(ctx, "test-dsc", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Equal(t, v1alpha1.DSCPhasePending, updatedDSC.Status.Phase)
		assert.Empty(t, updatedDSC.Status.BoundDataSource)
	})

	t.Run("should handle triggerRescheduling failure gracefully", func(t *testing.T) {
		ctx := context.Background()

		// Create test DSC (bound state) with WorkloadRef (no labels/annotations)
		dsc := &v1alpha1.DataSourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-dsc",
				Namespace: "default",
				UID:       "test-dsc-uid",
			},
			Spec: v1alpha1.DataSourceClaimSpec{
				System:         "amoro",
				DataSourceType: "table",
				DataSourceName: "dc1.db1.orders",
				Workload: v1alpha1.WorkloadRef{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "default",
				},
			},
			Status: v1alpha1.DataSourceClaimStatus{
				Phase:           v1alpha1.DSCPhaseBound,
				BoundDataSource: "test-ds",
			},
		}

		// Create test DataSource
		ds := &v1alpha1.DataSource{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-ds",
			},
			Spec: v1alpha1.DataSourceSpec{
				System: "amoro",
				Type:   "table",
				Name:   "dc1.db1.orders",
				Locality: &v1alpha1.DataSourceLocality{
					ClusterNames: []string{"cluster1", "cluster2"},
				},
			},
		}

		// Create test ResourceBinding that will be found but will fail during triggerRescheduling
		// We'll add it to the informer store but not to the fake client, causing Get() to fail
		rb := &workv1alpha2.ResourceBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app-deployment",
				Namespace: "default",
				Annotations: map[string]string{
					PlacementInjectedAnnotation: "true",
					ExcludedClustersAnnotation:  "cluster3",
				},
			},
			Spec: workv1alpha2.ResourceBindingSpec{
				Resource: workv1alpha2.ObjectReference{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-app",
					Namespace:  "default",
				},
				Placement: &policyv1alpha1.Placement{
					ClusterAffinity: &policyv1alpha1.ClusterAffinity{
						ExcludeClusters: []string{"cluster3"},
					},
				},
			},
		}

		// Setup test controller
		controller := setupTestController(ctx, dsc)

		// Add DataSource to the fake client
		controller.datadependencyClient.DatadependencyV1alpha1().DataSources().Create(ctx, ds, metav1.CreateOptions{})

		// Add ResourceBinding to the informer store (for findAssociatedRBs to find it)
		// but NOT to the fake client (so triggerRescheduling will fail when trying to Get it)
		controller.karmadaInformerFactory.Work().V1alpha2().ResourceBindings().Informer().GetStore().Add(rb)

		// Wait for cache sync
		cache.WaitForCacheSync(ctx.Done(), controller.karmadaInformerFactory.Work().V1alpha2().ResourceBindings().Informer().HasSynced)

		// Call handleUnbinding - should not fail even if triggerRescheduling fails
		err := controller.handleUnbinding(dsc, ds)
		assert.NoError(t, err)

		// Verify DSC status was still reset despite the triggerRescheduling error
		updatedDSC, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSourceClaims("default").Get(ctx, "test-dsc", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Equal(t, v1alpha1.DSCPhasePending, updatedDSC.Status.Phase)
		assert.Empty(t, updatedDSC.Status.BoundDataSource)

		// Verify DataSource claim ref was still removed despite the triggerRescheduling error
		updatedDS, err := controller.datadependencyClient.DatadependencyV1alpha1().DataSources().Get(ctx, "test-ds", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Empty(t, updatedDS.Status.ClaimRefs)
	})
}
