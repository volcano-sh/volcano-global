/*
Copyright 2025 The Volcano Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUTHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package datadependency

import (
	"context"
	"fmt"
	"reflect"

	workv1alpha2 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	"volcano.sh/apis/pkg/apis/datadependency/v1alpha1"
)

// GenerateWorkloadRefIndexKey generates an index key for a workload reference.
func GenerateWorkloadRefIndexKey(workloadRef *v1alpha1.WorkloadRef) (string, error) {
	if workloadRef == nil {
		return "", fmt.Errorf("workloadRef is nil")
	}

	// Validate required fields according to API definition
	if workloadRef.APIVersion == "" {
		return "", fmt.Errorf("workloadRef.APIVersion is required but empty")
	}
	if workloadRef.Kind == "" {
		return "", fmt.Errorf("workloadRef.Kind is required but empty")
	}
	if workloadRef.Name == "" {
		return "", fmt.Errorf("workloadRef.Name is required but empty")
	}

	return fmt.Sprintf("%s/%s/%s/%s", workloadRef.APIVersion, workloadRef.Kind, workloadRef.Namespace, workloadRef.Name), nil
}

// GenerateWorkloadRefIndexKeyFromResource generates an index key for a resource.
func GenerateWorkloadRefIndexKeyFromResource(resource workv1alpha2.ObjectReference) (string, error) {
	return fmt.Sprintf("%s/%s/%s/%s", resource.APIVersion, resource.Kind, resource.Namespace, resource.Name), nil
}

// addResourceBinding handles the addition of a ResourceBinding and enqueues related DataSourceClaims.
func (c *DataDependencyController) addResourceBinding(obj interface{}) {
	rb, ok := obj.(*workv1alpha2.ResourceBinding)
	if !ok {
		klog.Errorf("Failed to convert to ResourceBinding: %v", obj)
		return
	}

	workloadRefKey, err := GenerateWorkloadRefIndexKeyFromResource(rb.Spec.Resource)
	if err != nil {
		klog.Errorf("Failed to generate workload ref index key from resource binding %s/%s: %v", rb.Namespace, rb.Name, err)
		return
	}

	// Use the indexer to find matching DSCs
	indexer := c.dataInformerFactory.Datadependency().V1alpha1().DataSourceClaims().Informer().GetIndexer()
	indexedObjs, err := indexer.ByIndex(WorkloadRefIndex, workloadRefKey)
	if err != nil {
		klog.Errorf("Failed to get DSCs by workloadRef index %s: %v", workloadRefKey, err)
		return
	}

	for _, obj := range indexedObjs {
		dsc, ok := obj.(*v1alpha1.DataSourceClaim)
		if !ok {
			klog.Errorf("Failed to convert indexed object to DataSourceClaim: %v", obj)
			continue
		}

		// Only process DSCs in the same namespace as the ResourceBinding
		if dsc.Namespace != rb.Namespace {
			continue
		}

		klog.Infof("ResourceBinding %s/%s is associated with DataSourceClaim %s/%s, enqueueing dsc",
			rb.Namespace, rb.Name, dsc.Namespace, dsc.Name)

		// Directly enqueue without wrapper function
		key, err := cache.MetaNamespaceKeyFunc(dsc)
		if err != nil {
			klog.Errorf("Failed to get key for DataSourceClaim %s/%s: %v", dsc.Namespace, dsc.Name, err)
			continue
		}
		c.queue.Add(key)
	}
}

// ConfigMap event handlers
func (c *DataDependencyController) addConfigMap(obj interface{}) {
	cm := obj.(*corev1.ConfigMap)
	if c.isPluginConfigMap(cm) {
		klog.V(4).Infof("Adding plugin ConfigMap %s/%s", cm.Namespace, cm.Name)
		c.handleConfigMapChange(cm)
	}
}

func (c *DataDependencyController) updateConfigMap(oldObj, newObj interface{}) {
	oldCM := oldObj.(*corev1.ConfigMap)
	newCM := newObj.(*corev1.ConfigMap)

	if c.isPluginConfigMap(newCM) && !reflect.DeepEqual(oldCM.Data, newCM.Data) {
		klog.V(4).Infof("Updating plugin ConfigMap %s/%s", newCM.Namespace, newCM.Name)
		c.handleConfigMapChange(newCM)
	}
}

func (c *DataDependencyController) deleteConfigMap(obj interface{}) {
	cm, ok := obj.(*corev1.ConfigMap)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("couldn't get object from tombstone %#v", obj))
			return
		}
		cm, ok = tombstone.Obj.(*corev1.ConfigMap)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("tombstone contained object that is not a ConfigMap %#v", obj))
			return
		}
	}

	if c.isPluginConfigMap(cm) {
		klog.V(4).Infof("Deleting plugin ConfigMap %s/%s", cm.Namespace, cm.Name)
		// When ConfigMap is deleted, we might want to reset plugins to default configuration
		c.handleConfigMapDeletion(cm)
	}
}

func (c *DataDependencyController) isPluginConfigMap(cm *corev1.ConfigMap) bool {
	// Check if this is the plugin configuration ConfigMap
	return cm.Name == DefaultConfigMapName && cm.Namespace == DefaultConfigMapNamespace
}

func (c *DataDependencyController) handleConfigMapChange(cm *corev1.ConfigMap) {
	// Reload all plugin configurations when ConfigMap changes
	ctx := context.Background()
	if err := c.pluginManager.LoadConfigurations(ctx); err != nil {
		klog.Errorf("Failed to reload plugin configurations after ConfigMap change: %v", err)
		return
	}

	if err := c.pluginManager.InitializePlugins(ctx); err != nil {
		klog.Errorf("Failed to reinitialize plugins after ConfigMap change: %v", err)
		return
	}

	klog.Infof("Successfully reloaded plugin configurations from ConfigMap %s/%s", cm.Namespace, cm.Name)
}

func (c *DataDependencyController) handleConfigMapDeletion(cm *corev1.ConfigMap) {
	klog.Warningf("Plugin configuration ConfigMap %s/%s was deleted, plugins may not function correctly", cm.Namespace, cm.Name)
	// Could implement fallback to default configurations here
}

// DataSource event handlers
func (c *DataDependencyController) addDataSource(obj interface{}) {
	ds, ok := obj.(*v1alpha1.DataSource)
	if !ok {
		klog.Errorf("Expected DataSource but got %T", obj)
		return
	}

	klog.V(4).Infof("DataSource %s added. Triggering reconciliation for all pending DSCs.", ds.Name)

	// List all DataSourceClaims from the lister.
	// This operation is very cheap as it reads from the local cache.
	dscs, err := c.dscLister.List(labels.Everything())
	if err != nil {
		klog.Errorf("Failed to list DataSourceClaims while handling DS addition: %v", err)
		return
	}

	// Enqueue every DSC that is currently in the 'Pending' phase.
	for _, dsc := range dscs {
		if dsc.Status.Phase == v1alpha1.DSCPhasePending {
			key, err := cache.MetaNamespaceKeyFunc(dsc)
			if err != nil {
				klog.Warningf("Failed to get key for pending DSC %s/%s: %v", dsc.Namespace, dsc.Name, err)
				continue
			}
			klog.V(2).Infof("Enqueuing pending DSC %s due to new DS %s.", key, ds.Name)
			c.queue.Add(key)
		}
	}
}

func (c *DataDependencyController) updateDataSource(oldObj, newObj interface{}) {
	oldDS, ok := oldObj.(*v1alpha1.DataSource)
	if !ok {
		klog.Errorf("Expected DataSource but got %T for old object", oldObj)
		return
	}
	newDS, ok := newObj.(*v1alpha1.DataSource)
	if !ok {
		klog.Errorf("Expected DataSource but got %T for new object", newObj)
		return
	}

	klog.V(4).Infof("DataSource updated: %s", newDS.Name)

	// Check if this is a deletion event (DeletionTimestamp is set)
	if newDS.DeletionTimestamp != nil {

		// Case A: Deletion just started (Edge Trigger)
		// oldDS has no DeletionTimestamp, but newDS has one.
		if oldDS.DeletionTimestamp == nil {
			klog.V(4).Infof("DataSource %s marked for deletion", newDS.Name)

			if len(newDS.Status.ClaimRefs) > 0 {
				// A.1: Bound DSCs exist.
				// Action: Enqueue all DSCs to trigger the unbinding flow.
				klog.V(4).Infof("DataSource %s has %d bound DSCs, triggering unbinding flow", newDS.Name, len(newDS.Status.ClaimRefs))
				for _, claimRef := range newDS.Status.ClaimRefs {
					key := claimRef.Namespace + "/" + claimRef.Name
					klog.V(4).Infof("Enqueuing bound DSC %s due to DataSource %s deletion start", key, newDS.Name)
					c.queue.Add(key)
				}
			} else {
				// A.2: No bound DSCs (Idle DataSource).
				// Action: Remove finalizer immediately to allow physical deletion.
				klog.V(4).Infof("DataSource %s is idle (no refs), removing finalizer immediately", newDS.Name)
				if err := c.cleanupDataSourceFinalizer(newDS); err != nil {
					klog.Errorf("Failed to cleanup finalizer for %s: %v", newDS.Name, err)
				}
			}
			return // Exit after handling deletion initiation
		}

		// Case B: Deletion in progress (Level Trigger)
		// oldDS already had DeletionTimestamp. This update is likely a Status update
		// triggered by a DSC unbinding (removeClaimRefFromDS).
		// We check if all references have been cleared.
		if len(newDS.Status.ClaimRefs) == 0 {
			klog.V(4).Infof("DataSource %s is terminating and references dropped to zero. Removing finalizer.", newDS.Name)
			// Action: The last DSC has unbound, so it is safe to remove the finalizer.
			if err := c.cleanupDataSourceFinalizer(newDS); err != nil {
				klog.Errorf("Failed to cleanup finalizer for %s: %v", newDS.Name, err)
			}
		}

		// Note: If len > 0, it means unbinding is still in progress for other DSCs.
		// We do nothing here and wait for the next status update event.
		return
	}

	// Process Locality changes in DataSource Spec
	// We ignore other spec fields and status updates since DS is a cache resource
	localityChanged := !reflect.DeepEqual(oldDS.Spec.Locality, newDS.Spec.Locality)
	if !localityChanged {
		klog.V(5).Infof("DataSource %s updated, but Locality unchanged. Skipping.", newDS.Name)
		return
	}

	klog.V(4).Infof("DataSource %s Locality changed, will trigger ResourceBinding rescheduling for bound DSCs.", newDS.Name)

	// Only enqueue DSCs that were previously bound to this DataSource
	// This triggers ResourceBinding rescheduling for existing bindings
	// Pending DSCs don't need to be enqueued since DS spec changes won't affect their matching
	for _, claimRef := range oldDS.Status.ClaimRefs {
		key := claimRef.Namespace + "/" + claimRef.Name
		klog.V(4).Infof("Enqueuing previously bound DSC %s due to DataSource %s Locality change.", key, newDS.Name)
		c.queue.Add(key)
	}

	// Note: We don't enqueue pending DSCs here since they will be handled by their own reconciliation logic
	// and the updated DataSource will be available when they try to bind.
}

func (c *DataDependencyController) deleteDataSource(obj interface{}) {
	var ds *v1alpha1.DataSource

	// When a delete event occurs, the object may be passed as a DeletionFinalStateUnknown struct.
	// This happens if the watch connection is lost and the controller re-lists objects.
	// We need to handle this case to get the actual object.
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		ds, ok = tombstone.Obj.(*v1alpha1.DataSource)
		if !ok {
			klog.Errorf("Expected DataSource in DeletedFinalStateUnknown but got %T", tombstone.Obj)
			return
		}
	} else {
		ds, ok = obj.(*v1alpha1.DataSource)
		if !ok {
			klog.Errorf("Expected DataSource but got %T", obj)
			return
		}
	}

	klog.V(4).Infof("DataSource deleted: %s", ds.Name)

	// If the DS has no ClaimRefs, then no DSCs are affected.
	if len(ds.Status.ClaimRefs) == 0 {
		klog.V(4).Infof("Deleted DataSource %s had no ClaimRefs. No DSCs to reconcile.", ds.Name)
		return
	}

	// Note: This function is called when the DataSource is actually removed from etcd
	// after all finalizers are removed. The unbinding logic should have already been
	// triggered in updateDataSource when DeletionTimestamp was set (soft delete).
	// However, we still enqueue the DSCs here as a safety measure in case some were missed.

	klog.V(4).Infof("DataSource %s hard deleted. Enqueuing %d bound DSCs as a safety measure.", ds.Name, len(ds.Status.ClaimRefs))

	// Enqueue all DSCs that were referencing this DS.
	for _, claimRef := range ds.Status.ClaimRefs {
		// The claimRef contains the namespace and name of the DSC.
		key := claimRef.Namespace + "/" + claimRef.Name
		klog.V(4).Infof("Enqueuing bound DSC %s due to hard deletion of DS %s.", key, ds.Name)
		c.queue.Add(key)
	}
}

// DataSourceClaim event handlers

// addDataSourceClaim handles the addition of a DataSourceClaim.
func (c *DataDependencyController) addDataSourceClaim(obj interface{}) {
	dsc, ok := obj.(*v1alpha1.DataSourceClaim)
	if !ok {
		klog.Errorf("Expected DataSourceClaim but got %T", obj)
		return
	}
	klog.V(4).Infof("DataSourceClaim added: %s/%s", dsc.Namespace, dsc.Name)
	key, err := cache.MetaNamespaceKeyFunc(obj)
	if err != nil {
		klog.Errorf("Failed to get key for DataSourceClaim: %v", err)
		return
	}
	c.queue.Add(key)
}

// updateDataSourceClaim handles the update of a DataSourceClaim.
func (c *DataDependencyController) updateDataSourceClaim(oldObj, newObj interface{}) {
	dsc, ok := newObj.(*v1alpha1.DataSourceClaim)
	if !ok {
		klog.Errorf("Expected DataSourceClaim but got %T", newObj)
		return
	}
	klog.V(4).Infof("DataSourceClaim updated: %s/%s", dsc.Namespace, dsc.Name)
	key, err := cache.MetaNamespaceKeyFunc(newObj)
	if err != nil {
		klog.Errorf("Failed to get key for DataSourceClaim: %v", err)
		return
	}
	c.queue.Add(key)
}

// deleteDataSourceClaim handles the deletion of a DataSourceClaim.
func (c *DataDependencyController) deleteDataSourceClaim(obj interface{}) {
	// Extract the DataSourceClaim from the object, handling the case where it might be wrapped in a tombstone
	var dsc *v1alpha1.DataSourceClaim
	var ok bool

	if tombstone, isTombstone := obj.(cache.DeletedFinalStateUnknown); isTombstone {
		// Object was deleted while the controller was not running
		dsc, ok = tombstone.Obj.(*v1alpha1.DataSourceClaim)
		if !ok {
			klog.Errorf("Expected DataSourceClaim in tombstone but got %T", tombstone.Obj)
			return
		}
	} else {
		// Normal delete event
		dsc, ok = obj.(*v1alpha1.DataSourceClaim)
		if !ok {
			klog.Errorf("Expected DataSourceClaim but got %T", obj)
			return
		}
	}

	// Log the deletion event
	klog.V(4).Infof("DataSourceClaim deleted: %s/%s", dsc.Namespace, dsc.Name)

	// Note: Cleanup work is already handled by the finalizer in handleDeletion
	// during the soft delete phase, so we don't need to add the object to the work queue
}
