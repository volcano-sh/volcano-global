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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	policyv1alpha1 "github.com/karmada-io/karmada/pkg/apis/policy/v1alpha1"
	workv1alpha2 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/client-go/tools/reference"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"volcano.sh/apis/pkg/apis/datadependency/v1alpha1"
)

// handleDSCDeletion performs the cleanup logic when a DataSourceClaim is being deleted.
func (c *DataDependencyController) handleDeletion(dsc *v1alpha1.DataSourceClaim) error {
	klog.V(4).Infof("Handling deletion for DSC %s/%s", dsc.Namespace, dsc.Name)

	// Check if the finalizer is still present. If not, the cleanup is done.
	if !controllerutil.ContainsFinalizer(dsc, DataSourceClaimFinalizer) {
		klog.V(4).Infof("Finalizer already removed for DSC %s/%s. Skipping deletion logic.", dsc.Namespace, dsc.Name)
		return nil
	}

	// --- Core Cleanup Logic ---

	var allErrors []error
	// Check if there's a bound DataSource to clean up.
	if dsc.Status.BoundDataSource != "" {
		dsName := dsc.Status.BoundDataSource
		err := c.removeClaimRefFromDS(dsName, dsc.UID)
		if err != nil {
			// If we fail to find the DS, it might have been deleted already.
			// We should not treat 'NotFound' as a hard error that blocks finalizer removal.
			if errors.IsNotFound(err) {
				klog.Warningf("Bound DataSource %s for DSC %s/%s not found. Assuming it was deleted.", dsName, dsc.Namespace, dsc.Name)
			} else {
				allErrors = append(allErrors, err)
			}
		}
	}

	// If any of the cleanup steps failed, return an aggregated error.
	// This will cause the reconcile to be retried, and the finalizer will NOT be removed.
	if len(allErrors) > 0 {
		return utilerrors.NewAggregate(allErrors)
	}

	// --- Finalizer Removal ---

	// If all cleanup was successful, remove our finalizer from the list.
	dscToUpdate := dsc.DeepCopy()
	controllerutil.RemoveFinalizer(dscToUpdate, DataSourceClaimFinalizer)

	// Update the object to persist the finalizer removal.
	_, err := c.datadependencyClient.DatadependencyV1alpha1().DataSourceClaims(dscToUpdate.Namespace).Update(context.TODO(), dscToUpdate, metav1.UpdateOptions{})
	if err != nil {
		klog.Errorf("Failed to remove finalizer from DSC %s/%s: %v", dsc.Namespace, dsc.Name, err)
		return err
	}

	klog.V(2).Infof("Successfully removed finalizer from DSC %s/%s.", dsc.Namespace, dsc.Name)
	return nil
}

func (c *DataDependencyController) initializePhase(dsc *v1alpha1.DataSourceClaim) error {
	klog.V(2).Infof("Init the DataSourceClaim phase: %v", dsc.Name)
	dscToUpdate := dsc.DeepCopy()
	dscToUpdate.Status.Phase = v1alpha1.DSCPhasePending
	return c.updateDSCstatus(dscToUpdate)
}

func (c *DataDependencyController) handlePending(dsc *v1alpha1.DataSourceClaim) error {
	klog.V(4).Infof("Start processing DSC %s/%s", dsc.Namespace, dsc.Name)
	dsList, err := c.dsLister.List(labels.Everything())
	if err != nil {
		klog.Errorf("Failed to list DataSource: %v", err)
		return err
	}

	matchedDS, err := findMatchingDataSource(dsc, dsList)
	if err != nil {
		klog.Errorf("Something wrong when match dsc & dsList, %v", err)
		return err
	}

	if matchedDS != nil {
		// found matched datasource
		klog.V(4).Infof("DSC %s/%s: Found static match with DSList , procedding to bind the datasource.", dsc.Namespace, dsc.Name)
		return c.staticBinding(dsc, matchedDS)
	}

	// We can't find correct datasource form now dsLister,
	// execute dynamic binding
	klog.V(4).Infof("DSC %s/%s; No static match found, attempting dynamic provisioning", dsc.Namespace, dsc.Name)
	return c.dynamicBinding(dsc)
}

// findMatchingDataSource attempts to find a cached DataSource that exactly matches the spec of a DataSourceClaim.
// In the "DS as Query Cache" model, a match is found if and only if a DS object exists
// with a spec that is a deep-equal match to the DSC's spec.
//
// Relationship: Each DSC binds to exactly one DS (one-to-one from DSC perspective),
// but a single DS can be shared by multiple DSCs (one-to-many from DS perspective).
// This enables efficient data sharing when multiple workloads need the same data.
//
// Returns the first matching DataSource or nil if no match is found.
func findMatchingDataSource(dsc *v1alpha1.DataSourceClaim, dsList []*v1alpha1.DataSource) (*v1alpha1.DataSource, error) {
	// The spec form the claim is the "key" we are looking for in our cache.
	for _, ds := range dsList {
		// Use the centralized matching logic.
		if doesDataSourceMatchClaim(dsc, ds) {
			// If all filters pass, this DS is the match we're looking for.
			klog.V(4).Infof("Found matching DataSource %s for DataSourceClaim %s/%s", ds.Name, dsc.Namespace, dsc.Name)
			return ds, nil
		}
	}

	// No matching DataSource found.
	klog.V(4).Infof("No matching DataSource found for DataSourceClaim %s/%s.", dsc.Namespace, dsc.Name)
	return nil, nil
}

// attributesEqual compares two map[string]string for deep equality.
// Returns true if both maps have the same key-value pairs, false otherwise.
func attributesEqual(a, b map[string]string) bool {
	// If both are nil, they are equal
	if a == nil && b == nil {
		return true
	}
	// If one is nil and the other is not, they are not equal
	if a == nil || b == nil {
		return false
	}
	// If they have different lengths, they are not equal
	if len(a) != len(b) {
		return false
	}
	// Check each key-value pair
	for key, valueA := range a {
		valueB, exists := b[key]
		if !exists || valueA != valueB {
			return false
		}
	}
	return true
}

// doesDataSourceMatchClaim checks if a given DataSource satisfies the requirements of a DataSourceClaim.
// This is the core matching logic used for both initial binding and re-validation.
//
// This function implements the matching criteria for the DSC-DS relationship:
// - Each DSC can bind to exactly one DS that matches its spec
// - Multiple DSCs can share the same DS if they have identical specs
// - This enables efficient data sharing across workloads
func doesDataSourceMatchClaim(dsc *v1alpha1.DataSourceClaim, ds *v1alpha1.DataSource) bool {
	// Check 0: DataSource must not be marked for deletion.
	// If DeletionTimestamp is set, the DataSource is being deleted and should not be considered valid for binding.
	if ds.DeletionTimestamp != nil {
		return false
	}
	// Check 1: System must match.
	if dsc.Spec.System != ds.Spec.System {
		return false
	}
	// Check 2: Type must match.
	if dsc.Spec.DataSourceType != ds.Spec.Type {
		return false
	}
	// Check 3: Name must match.
	if dsc.Spec.DataSourceName != ds.Spec.Name {
		return false
	}
	// Check 4: Attributes must be equal.
	return attributesEqual(ds.Spec.Attributes, dsc.Spec.Attributes)
}

// staticBinding performs the binding process between a DataSourceClaim and a matched DataSource.
// The primary goal is to update the DSC's status with the matched DS name to ensure
// the controller has full visibility. Updating the back-reference (ClaimRef) on the DS is a secondary,
// best-effort operation.
func (c *DataDependencyController) staticBinding(dsc *v1alpha1.DataSourceClaim, matchedDS *v1alpha1.DataSource) error {
	klog.V(4).Infof("Starting static binding for DSC %s/%s with matched DataSource %s.", dsc.Namespace, dsc.Name, matchedDS.Name)

	// --- 1. The most critical step: Update the DataSourceClaim's status first. ---
	// This ensures the controller has the data location information for further processing.

	dscToUpdate := dsc.DeepCopy()
	dscToUpdate.Status.BoundDataSource = matchedDS.Name
	dscToUpdate.Status.Phase = v1alpha1.DSCPhaseBound

	boundCondition := metav1.Condition{
		Type:               "Bound",
		Status:             metav1.ConditionTrue,
		Reason:             "BindingSuccessful",
		Message:            fmt.Sprintf("Successfully bound to DataSource: %s", matchedDS.Name),
		LastTransitionTime: metav1.Now(),
	}
	meta.SetStatusCondition(&dscToUpdate.Status.Conditions, boundCondition)

	// Attempt to update the DSC status. If this fails, we must retry the whole process.
	if err := c.updateDSCstatus(dscToUpdate); err != nil {
		klog.Errorf("Crucial error: Failed to update status for DSC %s/%s. Retrying. Error: %v", dsc.Namespace, dsc.Name, err)
		return err // Return error to force a requeue.
	}

	// --- 2. Best-effort: Update the status of the matched DataSource. ---
	// Failures in this part will not fail the main binding process. They will be logged
	// and can be reconciled later.

	claimRef, err := reference.GetReference(c.Scheme, dsc)
	if err != nil {
		// This is a controller-side error, but not critical enough to fail the binding. Log it.
		klog.Errorf("Could not generate ClaimRef for DSC %s/%s, DS back-references will be missing. Error: %v", dsc.Namespace, dsc.Name, err)
		// We can proceed without the claimRef.
		return nil
	}

	if err := c.addClaimRefToDS(matchedDS, claimRef); err != nil {
		// Log the error but do not return it. The binding is already considered successful.
		klog.Warningf("Best-effort failed: could not add ClaimRef to DS %s for DSC %s/%s. This may be reconciled later. Error: %v", matchedDS.Name, dsc.Namespace, dsc.Name, err)
	}

	// --- 3. Finalize ---
	c.recorder.Eventf(dsc, corev1.EventTypeNormal, "Bound", "Successfully bound to DataSource %s", matchedDS.Name)
	klog.V(4).Infof("Successfully completed static binding for DSC %s/%s. Main status updated, DS back-reference updated on a best-effort basis.", dsc.Namespace, dsc.Name)

	return nil // The binding is successful from the controller's perspective.
}

// generateDataSourceName creates a unique name for a DataSource based on the DSC spec.
// If the DSC has attributes, a hash of the attributes is included to ensure uniqueness.
func generateDataSourceName(dsc *v1alpha1.DataSourceClaim) string {
	baseName := fmt.Sprintf("%s-%s-%s", dsc.Spec.System, dsc.Spec.DataSourceType, dsc.Spec.DataSourceName)

	// If no attributes, return the base name
	if len(dsc.Spec.Attributes) == 0 {
		return baseName
	}

	// Create a deterministic hash of the attributes
	// Sort keys to ensure consistent ordering
	var keys []string
	for k := range dsc.Spec.Attributes {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Build a string representation of the attributes
	var attrStr strings.Builder
	for i, k := range keys {
		if i > 0 {
			attrStr.WriteString(",")
		}
		attrStr.WriteString(k)
		attrStr.WriteString("=")
		attrStr.WriteString(dsc.Spec.Attributes[k])
	}

	// Create SHA256 hash and take first 8 characters
	hash := sha256.Sum256([]byte(attrStr.String()))
	hashStr := hex.EncodeToString(hash[:])[:8]

	return fmt.Sprintf("%s-%s", baseName, hashStr)
}

func (c *DataDependencyController) dynamicBinding(dsc *v1alpha1.DataSourceClaim) error {
	// Use plugin manager to select clusters for the data source claim
	clusters, err := c.pluginManager.SelectClusters(context.TODO(), dsc)
	if err != nil {
		// Detect non-retryable "not found" scenarios from plugin using structured error checking
		// In such cases, do not requeue; just return nil to stop retrying.
		if IsNotFoundErr(err) {
			klog.Warningf("No data found for DSC %s/%s from plugin. Not retrying. Error: %v", dsc.Namespace, dsc.Name, err)
			return nil
		}

		klog.Errorf("Failed to select clusters for DSC %s/%s: %v", dsc.Namespace, dsc.Name, err)
		return err
	}

	if len(clusters) == 0 {
		klog.V(3).Infof("No clusters selected for DSC %s/%s, keeping in Pending state", dsc.Namespace, dsc.Name)
		return nil
	}

	// Create DataSource with selected clusters
	// Use data source information for naming to ensure uniqueness even if DSC changes data sources
	ds := &v1alpha1.DataSource{
		ObjectMeta: metav1.ObjectMeta{
			Name: generateDataSourceName(dsc),
			// Initialize with Finalizer
			Finalizers: []string{DataSourceFinalizer},
		},
		Spec: v1alpha1.DataSourceSpec{
			System:     dsc.Spec.System,
			Type:       dsc.Spec.DataSourceType,
			Name:       dsc.Spec.DataSourceName,
			Attributes: dsc.Spec.Attributes,
			Locality: &v1alpha1.DataSourceLocality{
				ClusterNames: clusters,
			},
		},
	}

	// Create the DataSource
	klog.V(4).Infof("Creating DataSource %s for DSC %s/%s", ds.Name, dsc.Namespace, dsc.Name)
	_, err = c.datadependencyClient.DatadependencyV1alpha1().DataSources().Create(context.TODO(), ds, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("failed to create DataSource: %v", err)
	}

	// Bind the DSC to the created DS
	return c.staticBinding(dsc, ds)
}

func (c *DataDependencyController) updateDSCstatus(dsc *v1alpha1.DataSourceClaim) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		// Fetch latest object; if it no longer exists, treat as success and stop.
		latestDSC, err := c.datadependencyClient.DatadependencyV1alpha1().DataSourceClaims(dsc.Namespace).Get(context.TODO(), dsc.Name, metav1.GetOptions{})
		if err != nil {
			if errors.IsNotFound(err) {
				klog.V(2).Infof("DataSourceClaim %s/%s not found, skip status update", dsc.Namespace, dsc.Name)
				return nil
			}
			return err
		}

		// Apply desired status and update via status subresource.
		latestDSC.Status = dsc.Status
		_, err = c.datadependencyClient.DatadependencyV1alpha1().DataSourceClaims(dsc.Namespace).UpdateStatus(context.TODO(), latestDSC, metav1.UpdateOptions{})
		if err != nil {
			if errors.IsNotFound(err) {
				klog.V(2).Infof("DataSourceClaim %s/%s not found during status update, skip", dsc.Namespace, dsc.Name)
				return nil
			}
			klog.Errorf("Failed to update DataSourceClaim's status: %v", err)
			return err
		}
		return nil
	})
}

// addClaimRefToDS adds a reference to a DataSourceClaim to a DataSource's status.
// It uses RetryOnConflict to handle concurrent updates safely.
// This function is idempotent: it will not add a duplicate reference.
func (c *DataDependencyController) addClaimRefToDS(ds *v1alpha1.DataSource, claimRef *corev1.ObjectReference) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		// Get the latest version of the DataSource from the lister or API server.
		latestDS, err := c.datadependencyClient.DatadependencyV1alpha1().DataSources().Get(context.TODO(), ds.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		dsToUpdate := latestDS.DeepCopy()

		// Check if the reference already exists to ensure idempotency.
		for _, ref := range dsToUpdate.Status.ClaimRefs {
			if ref.UID == claimRef.UID {
				klog.V(5).Infof("ClaimRef for DSC %s/%s already exists in DS %s. Skipping update.", claimRef.Namespace, claimRef.Name, ds.Name)
				return nil // Nothing to do.
			}
		}

		// Check if DataSource needs finalizer
		needsFinalizerUpdate := false
		if !controllerutil.ContainsFinalizer(dsToUpdate, DataSourceFinalizer) {
			controllerutil.AddFinalizer(dsToUpdate, DataSourceFinalizer)
			needsFinalizerUpdate = true
			klog.V(4).Infof("Adding finalizer to DataSource %s", ds.Name)
		}

		// Strategy: Try to update status first (most common case)
		// If finalizer is needed, we'll handle it in a separate retry
		if !needsFinalizerUpdate {
			// No finalizer update needed, directly update status
			dsToUpdate.Status.ClaimRefs = append(dsToUpdate.Status.ClaimRefs, *claimRef)
			dsToUpdate.Status.BoundClaims = int32(len(dsToUpdate.Status.ClaimRefs))

			_, err = c.datadependencyClient.DatadependencyV1alpha1().DataSources().UpdateStatus(context.TODO(), dsToUpdate, metav1.UpdateOptions{})
			return err
		}

		// Finalizer update is needed - update object first, then status will be updated in next retry
		_, err = c.datadependencyClient.DatadependencyV1alpha1().DataSources().Update(context.TODO(), dsToUpdate, metav1.UpdateOptions{})
		if err != nil {
			return err
		}

		// Return a specific error to trigger retry for status update
		// This will cause RetryOnConflict to retry, and in the next iteration,
		// the finalizer will already be present, so we'll go to the status update path
		return errors.NewConflict(v1alpha1.Resource("datasource"), ds.Name, fmt.Errorf("finalizer added, retry for status update"))
	})
}

// removeClaimRefFromDS removes the reference of a DSC from a DS's status.
// It is idempotent and safe for concurrent calls.
// If the DS becomes un-referenced and its reclaim policy is 'Delete', it will be deleted.
func (c *DataDependencyController) removeClaimRefFromDS(dsName string, dscUID types.UID) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		klog.Infof("Attempt remove ClaimRef for DSC UID %s from DS %s", dscUID, dsName)
		latestDS, err := c.datadependencyClient.DatadependencyV1alpha1().DataSources().Get(context.TODO(), dsName, metav1.GetOptions{})
		if err != nil {
			return err // Let RetryOnConflict handle NotFound or other errors.
		}

		dsToUpdate := latestDS.DeepCopy()

		// Find and remove the claimRef.
		var newClaimRefs []corev1.ObjectReference
		found := false
		for _, ref := range dsToUpdate.Status.ClaimRefs {
			if ref.UID == dscUID {
				found = true
			} else {
				newClaimRefs = append(newClaimRefs, ref)
			}
		}

		// If the reference was not found, our work is already done.
		if !found {
			klog.V(4).Infof("ClaimRef for UID %s not found in DS %s. Assuming already removed.", dscUID, dsName)
			return nil
		}

		// Update status
		dsToUpdate.Status.ClaimRefs = newClaimRefs
		dsToUpdate.Status.BoundClaims = int32(len(newClaimRefs))

		// Attempt to update the status.
		updatedDS, updateErr := c.datadependencyClient.DatadependencyV1alpha1().DataSources().UpdateStatus(context.TODO(), dsToUpdate, metav1.UpdateOptions{})
		if updateErr != nil {
			return updateErr
		}

		// --- Handle Reclaim Policy ---
		// After a successful update, check if the DS should be reclaimed.
		if len(updatedDS.Status.ClaimRefs) == 0 && updatedDS.Spec.ReclaimPolicy == v1alpha1.ReclaimPolicyDelete {
			klog.V(2).Infof("DS %s is now un-referenced and has ReclaimPolicy 'Delete'. Deleting.", updatedDS.Name)
			if err := c.datadependencyClient.DatadependencyV1alpha1().DataSources().Delete(context.TODO(), updatedDS.Name, metav1.DeleteOptions{}); err != nil {
				// If deletion fails, log it. The DS might become an orphan,
				// but this shouldn't block the DSC's deletion.
				klog.Errorf("Failed to delete reclaimed DS %s: %v", updatedDS.Name, err)
				// We don't return the error here to avoid blocking the DSC finalizer removal.
				// Orphaned DS can be cleaned up manually or by a separate garbage collector.
			}
		}

		return nil
	})
}

// Shared constants moved to pkg/constants.

// handleBound orchestrates the process of injecting placement rules into a ResourceBinding
// for a 'Bound' DataSourceClaim.
func (c *DataDependencyController) handleBound(dsc *v1alpha1.DataSourceClaim) error {
	klog.V(4).Infof("Handling bound DSC %s/%s", dsc.Namespace, dsc.Name)

	dsName := dsc.Status.BoundDataSource
	ds, err := c.datadependencyClient.DatadependencyV1alpha1().DataSources().Get(context.TODO(), dsName, metav1.GetOptions{})
	if err != nil {
		klog.Errorf("Failed to get DS %s for DSC %s/%s: %v", dsName, dsc.Namespace, dsc.Name, err)
		return err
	}

	// 1. Check if the binding is still valid.
	// This involves fetching the bound DS and comparing its properties with the DSC's requirements.
	isValid := doesDataSourceMatchClaim(dsc, ds)

	// 2. Based on validity, decide which logic branch to take.
	if isValid {
		// The binding is still valid.
		// This branch handles injecting/updating data placement into the associated ResourceBinding.
		// It covers initial injection, updates due to DS locality changes, or workload selector changes.
		klog.V(4).Infof("Binding for DSC %s/%s is still valid. Proceeding with placement update.", dsc.Namespace, dsc.Name)
		return c.handlePlacementUpdate(dsc, ds)
	}

	// 3. The binding is no longer valid.
	// This branch handles the unbinding process, resetting the DSC's status to allow for rebinding.
	// This is triggered if the DSC's spec changes or the bound DS is deleted/modified.
	klog.Infof("Binding for DSC %s/%s is no longer valid. Proceeding with unbinding.", dsc.Namespace, dsc.Name)
	return c.handleUnbinding(dsc, ds)
}

// handlePlacementUpdate implements the RB injection logic for bound DSCs.
// This function is called when a DSC is bound to a valid DS and needs placement updates.
// It handles three scenarios:
// 1. No RB found (Pending->Bound): return directly
// 2. RB found but not injected: inject placement affinity
// 3. RB found and already injected: trigger rescheduling
func (c *DataDependencyController) handlePlacementUpdate(dsc *v1alpha1.DataSourceClaim, ds *v1alpha1.DataSource) error {
	klog.V(4).Infof("Implementing RB injection logic for DSC %s/%s with DS %s", dsc.Namespace, dsc.Name, ds.Name)

	// 1. Extract cluster names from DS locality
	var dsClusterNames []string
	if ds.Spec.Locality != nil {
		dsClusterNames = ds.Spec.Locality.ClusterNames
	}

	// 2. Find associated ResourceBinding using workloadRef
	associatedRB, err := c.findAssociatedRB(dsc)
	if err != nil {
		klog.Errorf("Failed to find associated RB for DSC %s/%s: %v", dsc.Namespace, dsc.Name, err)
		return err
	}

	// Scenario 1: No RB found - DSC just bound but RB not created yet
	if associatedRB == nil {
		klog.V(4).Infof("No ResourceBinding found for DSC %s/%s, waiting for RB creation", dsc.Namespace, dsc.Name)
		return nil
	}

	// 3. Process the RB based on injection status
	{
		rb := associatedRB
		// Check if RB has already been injected
		if rb.Annotations != nil && rb.Annotations[PlacementInjectedAnnotation] == "true" {
			// Scenario 3: RB already injected - trigger rescheduling
			klog.V(4).Infof("RB %s/%s already injected, triggering rescheduling due to DS cluster changes", rb.Namespace, rb.Name)
			if err := c.triggerRescheduling(rb, dsClusterNames); err != nil {
				klog.Errorf("Failed to trigger rescheduling for RB %s/%s: %v", rb.Namespace, rb.Name, err)
				return err
			}
		} else {
			// Scenario 2: RB not injected - inject placement affinity
			klog.V(4).Infof("RB %s/%s not injected, performing initial injection", rb.Namespace, rb.Name)
			if err := c.injectPlacementAffinity(rb, dsClusterNames); err != nil {
				klog.Errorf("Failed to inject placement affinity for RB %s/%s: %v", rb.Namespace, rb.Name, err)
				return err
			}
		}
	}

	klog.V(4).Infof("Successfully updated placement for DSC %s/%s with associated RB %s/%s", dsc.Namespace, dsc.Name, associatedRB.Namespace, associatedRB.Name)
	return nil
}

// findAssociatedRB finds the ResourceBinding associated with a DataSourceClaim via its workloadRef.
// Since DSC now specifies workloadRef, there is a one-to-one relationship between DSC and RB.
func (c *DataDependencyController) findAssociatedRB(dsc *v1alpha1.DataSourceClaim) (*workv1alpha2.ResourceBinding, error) {
	// Generate the workload reference key for indexer lookup
	workloadRefKey, err := GenerateWorkloadRefIndexKey(&dsc.Spec.Workload)
	if err != nil {
		return nil, fmt.Errorf("failed to generate workload ref key for DSC %s/%s: %v", dsc.Namespace, dsc.Name, err)
	}

	// Get the indexer from the ResourceBinding informer
	indexer := c.karmadaInformerFactory.Work().V1alpha2().ResourceBindings().Informer().GetIndexer()

	// Find ResourceBindings by workload reference index
	indexedObjs, err := indexer.ByIndex(WorkloadRefIndex, workloadRefKey)
	if err != nil {
		return nil, fmt.Errorf("failed to query ResourceBinding index for key %s: %v", workloadRefKey, err)
	}

	// Filter by namespace since the index is global
	// According to the design, there should be a one-to-one relationship between DSC and RB
	for _, obj := range indexedObjs {
		rb, ok := obj.(*workv1alpha2.ResourceBinding)
		if !ok {
			klog.Warningf("Expected ResourceBinding but got %T", obj)
			continue
		}
		if rb.Namespace == dsc.Namespace {
			klog.V(4).Infof("Found ResourceBinding %s/%s for DSC %s/%s", rb.Namespace, rb.Name, dsc.Namespace, dsc.Name)
			return rb, nil
		}
	}

	// No ResourceBinding found for this DSC
	klog.V(4).Infof("No associated ResourceBinding found for DSC %s/%s with workloadRef %s",
		dsc.Namespace, dsc.Name, workloadRefKey)
	return nil, nil
}

// triggerRescheduling triggers rescheduling of a ResourceBinding by clearing its clusters
// This allows the Karmada scheduler to reassign the workload based on updated DS locality
func (c *DataDependencyController) triggerRescheduling(rb *workv1alpha2.ResourceBinding, dsClusterNames []string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latestRB, err := c.karmadaClient.WorkV1alpha2().ResourceBindings(rb.Namespace).Get(context.TODO(), rb.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		rbToUpdate := latestRB.DeepCopy()

		// Initialize annotations if needed
		if rbToUpdate.Annotations == nil {
			rbToUpdate.Annotations = make(map[string]string)
		}

		// Update placement affinity with new DS cluster names
		if rbToUpdate.Spec.Placement == nil {
			rbToUpdate.Spec.Placement = &policyv1alpha1.Placement{}
		}

		// Set cluster affinity based on updated DS locality
		if len(dsClusterNames) > 0 {
			// Get all clusters first
			allClusters, err := c.cLister.List(labels.Everything())
			if err != nil {
				return fmt.Errorf("failed to list all clusters: %v", err)
			}

			// Convert dsClusterNames to a set for efficient lookup
			dsClusterSet := make(map[string]bool)
			for _, clusterName := range dsClusterNames {
				dsClusterSet[clusterName] = true
			}

			// Calculate new excluded clusters (all clusters - DS clusters)
			var newExcludedClusters []string
			for _, cluster := range allClusters {
				if !dsClusterSet[cluster.Name] {
					newExcludedClusters = append(newExcludedClusters, cluster.Name)
				}
			}

			// Get previously excluded clusters by this controller
			var previouslyExcluded []string
			if prevExcluded, exists := rbToUpdate.Annotations[ExcludedClustersAnnotation]; exists && prevExcluded != "" {
				previouslyExcluded = strings.Split(prevExcluded, ",")
			}

			// Merge with existing user-defined exclusions
			var finalExcludedClusters []string
			if rbToUpdate.Spec.Placement.ClusterAffinity != nil {
				// Start with existing exclusions, but remove our previous ones
				existingExcluded := rbToUpdate.Spec.Placement.ClusterAffinity.ExcludeClusters
				prevExcludedSet := make(map[string]bool)
				for _, cluster := range previouslyExcluded {
					prevExcludedSet[cluster] = true
				}

				// Keep user-defined exclusions (not added by us)
				for _, cluster := range existingExcluded {
					if !prevExcludedSet[cluster] {
						finalExcludedClusters = append(finalExcludedClusters, cluster)
					}
				}
			}

			// Add our new exclusions
			finalExcludedClusters = append(finalExcludedClusters, newExcludedClusters...)

			// Remove duplicates
			excludedSet := make(map[string]bool)
			var uniqueExcluded []string
			for _, cluster := range finalExcludedClusters {
				if !excludedSet[cluster] {
					excludedSet[cluster] = true
					uniqueExcluded = append(uniqueExcluded, cluster)
				}
			}

			// Update ClusterAffinity
			if rbToUpdate.Spec.Placement.ClusterAffinity == nil {
				rbToUpdate.Spec.Placement.ClusterAffinity = &policyv1alpha1.ClusterAffinity{}
			}
			rbToUpdate.Spec.Placement.ClusterAffinity.ExcludeClusters = uniqueExcluded

			// Record what we added for future cleanup (sorted for deterministic order)
			sort.Strings(newExcludedClusters)
			rbToUpdate.Annotations[ExcludedClustersAnnotation] = strings.Join(newExcludedClusters, ",")
		} else {
			// If no DS clusters, remove our previous exclusions
			if prevExcluded, exists := rbToUpdate.Annotations[ExcludedClustersAnnotation]; exists && prevExcluded != "" {
				previouslyExcluded := strings.Split(prevExcluded, ",")
				prevExcludedSet := make(map[string]bool)
				for _, cluster := range previouslyExcluded {
					prevExcludedSet[cluster] = true
				}

				if rbToUpdate.Spec.Placement.ClusterAffinity != nil {
					// Remove our previous exclusions, keep user-defined ones
					var remainingExcluded []string
					for _, cluster := range rbToUpdate.Spec.Placement.ClusterAffinity.ExcludeClusters {
						if !prevExcludedSet[cluster] {
							remainingExcluded = append(remainingExcluded, cluster)
						}
					}
					rbToUpdate.Spec.Placement.ClusterAffinity.ExcludeClusters = remainingExcluded
				}
				// Clear our annotations
				delete(rbToUpdate.Annotations, ExcludedClustersAnnotation)
				delete(rbToUpdate.Annotations, PlacementInjectedAnnotation)
			}
		}

		// Clear clusters to trigger rescheduling
		rbToUpdate.Spec.Clusters = nil

		klog.V(4).Infof("Triggering rescheduling for RB %s/%s by clearing clusters", rbToUpdate.Namespace, rbToUpdate.Name)

		// Attempt the update
		_, updateErr := c.karmadaClient.WorkV1alpha2().ResourceBindings(rbToUpdate.Namespace).Update(context.TODO(), rbToUpdate, metav1.UpdateOptions{})
		return updateErr
	})
}

// injectPlacementAffinity injects placement affinity into a ResourceBinding based on DS locality
// This function is only called for initial injection, so no need to handle previous exclusions
func (c *DataDependencyController) injectPlacementAffinity(rb *workv1alpha2.ResourceBinding, dsClusterNames []string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latestRB, err := c.karmadaClient.WorkV1alpha2().ResourceBindings(rb.Namespace).Get(context.TODO(), rb.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		rbToUpdate := latestRB.DeepCopy()

		// Add the annotation
		if rbToUpdate.Annotations == nil {
			rbToUpdate.Annotations = make(map[string]string)
		}
		rbToUpdate.Annotations[PlacementInjectedAnnotation] = "true"

		// Ensure Placement exists
		if rbToUpdate.Spec.Placement == nil {
			rbToUpdate.Spec.Placement = &policyv1alpha1.Placement{}
		}

		// Set cluster affinity based on DS locality
		if len(dsClusterNames) > 0 {
			// Get all clusters first
			allClusters, err := c.cLister.List(labels.Everything())
			if err != nil {
				return fmt.Errorf("failed to list all clusters: %v", err)
			}

			// Convert dsClusterNames to a set for efficient lookup
			dsClusterSet := make(map[string]bool)
			for _, clusterName := range dsClusterNames {
				dsClusterSet[clusterName] = true
			}

			// Calculate excluded clusters (all clusters - DS clusters)
			var excludedClusters []string
			for _, cluster := range allClusters {
				if !dsClusterSet[cluster.Name] {
					excludedClusters = append(excludedClusters, cluster.Name)
				}
			}

			// Merge with existing user-defined exclusions if any
			var finalExcludedClusters []string
			if rbToUpdate.Spec.Placement.ClusterAffinity != nil && len(rbToUpdate.Spec.Placement.ClusterAffinity.ExcludeClusters) > 0 {
				// Keep existing user-defined exclusions
				finalExcludedClusters = append(finalExcludedClusters, rbToUpdate.Spec.Placement.ClusterAffinity.ExcludeClusters...)
			}

			// Add our exclusions
			finalExcludedClusters = append(finalExcludedClusters, excludedClusters...)

			// Remove duplicates
			excludedSet := make(map[string]bool)
			var uniqueExcluded []string
			for _, cluster := range finalExcludedClusters {
				if !excludedSet[cluster] {
					excludedSet[cluster] = true
					uniqueExcluded = append(uniqueExcluded, cluster)
				}
			}

			// Update ClusterAffinity
			if rbToUpdate.Spec.Placement.ClusterAffinity == nil {
				rbToUpdate.Spec.Placement.ClusterAffinity = &policyv1alpha1.ClusterAffinity{}
			}
			rbToUpdate.Spec.Placement.ClusterAffinity.ExcludeClusters = uniqueExcluded

			// Record what we added for future cleanup (sorted for deterministic order)
			sort.Strings(excludedClusters)
			rbToUpdate.Annotations[ExcludedClustersAnnotation] = strings.Join(excludedClusters, ",")
		}
		// Note: If dsClusterNames is empty, we don't modify ClusterAffinity
		// This preserves any existing user configuration

		// Attempt the update
		_, updateErr := c.karmadaClient.WorkV1alpha2().ResourceBindings(rbToUpdate.Namespace).Update(context.TODO(), rbToUpdate, metav1.UpdateOptions{})
		return updateErr
	})
}

// handleUnbinding handles the unbinding process when a DSC-DS binding becomes invalid
// This function is called in two main scenarios:
// 1. DSC field is updated (non-workload field changes)
// 2. DataSource is being deleted
func (c *DataDependencyController) handleUnbinding(dsc *v1alpha1.DataSourceClaim, ds *v1alpha1.DataSource) error {
	klog.V(4).Infof("Handling unbinding for DSC %s/%s from DS %s", dsc.Namespace, dsc.Name, ds.Name)

	// Step 1: Reset DSC status first to prevent race conditions
	// This ensures DSC is marked as unbound before we start cleanup
	dscToUpdate := dsc.DeepCopy()
	dscToUpdate.Status.BoundDataSource = ""
	dscToUpdate.Status.Phase = v1alpha1.DSCPhasePending

	// Update condition to reflect unbinding
	unboundCondition := metav1.Condition{
		Type:               "Bound",
		Status:             metav1.ConditionFalse,
		Reason:             "BindingInvalid",
		Message:            "Binding became invalid, resetting to pending state",
		LastTransitionTime: metav1.Now(),
	}
	meta.SetStatusCondition(&dscToUpdate.Status.Conditions, unboundCondition)

	if err := c.updateDSCstatus(dscToUpdate); err != nil {
		klog.Errorf("Failed to update DSC status during unbinding: %v", err)
		return err
	}

	// Step 2: Clean up ResourceBinding placement rules
	// Find and trigger rescheduling for associated ResourceBinding
	rb, err := c.findAssociatedRB(dsc)
	if err != nil {
		klog.Errorf("Failed to find associated RB for DSC %s/%s: %v", dsc.Namespace, dsc.Name, err)
		// Continue with unbinding even if this fails
	} else if rb != nil {
		// Trigger rescheduling with nil dsClusterNames to clear placement rules
		if err := c.triggerRescheduling(rb, nil); err != nil {
			klog.Errorf("Failed to trigger rescheduling for RB %s/%s: %v", rb.Namespace, rb.Name, err)
			// Continue with unbinding even if this fails
		}
	}

	// Step 3: Remove claim reference from DataSource
	if err := c.removeClaimRefFromDS(ds.Name, dsc.UID); err != nil {
		klog.Errorf("Failed to remove claim ref from DS %s: %v", ds.Name, err)
	}

	return nil
}

// cleanupDataSourceFinalizer removes the finalizer from DataSource when it's being deleted
// and all ClaimRefs have been cleaned up
func (c *DataDependencyController) cleanupDataSourceFinalizer(ds *v1alpha1.DataSource) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		// Get the latest version of the DataSource
		latestDS, err := c.datadependencyClient.DatadependencyV1alpha1().DataSources().Get(context.TODO(), ds.Name, metav1.GetOptions{})
		if err != nil {
			if errors.IsNotFound(err) {
				// DS already deleted, nothing to do
				return nil
			}
			return err
		}

		// Remove the finalizer
		dsToUpdate := latestDS.DeepCopy()
		if controllerutil.ContainsFinalizer(dsToUpdate, DataSourceFinalizer) {
			controllerutil.RemoveFinalizer(dsToUpdate, DataSourceFinalizer)
			klog.V(4).Infof("Removing finalizer from DataSource %s", ds.Name)

			_, err = c.datadependencyClient.DatadependencyV1alpha1().DataSources().Update(context.TODO(), dsToUpdate, metav1.UpdateOptions{})
			return err
		}

		return nil
	})
}
