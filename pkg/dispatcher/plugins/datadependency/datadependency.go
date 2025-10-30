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
	workv1alpha2 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha2"
	"k8s.io/klog/v2"

	"volcano.sh/apis/pkg/apis/datadependency/v1alpha1"
	volcanoapi "volcano.sh/volcano/pkg/scheduler/api"

	"volcano.sh/volcano-global/pkg/dispatcher/api"
	"volcano.sh/volcano-global/pkg/dispatcher/framework"
	ddconst "volcano.sh/volcano-global/pkg/controller/datadependency"
)

const (
	PluginName = "datadependency"
)

// dataDependencyPlugin implements the Plugin interface for data dependency scheduling
type dataDependencyPlugin struct {
	dataSourceClaims map[string]*v1alpha1.DataSourceClaim
}

// New creates a new dataDependencyPlugin instance
func New() framework.Plugin {
	return &dataDependencyPlugin{
		dataSourceClaims: make(map[string]*v1alpha1.DataSourceClaim),
	}
}

// Name returns the plugin name
func (ddp *dataDependencyPlugin) Name() string {
	return PluginName
}

// OnSessionOpen is called when a new session is opened
func (ddp *dataDependencyPlugin) OnSessionOpen(ssn *framework.Session) {
	// Get DataSourceClaims from snapshot
	ddp.dataSourceClaims = ssn.Snapshot.DataSourceClaims

	ssn.AddAllocatableFn(ddp.Name(), ddp.allocatableFunc)
}

// OnSessionClose is called when the session is closed
func (ddp *dataDependencyPlugin) OnSessionClose(_ *framework.Session) {
}

// allocatableFunc checks if ResourceBinding meets data dependency requirements
// This function determines whether ResourceBinding can be scheduled (unSuspended)
func (ddp *dataDependencyPlugin) allocatableFunc(qi *volcanoapi.QueueInfo, candidate *api.ResourceBindingInfo) bool {
	rb := candidate.ResourceBinding

	klog.V(4).Infof("DataDependency plugin checking ResourceBinding <%s/%s>", rb.Namespace, rb.Name)

	// Check if associated with DataSourceClaim (DSC)
	isAssociatedWithDSC, dsc, err := ddp.checkForAssociatedDSC(rb)
	if err != nil {
		klog.Errorf("Failed to check ResourceBinding <%s/%s> association with DSC: %v", rb.Namespace, rb.Name, err)
		// Conservative handling on error, don't block scheduling
		return true
	}

	if !isAssociatedWithDSC {
		// Not associated with DSC, allow normal scheduling
		klog.V(4).Infof("ResourceBinding <%s/%s> is not associated with any DSC, allowing scheduling", rb.Namespace, rb.Name)
		return true
	}

	// Associated with DSC, need to check if data dependency has been processed
	klog.V(4).Infof("ResourceBinding <%s/%s> is associated with DSC <%s/%s>", rb.Namespace, rb.Name, dsc.Namespace, dsc.Name)

	// Check if placement has been injected (i.e., data dependency processed)
	if ddp.hasPlacementInjectedAnnotation(rb) {
		klog.V(4).Infof("ResourceBinding <%s/%s> has placement injected, allowing scheduling", rb.Namespace, rb.Name)
		return true
	}

	// Data dependency not processed, defer scheduling until placement is injected
	klog.Infof("ResourceBinding <%s/%s> is associated with DSC <%s/%s> but placement not injected yet, deferring scheduling",
		rb.Namespace, rb.Name, dsc.Namespace, dsc.Name)

	// Return false to block current scheduling, wait for data dependency processing to complete
	return false
}

// checkForAssociatedDSC checks if ResourceBinding is associated with DataSourceClaim
func (ddp *dataDependencyPlugin) checkForAssociatedDSC(rb *workv1alpha2.ResourceBinding) (bool, *v1alpha1.DataSourceClaim, error) {
	// Generate key for WorkloadRef (format: namespace/name/kind/apiVersion)
	workloadKey := generateWorkloadKey(rb)

	// Find the DataSourceClaim in our cached data using WorkloadRef as key
	dsc, found := ddp.dataSourceClaims[workloadKey]
	if !found {
		return false, nil, nil
	}

	return true, dsc, nil
}

// generateWorkloadKey generates a unique key for WorkloadRef
// Key format matches cache implementation: "apiVersion/kind/namespace/name"
func generateWorkloadKey(rb *workv1alpha2.ResourceBinding) string {
	// Build WorkloadRef from ResourceBinding's Resource field
	workloadRef := v1alpha1.WorkloadRef{
		APIVersion: rb.Spec.Resource.APIVersion,
		Kind:       rb.Spec.Resource.Kind,
		Name:       rb.Spec.Resource.Name,
		Namespace:  rb.Spec.Resource.Namespace,
	}
	return workloadRef.APIVersion + "/" + workloadRef.Kind + "/" + workloadRef.Namespace + "/" + workloadRef.Name
}

func (ddp *dataDependencyPlugin) hasPlacementInjectedAnnotation(rb *workv1alpha2.ResourceBinding) bool {
	if rb.Annotations == nil {
		return false
	}
    _, exists := rb.Annotations[ddconst.PlacementInjectedAnnotation]
    return exists
}
