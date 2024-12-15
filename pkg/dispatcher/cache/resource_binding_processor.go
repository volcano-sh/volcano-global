/*
Copyright 2024 The Volcano Authors.

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

package cache

import (
	"context"
	"encoding/json"
	"fmt"

	workv1alpha2 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha2"
	"gomodules.xyz/jsonpatch/v2"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"

	"volcano.sh/volcano-global/pkg/dispatcher/api"
)

// Reasons for resourceBinding events.
const (
	// FailedUnSuspendReason is added in an event when failed to unsuspend a ResourceBinding.
	FailedUnSuspendReason = "FailedUnSuspend"
)

func (dc *DispatcherCache) UnSuspendResourceBinding(key types.NamespacedName) {
	dc.mutex.Lock()
	defer dc.mutex.Unlock()
	rbi, ok := dc.resourceBindingInfos[key.Namespace][key.Name]
	if !ok {
		klog.Errorf("ResourceBindingInfo <%s/%s> not found in cache.", key.Namespace, key.Name)
		return
	}
	// Update the ResourceBindingInfo status to UnSuspending.
	rbi.DispatchStatus = api.UnSuspending
	dc.unSuspendRBTaskQueue.Add(key)
	klog.V(3).Infof("Add unsuspend ResourceBinding(%s) task to the unSuspendRBTaskQueue queue.", key)
}

// Its worker for update ResourceBinding.spec.suspend = false.
func (dc *DispatcherCache) unSuspendResourceBindingTaskWorker() {
	for {
		// Wait the queue receive a task, convert to NamespacedName.
		obj, shutdown := dc.unSuspendRBTaskQueue.Get()
		if shutdown {
			return
		}

		dc.mutex.Lock()
		key := obj.(types.NamespacedName)
		rbi, ok := dc.resourceBindingInfos[key.Namespace][key.Name]
		if !ok {
			klog.Errorf("ResourceBindingInfo <%s/%s> not found in cache.", key.Namespace, key.Name)
			dc.mutex.Unlock()
			break
		}
		dc.mutex.Unlock()

		klog.V(5).Infof("Start to patch ResourceBinding <%s/%s>.", key.Namespace, key.Name)
		go dc.unSuspendResourceBinding(rbi.ResourceBinding)
	}
}

// Try to update the suspend field, if failed add to err task queue.
func (dc *DispatcherCache) unSuspendResourceBinding(rb *workv1alpha2.ResourceBinding) {
	key := types.NamespacedName{
		Namespace: rb.Namespace,
		Name:      rb.Name,
	}

	if err := dc.patchUnSuspendResourceBinding(rb); err != nil {
		klog.Errorf("Failed to patch ResourceBinding <%s/%s>, update to Suspended status for next dispath round, err: %v",
			key.Namespace, key.Name, err)

		dc.mutex.Lock()
		rbi, ok := dc.resourceBindingInfos[key.Namespace][key.Name]
		if !ok {
			klog.Errorf("ResourceBindingInfo <%s/%s> not found in cache.", key.Namespace, key.Name)
		} else {
			// Recover the ResourceBindingInfo status to Suspended, wait for the next dispatch.
			dc.recorder.Event(rbi.ResourceBinding, v1.EventTypeWarning, FailedUnSuspendReason,
				fmt.Sprintf("Error unsuspending ResourceBinding: %+v", err))
			rbi.DispatchStatus = api.Suspended
		}
		dc.mutex.Unlock()
	}

	dc.unSuspendRBTaskQueue.Done(key)
}

func (dc *DispatcherCache) patchUnSuspendResourceBinding(rb *workv1alpha2.ResourceBinding) error {
	patchBytes, _ := json.Marshal([]jsonpatch.Operation{
		{Operation: "replace", Path: "/spec/suspend", Value: false},
	})

	// Patch the ResourceBinding.spec.suspend = false.
	_, err := dc.karmadaClient.WorkV1alpha2().ResourceBindings(rb.Namespace).Patch(context.TODO(),
		rb.Name, types.JSONPatchType, patchBytes, metav1.PatchOptions{})

	if err != nil {
		klog.Errorf("Failed to patch/continue ResourceBinding <%s/%s>, err: %v",
			rb.Namespace, rb.Namespace, err)
	} else {
		klog.V(3).Infof("Success patch/continue ResourceBinding <%s/%s>.",
			rb.Namespace, rb.Namespace)
	}
	return err
}
