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

package workload

import (
	"fmt"
	"sync"

	workv1alpha2 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha2"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/klog/v2"
)

type NewWorkloadFunc func(*unstructured.Unstructured) (Workload, error)

var (
	lock           sync.RWMutex
	workloadGVKMap = make(map[schema.GroupVersionKind]NewWorkloadFunc)
)

func TryGetNewWorkloadFunc(ref workv1alpha2.ObjectReference) (bool, NewWorkloadFunc, error) {
	gv, err := schema.ParseGroupVersion(ref.APIVersion)
	if err != nil {
		return false, nil, fmt.Errorf("failed to parse APIVersion, err: %v", err)
	}

	retFunc, exists := workloadGVKMap[schema.GroupVersionKind{
		Group:   gv.Group,
		Version: gv.Version,
		Kind:    ref.Kind,
	}]
	if !exists {
		return false, nil, nil
	}
	return true, retFunc, nil
}

// Register allows different workloads register themselves to workloadGVKMap,
// this is used when get queue and priorityClass of a specific workload,
// and only resourceBinding referenced to registered workload supports suspension.
func Register(gvk schema.GroupVersionKind, fn NewWorkloadFunc) {
	lock.Lock()
	defer lock.Unlock()

	if _, exists := workloadGVKMap[gvk]; exists {
		klog.ErrorS(nil, "Workload already registered", "gvk", gvk)
		return
	}
	workloadGVKMap[gvk] = fn
	klog.InfoS("Successfully registered workload", "gvk", gvk)
}
