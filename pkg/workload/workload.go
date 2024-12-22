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

	workv1alpha2 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha2"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	batchv1alpha1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"
)

type NewWorkloadFunc func(*unstructured.Unstructured) (Workload, error)

// This map contains the workload that we can handle now, the controllers will create PodGroup for them.
var workloadGVKMap = map[schema.GroupVersionKind]NewWorkloadFunc{
	{
		Group:   batchv1alpha1.SchemeGroupVersion.Group,
		Version: batchv1alpha1.SchemeGroupVersion.Version,
		Kind:    "Job",
	}: NewVolcanoJobWorkload,
}

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
