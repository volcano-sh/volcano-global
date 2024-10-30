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

package utils

import (
	"fmt"

	workv1alpha2 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha2"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	batchv1alpha1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"
)

// todo: we can do like kueue/pkg/controller/jobframework/interface.go, the workloads implement the interface so that we didnt need to know what kind of the resource.

// This map contains the workload that we can handle now, the controllers will create PodGroup for them.
var workloadGVKMap = map[schema.GroupVersionKind]struct{}{
	{
		Group:   batchv1alpha1.SchemeGroupVersion.Group,
		Version: batchv1alpha1.SchemeGroupVersion.Version,
		Kind:    "Job",
	}: {},
	{
		Group:   appsv1.SchemeGroupVersion.Group,
		Version: appsv1.SchemeGroupVersion.Version,
		Kind:    "Deployment",
	}: {},
	{
		Group:   corev1.SchemeGroupVersion.Group,
		Version: corev1.SchemeGroupVersion.Version,
		Kind:    "Pod",
	}: {},
}

// IsWorkload Return if the object reference is a workload.
func IsWorkload(ref workv1alpha2.ObjectReference) (bool, error) {
	gv, err := schema.ParseGroupVersion(ref.APIVersion)
	if err != nil {
		return false, fmt.Errorf("failed to parse APIVersion, err: %v", err)
	}

	_, exists := workloadGVKMap[schema.GroupVersionKind{
		Group:   gv.Group,
		Version: gv.Version,
		Kind:    ref.Kind,
	}]
	return exists, nil
}
