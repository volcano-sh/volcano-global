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

package api

import (
	corev1 "k8s.io/api/core/v1"
	volcanoapi "volcano.sh/volcano/pkg/scheduler/api"
)

const (
	// LastDispatchedContentAnnotationKey is last dispatched content key in annotations of ResourceBinding, value is DispatchContent.
	LastDispatchedContentAnnotationKey = "volcano-global.volcano.sh/last-dispatched-content"
)

type AllocatableFn func(qi *volcanoapi.QueueInfo, rbi *ResourceBindingInfo) bool

// DispatchContent includes Replicas and ResourceRequest of the ResourceBinding, it needs to be set in ResourceBinding annotation.
type DispatchContent struct {
	Replicas        int32               `json:"replicas"`
	ResourceRequest corev1.ResourceList `json:"resourceRequest"`
}
