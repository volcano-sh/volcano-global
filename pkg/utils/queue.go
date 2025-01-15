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

package utils

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	batchv1alpha1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
)

// GetObjQueue returns the queue name of an obj.
// There are 3 ways to get queue name for now:
// scheduling.volcano.sh/queue-name support only annotation
// volcano.sh/queue-name support both labels & annotation
// the key should be unified, maybe volcano.sh/queue-name is better
func GetObjQueue(obj metav1.Object) string {
	labels := obj.GetLabels()
	if _, ok := labels[batchv1alpha1.QueueNameKey]; ok {
		return labels[batchv1alpha1.QueueNameKey]
	}
	annotations := obj.GetAnnotations()
	if _, ok := annotations[batchv1alpha1.QueueNameKey]; ok {
		return annotations[batchv1alpha1.QueueNameKey]
	}
	if _, ok := annotations[schedulingv1beta1.QueueNameAnnotationKey]; ok {
		return annotations[schedulingv1beta1.QueueNameAnnotationKey]
	}
	return ""
}
