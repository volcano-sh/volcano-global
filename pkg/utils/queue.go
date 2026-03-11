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
)

const (
	QueueNameKey = "volcano.sh/queue-name"
	QueueNameAnnotationKey = "scheduling.volcano.sh/queue-name"

	defaultQueue = "default"
)
// GetObjQueue returns the queue name of an obj.
// There are 3 ways to get queue name for now:
// scheduling.volcano.sh/queue-name support only annotation
// volcano.sh/queue-name support both labels & annotation
// the key should be unified, maybe volcano.sh/queue-name is better
func GetObjQueue(obj metav1.Object) string {
	labels := obj.GetLabels()
	if q, ok := labels[QueueNameKey]; ok {
		return q
	}

	annotations := obj.GetAnnotations()
	if q, ok := annotations[QueueNameKey]; ok {
		return q
	}

	if q, ok := annotations[QueueNameAnnotationKey]; ok {
		return q
	}

	return defaultQueue
}

func SetObjQueue(obj metav1.Object, queue string) {
	labels := obj.GetLabels()
	if labels == nil {
		labels = make(map[string]string)
	}
	labels[QueueNameKey] = queue
	obj.SetLabels(labels)
}
