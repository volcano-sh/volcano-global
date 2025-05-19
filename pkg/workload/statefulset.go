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

package workload

import (
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"

	"volcano.sh/volcano-global/pkg/utils"
)

// init is called automatically during package initialization to register the StatefulSet workload.
func init() {
	Register(statefulSetGVK, NewStatefulSetWorkload)
}

var statefulSetGVK = appsv1.SchemeGroupVersion.WithKind("StatefulSet")

// StatefulSetWorkload is the implementation of the Workload interface for StatefulSet resources.
type StatefulSetWorkload struct {
	resource *appsv1.StatefulSet
}

// NewStatefulSetWorkload creates a StatefulSet workload from an unstructured object.
func NewStatefulSetWorkload(obj *unstructured.Unstructured) (Workload, error) {
	var sts appsv1.StatefulSet
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &sts)
	if err != nil {
		klog.ErrorS(err, "Failed to convert to statefulset workload")
		return nil, err
	}
	return &StatefulSetWorkload{resource: &sts}, nil
}

// QueueName returns the name of the queue the statefulset belongs to.
func (s *StatefulSetWorkload) QueueName() string {
	return utils.GetObjQueue(s.resource.Spec.Template.GetObjectMeta())
}

// PriorityClassName returns the priority class name of the statefulset.
func (s *StatefulSetWorkload) PriorityClassName() string {
	return s.resource.Spec.Template.Spec.PriorityClassName
}
