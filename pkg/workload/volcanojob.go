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
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	batchv1alpha1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"
)

// init is called automatically during package initialization to register the VolcanoJob workload.
func init() {
	Register(volcanoJobGVK, NewVolcanoJobWorkload)
}

var volcanoJobGVK = batchv1alpha1.SchemeGroupVersion.WithKind("Job")

// VolcanoJobWorkload is the implementation of the Workload interface for VolcanoJob resources.
type VolcanoJobWorkload struct {
	resource *batchv1alpha1.Job
}

var _ Workload = &VolcanoJobWorkload{}

// NewVolcanoJobWorkload creates a VolcanoJob workload from an unstructured object.
func NewVolcanoJobWorkload(obj *unstructured.Unstructured) (Workload, error) {
	var vcJob batchv1alpha1.Job
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &vcJob)
	if err != nil {
		klog.ErrorS(err, "Failed to convert to volcano job workload")
		return nil, err
	}
	return &VolcanoJobWorkload{resource: &vcJob}, nil
}

// QueueName returns the name of the queue the volcano job belongs to.
func (vj *VolcanoJobWorkload) QueueName() string {
	return vj.resource.Spec.Queue
}

// PriorityClassName returns the priority class name of the volcano job.
func (vj *VolcanoJobWorkload) PriorityClassName() string {
	return vj.resource.Spec.PriorityClassName
}
