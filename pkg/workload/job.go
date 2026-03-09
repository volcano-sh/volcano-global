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
	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"

	"volcano.sh/volcano-global/pkg/utils"
)

// init is called automatically during package initialization to register the Job workload.
func init() {
	Register(jobGVK, NewJobWorkload)
}

var jobGVK = batchv1.SchemeGroupVersion.WithKind("Job")

// JobWorkload is the implementation of the Workload interface for Job resources.
type JobWorkload struct {
	resource *batchv1.Job
}

// NewJobWorkload creates a Job workload from an unstructured object.
func NewJobWorkload(obj *unstructured.Unstructured) (Workload, error) {
	var job batchv1.Job
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &job)
	if err != nil {
		klog.ErrorS(err, "Failed to convert to job workload")
		return nil, err
	}
	return &JobWorkload{resource: &job}, nil
}

// QueueName returns the name of the queue the job belongs to.
func (j *JobWorkload) QueueName() string {
	return utils.GetObjQueue(j.resource.Spec.Template.GetObjectMeta())
}

// PriorityClassName returns the priority class name of the job.
func (j *JobWorkload) PriorityClassName() string {
	return j.resource.Spec.Template.Spec.PriorityClassName
}
