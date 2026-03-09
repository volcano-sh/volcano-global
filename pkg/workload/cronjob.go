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

// init is called automatically during package initialization to register the CronJob workload.
func init() {
	Register(cronJobGVK, NewCronJobWorkload)
}

var cronJobGVK = batchv1.SchemeGroupVersion.WithKind("CronJob")

// CronJobWorkload is the implementation of the Workload interface for CronJob resources.
type CronJobWorkload struct {
	resource *batchv1.CronJob
}

// NewCronJobWorkload creates a CronJob workload from an unstructured object.
func NewCronJobWorkload(obj *unstructured.Unstructured) (Workload, error) {
	var cj batchv1.CronJob
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &cj)
	if err != nil {
		klog.ErrorS(err, "Failed to convert to cronjob workload")
		return nil, err
	}
	return &CronJobWorkload{resource: &cj}, nil
}

// QueueName returns the name of the queue the cronjob belongs to.
func (cj *CronJobWorkload) QueueName() string {
	return utils.GetObjQueue(cj.resource.Spec.JobTemplate.Spec.Template.GetObjectMeta())
}

// PriorityClassName returns the priority class name of the cronjob.
func (cj *CronJobWorkload) PriorityClassName() string {
	return cj.resource.Spec.JobTemplate.Spec.Template.Spec.PriorityClassName
}
