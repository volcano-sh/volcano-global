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

// init is called automatically during package initialization to register the DaemonSet workload.
func init() {
	Register(daemonSetGVK, NewDaemonSetWorkload)
}

var daemonSetGVK = appsv1.SchemeGroupVersion.WithKind("DaemonSet")

// DaemonSetWorkload is the implementation of the Workload interface for DaemonSet resources.
type DaemonSetWorkload struct {
	resource *appsv1.DaemonSet
}

// NewDaemonSetWorkload creates a DaemonSet workload from an unstructured object.
func NewDaemonSetWorkload(obj *unstructured.Unstructured) (Workload, error) {
	var ds appsv1.DaemonSet
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &ds)
	if err != nil {
		klog.ErrorS(err, "Failed to convert to daemonset workload")
		return nil, err
	}
	return &DaemonSetWorkload{resource: &ds}, nil
}

// QueueName returns the name of the queue the daemonset belongs to.
func (d DaemonSetWorkload) QueueName() string {
	return utils.GetObjQueue(d.resource.Spec.Template.GetObjectMeta())
}

// PriorityClassName returns the priority class name of the daemonset.
func (d DaemonSetWorkload) PriorityClassName() string {
	return d.resource.Spec.Template.Spec.PriorityClassName
}
