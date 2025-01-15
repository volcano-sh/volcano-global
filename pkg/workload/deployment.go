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

// init is called automatically during package initialization to register the Deployment workload.
func init() {
	Register(deploymentGVK, NewDeploymentWorkload)
}

var deploymentGVK = appsv1.SchemeGroupVersion.WithKind("Deployment")

// DeploymentWorkload is the implementation of the Workload interface for Deployment resources.
type DeploymentWorkload struct {
	resource *appsv1.Deployment
}

// NewDeploymentWorkload creates a Deployment workload from an unstructured object.
func NewDeploymentWorkload(obj *unstructured.Unstructured) (Workload, error) {
	var deployment appsv1.Deployment
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &deployment)
	if err != nil {
		klog.ErrorS(err, "Failed to convert to deployment workload")
		return nil, err
	}
	return &DeploymentWorkload{resource: &deployment}, nil
}

// QueueName returns the name of the queue the deployment belongs to.
func (d DeploymentWorkload) QueueName() string {
	return utils.GetObjQueue(d.resource.Spec.Template.GetObjectMeta())
}

// PriorityClassName returns the priority class name of the deployment.
func (d DeploymentWorkload) PriorityClassName() string {
	return d.resource.Spec.Template.Spec.PriorityClassName
}
