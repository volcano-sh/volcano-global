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

package cache

import (
	workv1alpha2 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha2"
	schedulingv1 "k8s.io/api/scheduling/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
)

func convertToQueue(obj interface{}) *schedulingv1beta1.Queue {
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
	}

	queue, ok := obj.(*schedulingv1beta1.Queue)
	if !ok {
		klog.Errorf("Cant Convert obj to *schedulingv1beta1.Queue, obj: %v", obj)
		return nil
	}
	return queue
}

func convertToPriorityClass(obj interface{}) *schedulingv1.PriorityClass {
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
	}

	priorityClass, ok := obj.(*schedulingv1.PriorityClass)
	if !ok {
		klog.Errorf("Cant Convert obj to *schedulingv1.PriorityClass, obj: %v", obj)
		return nil
	}
	return priorityClass
}

func convertToResourceBinding(obj interface{}) *workv1alpha2.ResourceBinding {
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
	}

	resourceBinding, ok := obj.(*workv1alpha2.ResourceBinding)
	if !ok {
		klog.Errorf("Cant Convert obj to *workv1alpha2.ResourceBinding, obj: %v", obj)
		return nil
	}
	return resourceBinding
}
