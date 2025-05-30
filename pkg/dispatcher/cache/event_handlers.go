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
	"context"

	clusterv1alpha1 "github.com/karmada-io/karmada/pkg/apis/cluster/v1alpha1"
	workv1alpha2 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha2"
	schedulingv1 "k8s.io/api/scheduling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/klog/v2"
	"volcano.sh/apis/pkg/apis/scheduling"
	"volcano.sh/apis/pkg/apis/scheduling/scheme"
	schedulingapi "volcano.sh/volcano/pkg/scheduler/api"

	"volcano.sh/volcano-global/pkg/dispatcher/api"
	"volcano.sh/volcano-global/pkg/utils"
	"volcano.sh/volcano-global/pkg/workload"
)

func (dc *DispatcherCache) addQueue(obj interface{}) {
	queue := convertToQueue(obj)
	if queue == nil {
		return
	}

	// Convert the queue from v1beta1 to internal
	internalQueue := &scheduling.Queue{}
	if err := scheme.Scheme.Convert(queue, internalQueue, nil); err != nil {
		klog.Errorf("Failed to convert queue from %T to %T", queue, internalQueue)
		return
	}

	dc.mutex.Lock()
	defer dc.mutex.Unlock()

	dc.setQueue(internalQueue)
}

func (dc *DispatcherCache) deleteQueue(obj interface{}) {
	queue := convertToQueue(obj)
	if queue == nil {
		return
	}
	dc.mutex.Lock()
	defer dc.mutex.Unlock()

	delete(dc.queues, queue.Name)
}

func (dc *DispatcherCache) updateQueue(oldObj, newObj interface{}) {
	oldQueue := convertToQueue(oldObj)
	newQueue := convertToQueue(newObj)
	if oldQueue == nil || newQueue == nil {
		return
	}
	if oldQueue.ResourceVersion == newQueue.ResourceVersion {
		return
	}

	// Convert the queue from v1beta1 to internal
	internalQueue := &scheduling.Queue{}
	if err := scheme.Scheme.Convert(newQueue, internalQueue, nil); err != nil {
		klog.Errorf("Failed to convert queue from %T to %T", newQueue, internalQueue)
		return
	}

	dc.mutex.Lock()
	defer dc.mutex.Unlock()

	dc.setQueue(internalQueue)
}

func (dc *DispatcherCache) setQueue(queue *scheduling.Queue) {
	dc.queues[queue.Name] = schedulingapi.NewQueueInfo(queue)
}

func (dc *DispatcherCache) addPriorityClass(obj interface{}) {
	pc := convertToPriorityClass(obj)
	if pc == nil {
		return
	}
	dc.mutex.Lock()
	defer dc.mutex.Unlock()

	dc.setPriorityClass(pc)
}

func (dc *DispatcherCache) deletePriorityClass(obj interface{}) {
	pc := convertToPriorityClass(obj)
	if pc == nil {
		return
	}

	dc.mutex.Lock()
	defer dc.mutex.Unlock()

	if pc.GlobalDefault {
		klog.V(5).Infof("Delete default PriorityClass <%s>, Priority <%d>.", pc.Name, pc.Value)
		dc.defaultPriorityClass = nil
	}
	delete(dc.priorityClasses, pc.Name)
}

func (dc *DispatcherCache) updatePriorityClass(oldObj, newObj interface{}) {
	oldPc := convertToPriorityClass(oldObj)
	newPc := convertToPriorityClass(newObj)
	if oldPc == nil || newPc == nil {
		return
	}
	if oldPc.ResourceVersion == newPc.ResourceVersion {
		return
	}

	dc.mutex.Lock()
	defer dc.mutex.Unlock()

	if oldPc.GlobalDefault {
		klog.V(5).Infof("Delete default PriorityClass <%s>, Priority <%d>.", oldPc.Name, oldPc.Value)
		dc.defaultPriorityClass = nil
	}

	dc.setPriorityClass(newPc)
}

func (dc *DispatcherCache) setPriorityClass(pc *schedulingv1.PriorityClass) {
	if pc.GlobalDefault {
		klog.V(3).Infof("Set default PriorityClass to <%s>, Priority <%d>.", pc.Name, pc.Value)
		dc.defaultPriorityClass = pc
	}
	dc.priorityClasses[pc.Name] = pc
}

func (dc *DispatcherCache) addResourceBinding(obj interface{}) {
	rb := convertToResourceBinding(obj)
	if rb == nil {
		return
	}

	dc.mutex.Lock()
	defer dc.mutex.Unlock()

	dc.setResourceBinding(rb)
}

func (dc *DispatcherCache) deleteResourceBinding(obj interface{}) {
	rb := convertToResourceBinding(obj)
	if rb == nil {
		return
	}

	dc.mutex.Lock()
	defer dc.mutex.Unlock()

	if dc.resourceBindings[rb.Namespace] == nil {
		klog.Errorf("Failed to delete ResourceBinding <%s/%s>, the Resourcebinding's "+
			"Namespace is not in the cache.", rb.Namespace, rb.Name)
		return
	} else {
		delete(dc.resourceBindings[rb.Namespace], rb.Name)
		delete(dc.resourceBindingInfos[rb.Namespace], rb.Name)
	}
}

func (dc *DispatcherCache) updateResourceBinding(oldObj, newObj interface{}) {
	oldRb := convertToResourceBinding(oldObj)
	newRb := convertToResourceBinding(newObj)
	if oldRb == nil || newRb == nil {
		return
	}
	if oldRb.ResourceVersion == newRb.ResourceVersion {
		return
	}

	dc.mutex.Lock()
	defer dc.mutex.Unlock()

	dc.setResourceBinding(newRb)
}

func (dc *DispatcherCache) setResourceBinding(rb *workv1alpha2.ResourceBinding) {
	// Check if its workload, skip add to cache if not.
	isWorkload, newWorkloadFunc, err := workload.TryGetNewWorkloadFunc(rb.Spec.Resource)
	if err != nil {
		klog.Errorf("Failed to check ResourceBinding <%s/%s> if workload, stop add it to cache, err: %v",
			rb.Namespace, rb.Name, err)
		return
	}
	if !isWorkload {
		klog.V(3).Infof("ResourceBinding <%s/%s> is not a workload, skip add it to cache.",
			rb.Namespace, rb.Name)
		return
	}

	resource, err := dc.getResourceFromObjectReference(rb.Spec.Resource)
	if err != nil {
		klog.Errorf("Failed to get resource from object reference <%s/%s>, err: %v",
			rb.Namespace, rb.Name, err)
		return
	}
	workload, err := newWorkloadFunc(resource)
	if err != nil {
		klog.Errorf("Failed to create workload for resource <%s/%s>, err: %v", rb.Namespace, rb.Name, err)
		return
	}

	if dc.resourceBindings[rb.Namespace] == nil {
		dc.resourceBindings[rb.Namespace] = map[string]*workv1alpha2.ResourceBinding{
			rb.Name: rb,
		}
	} else {
		dc.resourceBindings[rb.Namespace][rb.Name] = rb
	}

	// Build the ResourceBindingInfo, the other elements will set when Snapshot.
	newResourceBindingInfo := &api.ResourceBindingInfo{
		ResourceBinding:   rb,
		ResourceUID:       rb.Spec.Resource.UID,
		Queue:             workload.QueueName(),
		PriorityClassName: workload.PriorityClassName(),
		DispatchStatus:    api.UnSuspended,
	}
	// Currently, our failurePolicy is set to Fail, which ensures that no unexpected ResourceBindings will exist.
	// When a ResourceBinding is created, it will definitely be updated to Suspend, so we don't need to check the Status,
	// so rb should be suspended normally.
	if rb.Spec.SchedulingSuspended() {
		newResourceBindingInfo.DispatchStatus = api.Suspended
	}

	if dc.resourceBindingInfos[rb.Namespace] == nil {
		dc.resourceBindingInfos[rb.Namespace] = map[string]*api.ResourceBindingInfo{
			rb.Name: newResourceBindingInfo,
		}
	} else {
		dc.resourceBindingInfos[rb.Namespace][rb.Name] = newResourceBindingInfo
	}
}

func (dc *DispatcherCache) addCluster(obj interface{}) {
	cluster := convertToCluster(obj)
	if cluster == nil {
		return
	}
	if ready, message := utils.CheckClusterReady(cluster); !ready {
		klog.V(5).Info(message)
		return
	}

	dc.mutex.Lock()
	defer dc.mutex.Unlock()

	dc.setCluster(cluster)
}

func (dc *DispatcherCache) deleteCluster(obj interface{}) {
	cluster := convertToCluster(obj)
	if cluster == nil {
		return
	}

	dc.mutex.Lock()
	defer dc.mutex.Unlock()

	delete(dc.clusters, cluster.Name)
}

func (dc *DispatcherCache) updateCluster(oldObj, newObj interface{}) {
	oldCluster := convertToCluster(oldObj)
	newCluster := convertToCluster(newObj)
	if oldCluster == nil || newCluster == nil {
		return
	}
	if oldCluster.ResourceVersion == newCluster.ResourceVersion {
		return
	}

	dc.mutex.Lock()
	defer dc.mutex.Unlock()

	dc.setCluster(newCluster)
}

func (dc *DispatcherCache) setCluster(cluster *clusterv1alpha1.Cluster) {
	dc.clusters[cluster.Name] = cluster
}

func (dc *DispatcherCache) getResourceFromObjectReference(ref workv1alpha2.ObjectReference) (*unstructured.Unstructured, error) {
	gv, err := schema.ParseGroupVersion(ref.APIVersion)
	if err != nil {
		klog.Errorf("Failed to parse GroupVersion <%s>, err: %v", ref.APIVersion, err)
		return nil, err
	}
	mapping, err := dc.restMapper.RESTMapping(schema.GroupKind{
		Group: gv.Group,
		Kind:  ref.Kind,
	})

	if err != nil {
		klog.Errorf("Failed to get resource mapping from reference <%s/%s>, err: %v")
		return nil, err
	}

	// TODO: use informer instead.
	resource, err := dc.dynamicClient.Resource(mapping.Resource).Namespace(ref.Namespace).Get(context.Background(), ref.Name, metav1.GetOptions{})
	if err != nil {
		klog.Errorf("Failed to get resource <%s/%s>, err: %v", ref.Namespace, ref.Name, err)
		return nil, err
	}
	return resource, nil
}
