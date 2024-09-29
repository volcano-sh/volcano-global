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
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	schedulingapi "volcano.sh/volcano/pkg/scheduler/api"

	"volcano.sh/volcano-global/pkg/dispatcher/api"
)

type DispatcherCacheSnapshot struct {
	// The map of the Queue name to Queue info.
	DefaultQueue string
	QueueInfos   map[string]*schedulingapi.QueueInfo

	ResourceBindingInfos map[types.UID]*api.ResourceBindingInfo
}

func (dc *DispatcherCache) Snapshot() *DispatcherCacheSnapshot {
	dc.mutex.Lock()
	defer dc.mutex.Unlock()

	snapshot := &DispatcherCacheSnapshot{
		DefaultQueue:         dc.defaultQueue,
		QueueInfos:           make(map[string]*schedulingapi.QueueInfo, len(dc.queues)),
		ResourceBindingInfos: make(map[types.UID]*api.ResourceBindingInfo),
	}

	for _, queue := range dc.queues {
		snapshot.QueueInfos[queue.Name] = queue.Clone()
	}

	// Collect the PodGroups for ResourceBindingInfo.
	// The map key was the PodGroup source resource's UID (like Deployment, Pod, volcano-job).
	// A PodGroup may have multiple OwnerReference, we need to find the binding ResourceBinding by the map.
	podGroupMap := map[types.UID]*schedulingv1beta1.PodGroup{}
	for _, podGroups := range dc.podGroups {
		for _, podGroup := range podGroups {
			for _, ownerRef := range podGroup.OwnerReferences {
				podGroupMap[ownerRef.UID] = podGroup
			}
		}
	}

	// Collect the ResourceBindingInfos.
	// First, we should update the elements of the ResourceBindingInfo,
	// because we only set some elements of them when creating the ResourceBindingInfo.
	for _, resourceBindingInfoMap := range dc.resourceBindingInfos {
		for _, rbi := range resourceBindingInfoMap {
			// Collect the priority and PodGroup, only the Deployment, Pod and volcano-job will create PodGroup,
			// So the PodGroup field may be nil.
			rbi.Priority = 0
			if dc.defaultPriorityClass != nil {
				rbi.Priority = dc.defaultPriorityClass.Value
			}

			// Try find the binding PodGroup.
			if pg, ok := podGroupMap[rbi.ResourceBinding.Spec.Resource.UID]; ok {
				if pcName := pg.Spec.PriorityClassName; pcName != "" {
					if dc.priorityClasses[pcName] == nil {
						// It shouldn't happen. All the PriorityClass should in the cache.
						klog.Errorf("PriorityClass <%s> not found in the cache when execute PodGroup <%s/%s>.",
							pcName, pg.Namespace, pg.Name)
					} else {
						rbi.Priority = dc.priorityClasses[pcName].Value
					}
				}
				rbi.PodGroup = pg
				rbi.Queue = pg.Spec.Queue
			}

			// On the end, we need to copy it.
			snapshot.ResourceBindingInfos[rbi.ResourceBinding.UID] = rbi.DeepCopy()
		}
	}

	return snapshot
}
