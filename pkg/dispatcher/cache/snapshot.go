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
	schedulingapi "volcano.sh/volcano/pkg/scheduler/api"

	"volcano.sh/volcano-global/pkg/dispatcher/api"
)

type DispatcherCacheSnapshot struct {
	// The map of the Queue name to Queue info.
	DefaultQueue string
	QueueInfos   map[string]*schedulingapi.QueueInfo

	ResourceBindingInfos map[types.UID]*api.ResourceBindingInfo

	TotalResource *schedulingapi.Resource
}

func (dc *DispatcherCache) Snapshot() *DispatcherCacheSnapshot {
	dc.mutex.Lock()
	defer dc.mutex.Unlock()

	snapshot := &DispatcherCacheSnapshot{
		DefaultQueue:         dc.defaultQueue,
		QueueInfos:           make(map[string]*schedulingapi.QueueInfo, len(dc.queues)),
		ResourceBindingInfos: make(map[types.UID]*api.ResourceBindingInfo),
		TotalResource:        schedulingapi.EmptyResource(),
	}

	for _, queue := range dc.queues {
		snapshot.QueueInfos[queue.Name] = queue.Clone()
	}

	for _, cluster := range dc.clusters {
		snapshot.TotalResource.Add(schedulingapi.NewResource(cluster.Status.ResourceSummary.Allocatable))
	}

	// Collect the ResourceBindingInfos.
	// First, we should update the elements of the ResourceBindingInfo,
	// because we only set some elements of them when creating the ResourceBindingInfo.
	for _, resourceBindingInfoMap := range dc.resourceBindingInfos {
		for _, rbi := range resourceBindingInfoMap {
			// Get the priority of the workload.
			if rbi.PriorityClassName != "" {
				if dc.priorityClasses[rbi.PriorityClassName] == nil {
					// It shouldn't happen. All the PriorityClass should in the cache.
					klog.Errorf("PriorityClass <%s> not found in the cache.", rbi.PriorityClassName)
					continue
				}
				rbi.Priority = dc.priorityClasses[rbi.PriorityClassName].Value
			} else if dc.defaultPriorityClass != nil {
				rbi.Priority = dc.defaultPriorityClass.Value
			}

			// On the end, we need to copy it.
			snapshot.ResourceBindingInfos[rbi.ResourceBinding.UID] = rbi.DeepCopy()
		}
	}

	return snapshot
}
