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

package uthelper

import (
	"sync"

	"k8s.io/apimachinery/pkg/types"
	schedulingapi "volcano.sh/volcano/pkg/scheduler/api"

	"volcano.sh/volcano-global/pkg/dispatcher/api"
	"volcano.sh/volcano-global/pkg/dispatcher/cache"
)

// FakeDispatcherCache is used for testing the dispatcher logic
type FakeDispatcherCache struct {
	mutex sync.Mutex

	DefaultQueue         string
	Queues               map[string]*schedulingapi.QueueInfo
	ResourceBindingInfos map[types.UID]*api.ResourceBindingInfo
	TotalResource        *schedulingapi.Resource

	UnSuspendingOrder []types.NamespacedName
}

var _ cache.DispatcherCacheInterface = &FakeDispatcherCache{}

// Run will not really monitor the cluster
func (f *FakeDispatcherCache) Run(stopCh <-chan struct{}) {
	// do nothing
}

// Snapshot simply returns the pre-filled data
func (f *FakeDispatcherCache) Snapshot() *cache.DispatcherCacheSnapshot {
	return &cache.DispatcherCacheSnapshot{
		DefaultQueue:         f.DefaultQueue,
		QueueInfos:           f.Queues,
		ResourceBindingInfos: f.ResourceBindingInfos,
		TotalResource:        f.TotalResource,
	}
}

// UnSuspendResourceBinding manually fills in the ResourceBindingInfos
func (f *FakeDispatcherCache) UnSuspendResourceBinding(nn types.NamespacedName) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	f.UnSuspendingOrder = append(f.UnSuspendingOrder, nn)
	for _, rbi := range f.ResourceBindingInfos {
		if rbi.ResourceBinding.Namespace == nn.Namespace &&
			rbi.ResourceBinding.Name == nn.Name {
			rbi.DispatchStatus = api.UnSuspended
		}
	}
}
