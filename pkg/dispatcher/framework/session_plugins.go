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

package framework

import (
	volcanoapi "volcano.sh/volcano/pkg/scheduler/api"

	"volcano.sh/volcano-global/pkg/dispatcher/api"
)

// AddResourceBindingInfoOrderFn add workload order function to the session.
func (ssn *Session) AddResourceBindingInfoOrderFn(name string, compareFn volcanoapi.CompareFn) {
	ssn.resourceBindingInfoOrderFns[name] = compareFn
}

// AddQueueInfoOrderFn add queue order function to the session.
func (ssn *Session) AddQueueInfoOrderFn(name string, compareFn volcanoapi.CompareFn) {
	ssn.queueInfoOrderFns[name] = compareFn
}

func (ssn *Session) QueueInfoOrderFn(l, r interface{}) bool {
	for _, orderFn := range ssn.queueInfoOrderFns {
		if result := orderFn(l, r); result != 0 {
			return result < 0
		}
	}

	// If there is no Queue order func, order by CreationTimestamp first, then by UID.
	lv := l.(*volcanoapi.QueueInfo)
	rv := r.(*volcanoapi.QueueInfo)

	if lv.Queue.CreationTimestamp.Equal(&rv.Queue.CreationTimestamp) {
		return lv.UID < rv.UID
	}

	return lv.Queue.CreationTimestamp.Before(&rv.Queue.CreationTimestamp)
}

func (ssn *Session) ResourceBindingInfoOrderFn(l, r interface{}) bool {
	for _, orderFn := range ssn.resourceBindingInfoOrderFns {
		if result := orderFn(l, r); result != 0 {
			return result < 0
		}
	}

	// If there is no ResourceBindingInfo order func, order by CreationTimestamp first, then by UID.
	lv := l.(*api.ResourceBindingInfo)
	rv := r.(*api.ResourceBindingInfo)

	if lv.ResourceBinding.CreationTimestamp.Equal(&rv.ResourceBinding.CreationTimestamp) {
		return lv.ResourceBinding.UID < rv.ResourceBinding.UID
	}

	return lv.ResourceBinding.CreationTimestamp.Before(&rv.ResourceBinding.CreationTimestamp)
}
