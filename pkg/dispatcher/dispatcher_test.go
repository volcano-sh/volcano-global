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

package dispatcher

import (
	"testing"

	workv1alpha2 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling"
	schedulingapi "volcano.sh/volcano/pkg/scheduler/api"

	"volcano.sh/volcano-global/pkg/dispatcher/api"
	"volcano.sh/volcano-global/pkg/dispatcher/options"
	"volcano.sh/volcano-global/pkg/dispatcher/uthelper"
)

func TestDispatcherRoundRobin(t *testing.T) {
	// buildQueue return a scheduling QueueInfo
	buildQueue := func(name string) *schedulingapi.QueueInfo {
		queue := &schedulingv1beta1.Queue{
			ObjectMeta: metav1.ObjectMeta{
				Name:              name,
				CreationTimestamp: metav1.Now(),
			},
		}
		return schedulingapi.NewQueueInfo(queue)
	}

	// buildResourceBinding return a ResourceBindingInfo
	buildResourceBinding := func(name string, namespace string, queueName string) *api.ResourceBindingInfo {
		rb := &workv1alpha2.ResourceBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name:              name,
				Namespace:         namespace,
				CreationTimestamp: metav1.Now(),
			},
			Spec: workv1alpha2.ResourceBindingSpec{
				Resource: workv1alpha2.ObjectReference{
					UID: types.UID(name),
				},
			},
		}
		return &api.ResourceBindingInfo{
			ResourceBinding: rb,
			ResourceUID:     rb.Spec.Resource.UID,
			Queue:           queueName,
			DispatchStatus:  api.Suspended,
		}
	}

	// Define the tests
	tests := []struct {
		name                 string
		queueInfos           []*schedulingapi.QueueInfo
		resourceBindingInfos []*api.ResourceBindingInfo
		expectedOrder        []types.NamespacedName
	}{
		{
			name: "Test Single Queue",
			queueInfos: []*schedulingapi.QueueInfo{
				buildQueue("queue1"),
			},
			resourceBindingInfos: []*api.ResourceBindingInfo{
				buildResourceBinding("rb1", "default", "queue1"),
				buildResourceBinding("rb2", "default", "queue1"),
			},
			expectedOrder: []types.NamespacedName{
				{Namespace: "default", Name: "rb1"},
				{Namespace: "default", Name: "rb2"},
			},
		},
		{
			name: "Test Two Queues",
			queueInfos: []*schedulingapi.QueueInfo{
				buildQueue("queue1"),
				buildQueue("queue2"),
			},
			resourceBindingInfos: []*api.ResourceBindingInfo{
				buildResourceBinding("rb1", "default", "queue1"),
				buildResourceBinding("rb2", "default", "queue1"),
				buildResourceBinding("rb3", "default", "queue1"),
				buildResourceBinding("rb4", "default", "queue2"),
				buildResourceBinding("rb5", "default", "queue2"),
				buildResourceBinding("rb6", "default", "queue2"),
			},
			expectedOrder: []types.NamespacedName{
				{Namespace: "default", Name: "rb1"},
				{Namespace: "default", Name: "rb4"},
				{Namespace: "default", Name: "rb2"},
				{Namespace: "default", Name: "rb5"},
				{Namespace: "default", Name: "rb3"},
				{Namespace: "default", Name: "rb6"},
			},
		},
	}

	// testDispatcherOrder is a helper function to test the order of the dispatched ResourceBindingInfos.
	testDispatcherOrder := func(t *testing.T,
		queueInfos []*schedulingapi.QueueInfo,
		resourceBindingInfos []*api.ResourceBindingInfo,
		expectedOrder []types.NamespacedName,
	) {
		// Create a fake dispatcher cache
		queueInfoMap := make(map[string]*schedulingapi.QueueInfo)
		for _, queueInfo := range queueInfos {
			queueInfoMap[queueInfo.Name] = queueInfo
		}

		resourceBindingInfoMap := make(map[types.UID]*api.ResourceBindingInfo)
		for _, rbi := range resourceBindingInfos {
			resourceBindingInfoMap[rbi.ResourceUID] = rbi
		}
		fakeCache := &uthelper.FakeDispatcherCache{
			DefaultQueue:         options.DefaultQueue,
			Queues:               queueInfoMap,
			ResourceBindingInfos: resourceBindingInfoMap,
			UnSuspendingOrder:    []types.NamespacedName{},
		}

		// Create a dispatcher
		dispatcher := &Dispatcher{
			cache:          fakeCache,
			dispatchPeriod: options.DefaultDispatchPeriod,
		}

		dispatcher.runOnce()

		// Check the order
		if len(fakeCache.UnSuspendingOrder) != len(expectedOrder) {
			t.Errorf("The length of the unsuspend order is not expected, expected: %d, got: %d",
				len(expectedOrder), len(fakeCache.UnSuspendingOrder))
		}

		for i, nn := range expectedOrder {
			if fakeCache.UnSuspendingOrder[i] != nn {
				t.Errorf("The unsuspend order is not expected, expected: %v, got: %v",
					expectedOrder, fakeCache.UnSuspendingOrder)
			}
		}
	}

	// Run the tests
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testDispatcherOrder(t, tt.queueInfos, tt.resourceBindingInfos, tt.expectedOrder)
		})
	}
}

func TestDispatcherCapacity(t *testing.T) {
	// buildQueue return a scheduling QueueInfo
	buildQueue := func(name string, capacity corev1.ResourceList) *schedulingapi.QueueInfo {
		queue := &schedulingv1beta1.Queue{
			ObjectMeta: metav1.ObjectMeta{
				Name:              name,
				CreationTimestamp: metav1.Now(),
			},
			Spec: schedulingv1beta1.QueueSpec{
				Capability: capacity,
			},
		}
		return schedulingapi.NewQueueInfo(queue)
	}

	// buildResourceBinding return a ResourceBindingInfo
	buildResourceBinding := func(name string, namespace string, queueName string, replicas int32, resReq corev1.ResourceList, dispatchStatus api.DispatchStatus) *api.ResourceBindingInfo {
		rb := &workv1alpha2.ResourceBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name:              name,
				Namespace:         namespace,
				CreationTimestamp: metav1.Now(),
			},
			Spec: workv1alpha2.ResourceBindingSpec{
				Resource: workv1alpha2.ObjectReference{
					UID: types.UID(name),
				},
				Replicas: replicas,
				ReplicaRequirements: &workv1alpha2.ReplicaRequirements{
					ResourceRequest: resReq,
				},
			},
		}
		return &api.ResourceBindingInfo{
			ResourceBinding: rb,
			ResourceUID:     rb.Spec.Resource.UID,
			Queue:           queueName,
			DispatchStatus:  dispatchStatus,
		}
	}

	// Define the tests
	tests := []struct {
		name                 string
		queueInfos           []*schedulingapi.QueueInfo
		resourceBindingInfos []*api.ResourceBindingInfo
		// expectedAllocatable are can allocate resourceBindingInfos
		expectedAllocatable []types.NamespacedName
		// expectedAllocatable are can not allocate resourceBindingInfos
		expectedUnAllocatable []types.NamespacedName
	}{
		{
			name: "Test only can allocate ResourceBindingInfo",
			queueInfos: []*schedulingapi.QueueInfo{
				buildQueue("queue1", schedulingapi.BuildResourceList("5", "5G")),
			},
			resourceBindingInfos: []*api.ResourceBindingInfo{
				buildResourceBinding("rb1", "default", "queue1", 2, schedulingapi.BuildResourceList("1", "1G"), api.UnSuspended),
				buildResourceBinding("rb2", "default", "queue1", 2, schedulingapi.BuildResourceList("1", "1G"), api.Suspended),
			},
			expectedAllocatable: []types.NamespacedName{
				{Namespace: "default", Name: "rb2"},
			},
		},
		{
			name: "Test only can not allocate ResourceBindingInfo",
			queueInfos: []*schedulingapi.QueueInfo{
				buildQueue("queue1", schedulingapi.BuildResourceList("5", "5G")),
			},
			resourceBindingInfos: []*api.ResourceBindingInfo{
				buildResourceBinding("rb1", "default", "queue1", 2, schedulingapi.BuildResourceList("1", "1G"), api.UnSuspended),
				buildResourceBinding("rb2", "default", "queue1", 4, schedulingapi.BuildResourceList("1", "1G"), api.Suspended),
			},
			expectedUnAllocatable: []types.NamespacedName{
				{Namespace: "default", Name: "rb2"},
			},
		},
		{
			name: "Test can allocate and can not allocate ResourceBindingInfo",
			queueInfos: []*schedulingapi.QueueInfo{
				buildQueue("queue1", schedulingapi.BuildResourceList("5", "5G")),
			},
			resourceBindingInfos: []*api.ResourceBindingInfo{
				buildResourceBinding("rb1", "default", "queue1", 2, schedulingapi.BuildResourceList("1", "1G"), api.UnSuspended),
				buildResourceBinding("rb2", "default", "queue1", 2, schedulingapi.BuildResourceList("1", "1G"), api.Suspended),
				buildResourceBinding("rb3", "default", "queue1", 1, schedulingapi.BuildResourceList("2", "2G"), api.Suspended),
			},
			expectedAllocatable: []types.NamespacedName{
				{Namespace: "default", Name: "rb2"},
			},
			expectedUnAllocatable: []types.NamespacedName{
				{Namespace: "default", Name: "rb3"},
			},
		},
	}

	// testDispatcherCapacity is a helper function to test the capacity capability of dispatcher.
	testDispatcherCapacity := func(t *testing.T,
		queueInfos []*schedulingapi.QueueInfo,
		resourceBindingInfos []*api.ResourceBindingInfo,
		expectedAllocatable, expectedUnAllocatable []types.NamespacedName,
	) {
		// Create a fake dispatcher cache
		queueInfoMap := make(map[string]*schedulingapi.QueueInfo)
		for _, queueInfo := range queueInfos {
			queueInfoMap[queueInfo.Name] = queueInfo
		}

		resourceBindingInfoMap := make(map[types.UID]*api.ResourceBindingInfo)
		for _, rbi := range resourceBindingInfos {
			resourceBindingInfoMap[rbi.ResourceUID] = rbi
		}
		fakeCache := &uthelper.FakeDispatcherCache{
			DefaultQueue:         options.DefaultQueue,
			Queues:               queueInfoMap,
			ResourceBindingInfos: resourceBindingInfoMap,
			UnSuspendingOrder:    []types.NamespacedName{},
			TotalResource:        schedulingapi.NewResource(schedulingapi.BuildResourceList("100", "100G")),
		}

		// Create a dispatcher
		dispatcher := &Dispatcher{
			cache:          fakeCache,
			dispatchPeriod: options.DefaultDispatchPeriod,
		}

		dispatcher.runOnce()

		// Check the expectedAllocatable
		for _, nn := range expectedAllocatable {
			for _, rbi := range fakeCache.ResourceBindingInfos {
				if nn.Namespace == rbi.ResourceBinding.Namespace && nn.Name == rbi.ResourceBinding.Name && rbi.DispatchStatus != api.UnSuspended {
					t.Errorf("ResourceBindingInfos <%s/%s> is not expected, expect allocatable", rbi.ResourceBinding.Namespace, rbi.ResourceBinding.Name)
				}
			}
		}
		// Check the expectedUnAllocatable
		for _, nn := range expectedUnAllocatable {
			for _, rbi := range fakeCache.ResourceBindingInfos {
				if nn.Namespace == rbi.ResourceBinding.Namespace && nn.Name == rbi.ResourceBinding.Name && rbi.DispatchStatus != api.Suspended {
					t.Errorf("ResourceBindingInfos <%s/%s> is not expected, expect unallocatable", rbi.ResourceBinding.Namespace, rbi.ResourceBinding.Name)
				}
			}
		}
	}

	// Run the tests
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testDispatcherCapacity(t, tt.queueInfos, tt.resourceBindingInfos, tt.expectedAllocatable, tt.expectedUnAllocatable)
		})
	}
}
