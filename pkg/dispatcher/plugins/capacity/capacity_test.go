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

package capacity

import (
	"math"
	"testing"

	workv1alpha2 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling"
	volcanoapi "volcano.sh/volcano/pkg/scheduler/api"

	"volcano.sh/volcano-global/pkg/dispatcher/api"
	"volcano.sh/volcano-global/pkg/dispatcher/framework"
	"volcano.sh/volcano-global/pkg/dispatcher/uthelper"
)

func buildQueue(name string, priority int32, capability corev1.ResourceList) *volcanoapi.QueueInfo {
	return volcanoapi.NewQueueInfo(&schedulingv1beta1.Queue{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       schedulingv1beta1.QueueSpec{Priority: priority, Capability: capability},
	})
}

func buildRBI(name, queue, cpu, memory string, status api.DispatchStatus) *api.ResourceBindingInfo {
	return &api.ResourceBindingInfo{
		ResourceBinding: &workv1alpha2.ResourceBinding{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		},
		ResourceUID:    types.UID(name),
		Queue:          queue,
		DispatchStatus: status,
		ResReq:         volcanoapi.NewResource(volcanoapi.BuildResourceList(cpu, memory)),
	}
}

// openSession builds a session over a fake cache and opens the plugin on it.
func openSession(queues []*volcanoapi.QueueInfo, rbis []*api.ResourceBindingInfo, total *volcanoapi.Resource) (*capacityPlugin, *framework.Session) {
	queueMap := map[string]*volcanoapi.QueueInfo{}
	for _, q := range queues {
		queueMap[q.Name] = q
	}
	rbiMap := map[types.UID]*api.ResourceBindingInfo{}
	for _, rbi := range rbis {
		rbiMap[rbi.ResourceUID] = rbi
	}
	ssn := framework.OpenSession(&uthelper.FakeDispatcherCache{
		DefaultQueue:         "default",
		Queues:               queueMap,
		ResourceBindingInfos: rbiMap,
		TotalResource:        total,
	})
	cp := New().(*capacityPlugin)
	cp.OnSessionOpen(ssn)
	return cp, ssn
}

func TestQueueOrderFunc(t *testing.T) {
	cp := New().(*capacityPlugin)
	high := buildQueue("high", 10, nil)
	low := buildQueue("low", 1, nil)

	if got := cp.queueOrderFunc(high, low); got != -1 {
		t.Errorf("higher priority queue should go first, got %d", got)
	}
	if got := cp.queueOrderFunc(low, high); got != 1 {
		t.Errorf("lower priority queue should go last, got %d", got)
	}
	if got := cp.queueOrderFunc(high, high); got != 0 {
		t.Errorf("queues with equal priority should compare equal, got %d", got)
	}
}

func TestBuildQueueAttrs(t *testing.T) {
	total := volcanoapi.NewResource(volcanoapi.BuildResourceList("100", "100G"))

	t.Run("real capability is the smaller of the queue capability and the total resource", func(t *testing.T) {
		q := buildQueue("q", 0, volcanoapi.BuildResourceList("10", "10G"))
		rbi := buildRBI("rb", "q", "1", "1G", api.Suspended)
		cp, _ := openSession([]*volcanoapi.QueueInfo{q}, []*api.ResourceBindingInfo{rbi}, total)

		attr := cp.queueOpts[q.UID]
		if attr == nil {
			t.Fatalf("queue attributes were not built")
		}
		want := volcanoapi.NewResource(volcanoapi.BuildResourceList("10", "10G"))
		if !attr.realCapability.Equal(want, volcanoapi.Zero) {
			t.Errorf("realCapability = %v, want %v", attr.realCapability, want)
		}
	})

	t.Run("a queue without a capability can use the whole cluster", func(t *testing.T) {
		q := buildQueue("q", 0, nil)
		rbi := buildRBI("rb", "q", "1", "1G", api.UnSuspended)
		cp, _ := openSession([]*volcanoapi.QueueInfo{q}, []*api.ResourceBindingInfo{rbi}, total)

		attr := cp.queueOpts[q.UID]
		if attr == nil {
			t.Fatalf("queue attributes were not built")
		}
		if !attr.realCapability.Equal(total, volcanoapi.Zero) {
			t.Errorf("realCapability = %v, want %v", attr.realCapability, total)
		}
	})

	t.Run("only dispatched bindings count as allocated", func(t *testing.T) {
		q := buildQueue("q", 0, nil)
		rbis := []*api.ResourceBindingInfo{
			buildRBI("suspended", "q", "5", "5G", api.Suspended),
			buildRBI("unsuspended", "q", "2", "2G", api.UnSuspended),
			buildRBI("unsuspending", "q", "1", "1G", api.UnSuspending),
		}
		cp, _ := openSession([]*volcanoapi.QueueInfo{q}, rbis, total)

		attr := cp.queueOpts[q.UID]
		if attr == nil {
			t.Fatalf("queue attributes were not built")
		}
		want := volcanoapi.NewResource(volcanoapi.BuildResourceList("3", "3G"))
		if !attr.allocated.Equal(want, volcanoapi.Zero) {
			t.Errorf("allocated = %v, want %v", attr.allocated, want)
		}
	})

	t.Run("a zero cpu or memory capability means unlimited", func(t *testing.T) {
		q := buildQueue("q", 0, corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("1")})
		rbi := buildRBI("rb", "q", "1", "1G", api.Suspended)
		cp, _ := openSession([]*volcanoapi.QueueInfo{q}, []*api.ResourceBindingInfo{rbi}, total)

		attr := cp.queueOpts[q.UID]
		if attr == nil {
			t.Fatalf("queue attributes were not built")
		}
		if attr.capability.MilliCPU != math.MaxFloat64 || attr.capability.Memory != math.MaxFloat64 {
			t.Errorf("capability = %v, want unlimited cpu and memory", attr.capability)
		}
	})

	t.Run("a queue without any binding gets no attributes", func(t *testing.T) {
		q := buildQueue("q", 0, nil)
		cp, _ := openSession([]*volcanoapi.QueueInfo{q}, nil, total)

		if len(cp.queueOpts) != 0 {
			t.Errorf("queueOpts = %v, want it to be empty", cp.queueOpts)
		}
	})

	t.Run("bindings of an unknown queue are ignored", func(t *testing.T) {
		rbi := buildRBI("rb", "missing", "1", "1G", api.UnSuspended)
		cp, _ := openSession(nil, []*api.ResourceBindingInfo{rbi}, total)

		if len(cp.queueOpts) != 0 {
			t.Errorf("queueOpts = %v, want it to be empty", cp.queueOpts)
		}
	})
}

func TestAllocatableFunc(t *testing.T) {
	total := volcanoapi.NewResource(volcanoapi.BuildResourceList("100", "100G"))
	q := buildQueue("q", 0, volcanoapi.BuildResourceList("10", "10G"))
	running := buildRBI("running", "q", "8", "8G", api.UnSuspended)
	cp, _ := openSession([]*volcanoapi.QueueInfo{q}, []*api.ResourceBindingInfo{running}, total)

	small := buildRBI("small", "q", "2", "2G", api.Suspended)
	if !cp.allocatableFunc(q, small) {
		t.Errorf("a request that exactly fills the queue capability should be allocatable")
	}

	large := buildRBI("large", "q", "3", "1G", api.Suspended)
	if cp.allocatableFunc(q, large) {
		t.Errorf("a request that exceeds the queue capability should not be allocatable")
	}
}

func TestAllocateAndDeallocate(t *testing.T) {
	total := volcanoapi.NewResource(volcanoapi.BuildResourceList("100", "100G"))
	q := buildQueue("q", 0, volcanoapi.BuildResourceList("10", "10G"))
	// A suspended binding makes the queue known to the plugin without counting as allocated.
	suspended := buildRBI("suspended", "q", "1", "1G", api.Suspended)
	cp, _ := openSession([]*volcanoapi.QueueInfo{q}, []*api.ResourceBindingInfo{suspended}, total)
	attr := cp.queueOpts[q.UID]
	if attr == nil {
		t.Fatalf("queue attributes were not built")
	}

	rbi := buildRBI("rb", "q", "5", "2G", api.Suspended)

	if err := cp.allocateFunc(rbi); err != nil {
		t.Fatalf("allocateFunc() returned an error: %v", err)
	}
	want := volcanoapi.NewResource(volcanoapi.BuildResourceList("5", "2G"))
	if !attr.allocated.Equal(want, volcanoapi.Zero) {
		t.Errorf("allocated after allocate = %v, want %v", attr.allocated, want)
	}
	// 5 of 10 cpu is the largest share across the dimensions.
	if attr.share != 0.5 {
		t.Errorf("share after allocate = %v, want 0.5", attr.share)
	}

	if err := cp.deallocateFunc(rbi); err != nil {
		t.Fatalf("deallocateFunc() returned an error: %v", err)
	}
	if !attr.allocated.IsEmpty() {
		t.Errorf("allocated after deallocate = %v, want it to be empty", attr.allocated)
	}
	if attr.share != 0 {
		t.Errorf("share after deallocate = %v, want 0", attr.share)
	}
}

func TestAllocateAndDeallocateUnknownQueue(t *testing.T) {
	cp := New().(*capacityPlugin)
	rbi := buildRBI("rb", "missing", "1", "1G", api.Suspended)

	if err := cp.allocateFunc(rbi); err == nil {
		t.Errorf("allocateFunc() should fail for an unknown queue")
	}
	if err := cp.deallocateFunc(rbi); err == nil {
		t.Errorf("deallocateFunc() should fail for an unknown queue")
	}
}
