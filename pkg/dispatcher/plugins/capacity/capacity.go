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

package capacity

import (
	"fmt"
	"math"

	"k8s.io/klog/v2"
	volcanoapi "volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/api/helpers"

	"volcano.sh/volcano-global/pkg/dispatcher/api"
	"volcano.sh/volcano-global/pkg/dispatcher/framework"
)

const PluginName = "capacity"

type capacityPlugin struct {
	queueOpts map[volcanoapi.QueueID]*queueAttr
}
type queueAttr struct {
	queueID    volcanoapi.QueueID
	name       string
	share      float64
	capability *volcanoapi.Resource
	// realCapacity represents the resource limit of the queue.
	realCapability *volcanoapi.Resource
	// allocated represents the resource request of all allocated jobs in the queue.
	allocated *volcanoapi.Resource
}

func New() framework.Plugin {
	return &capacityPlugin{
		queueOpts: map[volcanoapi.QueueID]*queueAttr{},
	}
}

func (cp *capacityPlugin) Name() string {
	return PluginName
}

func (cp *capacityPlugin) OnSessionOpen(ssn *framework.Session) {
	cp.buildQueueAttrs(ssn)

	// Register the Queue order func
	ssn.AddQueueInfoOrderFn(cp.Name(), cp.queueOrderFunc)
	// Register the Allocatable func
	ssn.AddAllocatableFn(cp.Name(), cp.allocatableFunc)
	// Register the Event handler func
	ssn.AddEventHandler(&framework.EventHandler{
		AllocateFunc:   cp.allocateFunc,
		DeallocateFunc: cp.deallocateFunc,
	})
}

func (cp *capacityPlugin) OnSessionClose(_ *framework.Session) {
	cp.queueOpts = nil
}

func (cp *capacityPlugin) queueOrderFunc(l, r interface{}) int {
	lv := l.(*volcanoapi.QueueInfo)
	rv := r.(*volcanoapi.QueueInfo)

	klog.V(4).Infof("Capacity plugin QueueOrder: <%s/%s> Queue priority %d, <%s/%s> Queue priority %d",
		lv.Queue.Namespace, lv.Queue.Name, lv.Queue.Spec.Priority, rv.Queue.Namespace, rv.Name, rv.Queue.Spec.Priority)

	if lv.Queue.Spec.Priority == rv.Queue.Spec.Priority {
		return 0
	}

	if lv.Queue.Spec.Priority > rv.Queue.Spec.Priority {
		return -1
	}

	return 1
}

func (cp *capacityPlugin) buildQueueAttrs(ssn *framework.Session) {
	for _, rbi := range ssn.Snapshot.ResourceBindingInfos {
		queue, found := ssn.Snapshot.QueueInfos[ssn.GetResourceBindingInfoQueue(rbi)]
		if !found {
			klog.V(4).Infof("ResourceBindingInfo <%s/%s> used Queue <%s> not found in cache, ignore it.", rbi.ResourceBinding.Namespace, rbi.ResourceBinding.Name, rbi.Queue)
			continue
		}
		var attr *queueAttr
		if attr, found = cp.queueOpts[queue.UID]; !found {
			attr = &queueAttr{
				queueID:   queue.UID,
				name:      queue.Name,
				allocated: volcanoapi.EmptyResource(),
			}
			if len(queue.Queue.Spec.Capability) != 0 {
				attr.capability = volcanoapi.NewResource(queue.Queue.Spec.Capability)
				if attr.capability.MilliCPU <= 0 {
					attr.capability.MilliCPU = math.MaxFloat64
				}
				if attr.capability.Memory <= 0 {
					attr.capability.Memory = math.MaxFloat64
				}
			}
			realCapability := ssn.Snapshot.TotalResource.Clone()
			if attr.capability == nil {
				attr.realCapability = realCapability
			} else {
				realCapability.MinDimensionResource(attr.capability, volcanoapi.Infinity)
				attr.realCapability = realCapability
			}
			cp.queueOpts[queue.UID] = attr
		}
		if rbi.DispatchStatus != api.Suspended {
			resRes := volcanoapi.NewResource(rbi.ResourceBinding.Spec.ReplicaRequirements.ResourceRequest).Multi(float64(rbi.ResourceBinding.Spec.Replicas))
			attr.allocated = attr.allocated.Add(resRes)
		}
	}
	for _, attr := range cp.queueOpts {
		cp.updateShare(attr)
		klog.V(4).Infof("The attributes of queue <%s> in capacity: realCapability <%v>, allocated <%v>, share <%0.2f>.",
			attr.name, attr.realCapability, attr.allocated, attr.share)
	}
}

func (cp *capacityPlugin) allocatableFunc(qi *volcanoapi.QueueInfo, candidate *api.ResourceBindingInfo) bool {
	attr := cp.queueOpts[qi.UID]
	resReq := volcanoapi.NewResource(candidate.ResourceBinding.Spec.ReplicaRequirements.ResourceRequest).Multi(float64(candidate.ResourceBinding.Spec.Replicas))
	futureUsed := attr.allocated.Clone().Add(resReq)
	allocatable := futureUsed.LessEqualWithDimension(attr.realCapability, resReq)
	if !allocatable {
		klog.V(3).Infof("Queue <%v>: realCapability <%v>, allocated <%v>; Candidate <%v/%v>: resource request <%v>",
			qi.Name, attr.realCapability, attr.allocated, candidate.ResourceBinding.Namespace, candidate.ResourceBinding.Name, resReq)
	}
	return allocatable
}

func (cp *capacityPlugin) allocateFunc(rbi *api.ResourceBindingInfo) error {
	attr, ok := cp.queueOpts[volcanoapi.QueueID(rbi.Queue)]
	if !ok {
		err := fmt.Errorf("queue <%v> not found in queueOpts", rbi.Queue)
		klog.Error(err)
		return err
	}
	resReq := volcanoapi.NewResource(rbi.ResourceBinding.Spec.ReplicaRequirements.ResourceRequest).Multi(float64(rbi.ResourceBinding.Spec.Replicas))
	attr.allocated.Add(resReq)
	cp.updateShare(attr)
	klog.V(4).Infof("Capacity allocateFunc: ResourceBindingInfo <%v/%v>, resreq <%v>,  share <%v>",
		rbi.ResourceBinding.Namespace, rbi.ResourceBinding.Name, resReq, attr.share)
	return nil
}

func (cp *capacityPlugin) deallocateFunc(rbi *api.ResourceBindingInfo) error {
	attr, ok := cp.queueOpts[volcanoapi.QueueID(rbi.Queue)]
	if !ok {
		err := fmt.Errorf("queue <%v> not found in queueOpts", rbi.Queue)
		klog.Error(err)
		return err
	}
	resReq := volcanoapi.NewResource(rbi.ResourceBinding.Spec.ReplicaRequirements.ResourceRequest).Multi(float64(rbi.ResourceBinding.Spec.Replicas))
	attr.allocated.Sub(resReq)
	cp.updateShare(attr)
	klog.V(4).Infof("Capacity deallocateFunc: ResourceBindingInfo <%v/%v>, resreq <%v>,  share <%v>",
		rbi.ResourceBinding.Namespace, rbi.ResourceBinding.Name, resReq, attr.share)
	return nil
}

func (cp *capacityPlugin) updateShare(attr *queueAttr) {
	res := float64(0)

	for _, rn := range attr.realCapability.ResourceNames() {
		res = max(res, helpers.Share(attr.allocated.Get(rn), attr.realCapability.Get(rn)))
	}

	attr.share = res
}
