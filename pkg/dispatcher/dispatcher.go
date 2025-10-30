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

package dispatcher

import (
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"volcano.sh/volcano/pkg/controllers/framework"
	schedulingapi "volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/util"

	"volcano.sh/volcano-global/pkg/dispatcher/api"
	"volcano.sh/volcano-global/pkg/dispatcher/cache"
	dispatcherframework "volcano.sh/volcano-global/pkg/dispatcher/framework"
	"volcano.sh/volcano-global/pkg/dispatcher/options"
	"volcano.sh/volcano-global/pkg/dispatcher/plugins"
	"volcano.sh/volcano-global/pkg/utils/feature"
)

func init() {
	if err := framework.RegisterController(&Dispatcher{}); err != nil {
		panic(fmt.Sprintf("failed to register dispatcher controller: %v", err))
	}
}

const (
	dispatcherName = "dispatcher"
)

type Dispatcher struct {
	cache          cache.DispatcherCacheInterface
	dispatchPeriod time.Duration
}

func (dispatcher *Dispatcher) Name() string {
	return dispatcherName
}

func (dispatcher *Dispatcher) Initialize(opt *framework.ControllerOption) error {
	cacheOption := &cache.DispatcherCacheOption{
		WorkerNum:        opt.WorkerNum,
		DefaultQueueName: options.Opt.DefaultQueueName,
		RestConfig:       opt.Config,
	}

	// Register data dependency plugin if the feature is enabled
	if utilfeature.DefaultFeatureGate.Enabled(feature.DataDependencyAwareness) {
		plugins.RegisterDataDependencyPlugin()
	}

	dispatcher.dispatchPeriod = options.Opt.DispatchPeriod
	cache, err := cache.NewDispatcherCache(cacheOption)
	if err != nil {
		return fmt.Errorf("failed to create dispatcher cache: %w", err)
	}
	dispatcher.cache = cache
	return nil
}

func (dispatcher *Dispatcher) Run(stopCh <-chan struct{}) {
	// Run the dispatcher cache.
	dispatcher.cache.Run(stopCh)

	go wait.Until(dispatcher.runOnce, dispatcher.dispatchPeriod, stopCh)
	klog.V(2).Infof("Dispatcher initialization complete. Starting to run with a period of %v seconds...",
		dispatcher.dispatchPeriod.Seconds())
}

func (dispatcher *Dispatcher) runOnce() {
	klog.V(4).Infof("Start dispatching...")
	defer klog.V(4).Infof("End dispatching...")

	ssn := dispatcherframework.OpenSession(dispatcher.cache)
	defer ssn.CloseSession()

	dispatcher.dispatch(ssn)
}

// Dispatch is the main behavior of the Dispatcher.
// Its primary responsibilities are: sorting the queues and the pending RBs (Resource Bundles) within the queues,
// and then, according to the queue priority, sequentially retrieving all RBs from the queues.
// If each RB meets certain conditions, it will be placed in the queue
// and subsequently updated with their Suspend set to false.
func (dispatcher *Dispatcher) dispatch(ssn *dispatcherframework.Session) {
	klog.V(5).Infof("Dispatcher started running...")
	defer klog.V(5).Infof("Dispatcher finished running...")

	ss := ssn.Snapshot
	queues := util.NewPriorityQueue(ssn.QueueInfoOrderFn)
	resourceBindingMap := map[string]*util.PriorityQueue{}
	// The counts for logs.
	enqueueResourceBindingCount := 0
	dispatchResourceBindingCount := 0

	// Collect the workloads to the queue map.
	for _, rbi := range ss.ResourceBindingInfos {
		rb := rbi.ResourceBinding

		// Check if its Suspended, dispatcher cares the suspended rbi only.
		if rbi.DispatchStatus != api.Suspended {
			continue
		}

		// Get the workload's queue name, it may be nil.
		rbiQueueName := ssn.GetResourceBindingInfoQueue(rbi)
		resource := rb.Spec.Resource

		// Check if the queue set in the map.
		if rbiPriorityQueue, found := resourceBindingMap[rbiQueueName]; found {
			// Add this workload to the queue.
			rbiPriorityQueue.Push(rbi)
			enqueueResourceBindingCount++
		} else {
			// This queue didn't set in the map, we should check if the queue exists first, then add it to the map.
			if queue, found := ss.QueueInfos[rbiQueueName]; found {
				klog.V(5).Infof("Added ResourceBinding <%s/%s> into Queue <%s>.",
					rb.Namespace, rb.Name, rbiQueueName)
				// Create the priority queue for ResourceBindings, and push it.
				resourceBindingMap[rbiQueueName] = util.NewPriorityQueue(ssn.ResourceBindingInfoOrderFn)
				resourceBindingMap[rbiQueueName].Push(rbi)

				queues.Push(queue)
				enqueueResourceBindingCount++
			} else {
				// We can't find this queue in the cache snapshot, skipping it.
				klog.V(3).Infof("Queue <%s> for Resource %s <%s/%s> not found in cache snapshot, skipping dispatch.",
					rbiQueueName, resource.Kind, resource.Namespace, resource.Name)
				continue
			}
		}
	}

	klog.V(5).Infof("Successfully enqueued %d ResourceBindingInfos and %d Queues. Starting dispatch now...",
		enqueueResourceBindingCount, len(resourceBindingMap))

	// Sort the queues by priority
	roundedQueues := util.NewPriorityQueue(ssn.QueueInfoOrderFn)

	// Round-robin dispatch the ResourceBindingInfos in the queues.
	// TODO: We will add capacity-based dispatching in the next major release.
	for {
		if queues.Empty() {

			if roundedQueues.Empty() {
				// Finish dispatching when all the queues dispatch done.
				break
			} else {
				// Swap queues and roundedQueues
				queues, roundedQueues = roundedQueues, queues
			}
		}

		queue := queues.Pop().(*schedulingapi.QueueInfo)

		resourceBindings, found := resourceBindingMap[queue.Name]
		if !found || resourceBindings.Empty() {
			// This queue has no ResourceBinding, skip it.
			continue
		}

		// Get the highest priority ResourceBinding from the queue.
		rbi := resourceBindings.Pop().(*api.ResourceBindingInfo)
		if ssn.Allocatable(queue, rbi) {
			if err := ssn.Allocate(rbi); err != nil {
				klog.Errorf("Failed to allocate ResourceBindingInfo <%s/%s>, err: %v",
					rbi.ResourceBinding.Namespace, rbi.ResourceBinding.Name, err)
				if err = ssn.UnAllocate(rbi); err != nil {
					klog.Errorf("Failed to unallocate ResourceBindingInfo <%s/%s>, err: %v",
						rbi.ResourceBinding.Namespace, rbi.ResourceBinding.Name, err)
				}
			} else {
				rbi.DispatchStatus = api.UnSuspending
				dispatcher.cache.UnSuspendResourceBinding(types.NamespacedName{
					Namespace: rbi.ResourceBinding.Namespace,
					Name:      rbi.ResourceBinding.Name,
				})
				dispatchResourceBindingCount++
			}
		} else {
			klog.V(3).Infof("Queue <%s> is overused when considering ResourceBindingInfo <%s/%s>, ignore it.", queue.Name, rbi.ResourceBinding.Namespace, rbi.ResourceBinding.Name)
		}

		// Add the queue to roundedQueues if it still has ResourceBinding.
		if !resourceBindings.Empty() {
			roundedQueues.Push(queue)
		}
	}

	klog.V(2).Infof("Successfully dispatched %d ResourceBindingInfos.", dispatchResourceBindingCount)
}
