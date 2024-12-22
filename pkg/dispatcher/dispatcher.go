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
	"flag"
	"fmt"
	"os"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
	"volcano.sh/volcano/pkg/controllers/framework"
	schedulingapi "volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/util"

	"volcano.sh/volcano-global/pkg/dispatcher/api"
	"volcano.sh/volcano-global/pkg/dispatcher/cache"
	dispatcherframework "volcano.sh/volcano-global/pkg/dispatcher/framework"
)

func init() {
	if err := framework.RegisterController(&Dispatcher{}); err != nil {
		panic(fmt.Sprintf("failed to register dispatcher controller: %v", err))
	}
}

const (
	dispatcherName = "dispatcher"

	defaultDispatchPeriod = time.Second
	defaultQueue          = "default"
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
		WorkerNum: opt.WorkerNum,
	}

	{
		// We need to get some additional parameters from the command line.The dispatcher is actually a controller, but it also has functions similar to the volcano-scheduler.
		// However, adding a new component seems to be a heavy-handed approach at the moment. Therefore, while getting the controllerOption,
		// we need to get some additional parameters here, even though this approach isn't very elegant :)
		fs := flag.NewFlagSet(fmt.Sprintf("%s-%s", dispatcherName, "flags"), flag.ContinueOnError)
		fs.StringVar(&cacheOption.KubeClientOptions.Master, "master", cacheOption.KubeClientOptions.Master, "The address of the Kubernetes API server (overrides any value in kubeconfig)")
		fs.StringVar(&cacheOption.KubeClientOptions.KubeConfig, "kubeconfig", cacheOption.KubeClientOptions.KubeConfig, "Path to kubeconfig file with authorization and master location information")
		fs.StringVar(&cacheOption.DefaultQueueName, "default-queue", defaultQueue, "The default queue name of the workload")

		fs.DurationVar(&dispatcher.dispatchPeriod, "dispatch-period", defaultDispatchPeriod, "The period between each scheduling cycle")

		if err := fs.Parse(os.Args[1:]); err != nil {
			klog.Errorf("Parse flags err: %v", err)
		}
	}

	dispatcher.cache = cache.NewDispatcherCache(cacheOption)
	return nil
}

func (dispatcher *Dispatcher) Run(stopCh <-chan struct{}) {
	// Run the dispatcher cache.
	dispatcher.cache.Run(stopCh)

	go wait.Until(dispatcher.runOnce, dispatcher.dispatchPeriod, stopCh)
	klog.V(2).Infof("Dispatcher completes initialization and start to run, period <%v> seconds...", dispatcher.dispatchPeriod.Seconds())
}

func (dispatcher *Dispatcher) runOnce() {
	klog.V(4).Infof("Start dispatching...")
	defer klog.V(4).Infof("End dispatching...")

	ssn := dispatcherframework.OpenSession(dispatcher.cache)
	dispatcher.dispatch(ssn)
	ssn.CloseSession()
}

// Dispatch is the main behavior of the Dispatcher.
// Its primary responsibilities are: sorting the queues and the pending RBs (Resource Bundles) within the queues,
// and then, according to the queue priority, sequentially retrieving all RBs from the queues.
// If each RB meets certain conditions, it will be placed in the queue
// and subsequently updated with their Suspend set to false.
func (dispatcher *Dispatcher) dispatch(ssn *dispatcherframework.Session) {
	klog.V(5).Infof("Dispatcher start running...")
	defer klog.V(5).Infof("Dispatcher end running...")

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
		} else {
			// This queue didn't set in the map, we should check if the queue exists first, then add it to the map.
			if queue, found := ss.QueueInfos[rbiQueueName]; found {
				klog.V(5).Infof("Added Queue <%s> for ResourceBinding <%s/%s>.",
					rbiQueueName, rb.Namespace, rb.Name)
				// Create the priority queue for ResourceBindings, and push it.
				resourceBindingMap[rbiQueueName] = util.NewPriorityQueue(ssn.ResourceBindingInfoOrderFn)
				resourceBindingMap[rbiQueueName].Push(rbi)

				queues.Push(queue)
				enqueueResourceBindingCount++
			} else {
				// We cant find this queue in the cache snapshot, skip it.
				klog.V(3).Infof("Resource %s <%s/%s> Queue <%s> not found, skip dispatching.",
					resource.Kind, resource.Namespace, resource.Name, rbiQueueName)
				continue
			}
		}
	}

	klog.V(5).Infof("Success enqueue <%d> ResourceBindingInfos and <%d> Queues, start dispatching now...",
		enqueueResourceBindingCount, len(resourceBindingMap))

	for {
		// Finish dispatching when all the queues dispatch done.
		if queues.Empty() {
			break
		}

		queue := queues.Pop().(*schedulingapi.QueueInfo)
		resourceBindingsQueue := resourceBindingMap[queue.Name]

		// Get all the ResourceBindingInfos from the priority queue.
		for !resourceBindingsQueue.Empty() {
			rbi := resourceBindingsQueue.Pop().(*api.ResourceBindingInfo)

			rbi.DispatchStatus = api.UnSuspending
			dispatcher.cache.UnSuspendResourceBinding(types.NamespacedName{
				Namespace: rbi.ResourceBinding.Namespace,
				Name:      rbi.ResourceBinding.Name,
			})
			dispatchResourceBindingCount++
		}
	}

	klog.V(2).Infof("Success dispatch <%d> ResourceBindingInfos.", dispatchResourceBindingCount)
}
