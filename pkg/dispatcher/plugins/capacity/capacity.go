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
	"k8s.io/klog/v2"
	"volcano.sh/apis/pkg/apis/scheduling"

	"volcano.sh/volcano-global/pkg/dispatcher/framework"
)

const PluginName = "capacity"

type capacityPlugin struct{}

func New() framework.Plugin {
	return &capacityPlugin{}
}

func (cp *capacityPlugin) Name() string {
	return PluginName
}

func (cp *capacityPlugin) OnSessionOpen(ssn *framework.Session) {
	// Register the Queue order func
	ssn.AddQueueInfoOrderFn(cp.Name(), cp.queueOrderFunc)
}

func (cp *capacityPlugin) OnSessionClose(_ *framework.Session) {}

func (cp *capacityPlugin) queueOrderFunc(l, r interface{}) int {
	lv := l.(*scheduling.Queue)
	rv := r.(*scheduling.Queue)

	klog.V(4).Infof("Capacity plugin QueueOrder: <%s/%s> Queue priority %d, <%s/%s> Queue priority %d",
		lv.Namespace, lv.Name, lv.Spec.Priority, rv.Namespace, rv.Name, rv.Spec.Priority)

	if lv.Spec.Priority == rv.Spec.Priority {
		return 0
	}

	if lv.Spec.Priority > rv.Spec.Priority {
		return -1
	}

	return 1
}
