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

package priority

import (
	"k8s.io/klog/v2"

	"volcano.sh/volcano-global/pkg/dispatcher/api"
	"volcano.sh/volcano-global/pkg/dispatcher/framework"
)

const PluginName = "priority"

type priorityPlugin struct{}

func New() framework.Plugin {
	return &priorityPlugin{}
}

func (pp *priorityPlugin) Name() string {
	return PluginName
}

func (pp *priorityPlugin) OnSessionOpen(ssn *framework.Session) {
	// Register the ResourceBinding order func
	ssn.AddResourceBindingInfoOrderFn(pp.Name(), pp.resourceBindingInfoOrderFunc)
}

func (pp *priorityPlugin) OnSessionClose(_ *framework.Session) {}

func (pp *priorityPlugin) resourceBindingInfoOrderFunc(l, r interface{}) int {
	lv := l.(*api.ResourceBindingInfo)
	rv := r.(*api.ResourceBindingInfo)

	klog.V(4).Infof("Priority plugin ResourceBindingOrder: <%s/%s> ResourceBinding priority %d, <%s/%s> ResourceBInding priority %d",
		lv.ResourceBinding.Namespace, lv.ResourceBinding.Name, lv.Priority,
		rv.ResourceBinding.Namespace, rv.ResourceBinding.Name, rv.Priority)

	if lv.Priority == rv.Priority {
		return 0
	}

	if lv.Priority > rv.Priority {
		return -1
	}

	return 1
}
