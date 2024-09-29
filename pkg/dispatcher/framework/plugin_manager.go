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
	"sync"

	"k8s.io/klog/v2"
)

var PluginManagerInstance *PluginManager

// PluginBuilder the builder of a plugin.
type PluginBuilder func() Plugin

// PluginManager The manager of plugins.
type PluginManager struct {
	mutex          sync.Mutex
	pluginBuilders map[string]PluginBuilder
}

func init() {
	PluginManagerInstance = &PluginManager{
		pluginBuilders: make(map[string]PluginBuilder),
	}
}

func (pm *PluginManager) RegisterPluginBuilder(name string, pb PluginBuilder) {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	pm.pluginBuilders[name] = pb
	klog.V(3).Infof("Register plugin builder <%s> done.", name)
}

func (pm *PluginManager) GetPluginBuilders() map[string]PluginBuilder {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	return pm.pluginBuilders
}
