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
	"k8s.io/klog/v2"
	"k8s.io/kube-openapi/pkg/util/sets"
	volcanoapi "volcano.sh/volcano/pkg/scheduler/api"

	"volcano.sh/volcano-global/pkg/dispatcher/api"
	dispatchercache "volcano.sh/volcano-global/pkg/dispatcher/cache"
)

// Session The session stores the information needed by the dispatcher during each dispatch operation.
// It is not a persistent state and will be destroyed after each dispatch operation runs.
// If you want to store content that is effective throughout the dispatcher's entire lifecycle,
// you need to store it in the Cache and retrieve it during the Snapshot.
type Session struct {
	cache    dispatchercache.DispatcherCacheInterface
	Snapshot *dispatchercache.DispatcherCacheSnapshot

	plugins                     map[string]Plugin
	queueInfoOrderFns           map[string]volcanoapi.CompareFn
	resourceBindingInfoOrderFns map[string]volcanoapi.CompareFn
}

func OpenSession(cache dispatchercache.DispatcherCacheInterface) *Session {
	session := &Session{
		cache:    cache,
		Snapshot: cache.Snapshot(),

		plugins:                     map[string]Plugin{},
		queueInfoOrderFns:           map[string]volcanoapi.CompareFn{},
		resourceBindingInfoOrderFns: map[string]volcanoapi.CompareFn{},
	}

	// Register all the plugins to session.
	for pluginName, pluginBuilder := range PluginManagerInstance.GetPluginBuilders() {
		session.plugins[pluginName] = pluginBuilder()
		session.plugins[pluginName].OnSessionOpen(session)
	}

	klog.V(5).Infof("OpenSession done, QueueCount <%d>, ResourceBindingCount <%d> ...",
		len(session.Snapshot.QueueInfos), len(session.Snapshot.ResourceBindingInfos))

	return session
}

func (ssn *Session) CloseSession() {
	// For logs.
	pluginNameSet := sets.NewString()

	for pluginName, plugin := range ssn.plugins {
		pluginNameSet.Insert(pluginName)
		plugin.OnSessionClose(ssn)
	}

	klog.V(5).Infof("CloseSession done, Registered plugins <%v> ...", pluginNameSet.List())
}

// GetResourceBindingInfoQueue Get the workload's queue name, it may be empty when DefaultQueue is empty.
func (ssn *Session) GetResourceBindingInfoQueue(rbi *api.ResourceBindingInfo) string {
	name := rbi.Queue
	if name == "" {
		name = ssn.Snapshot.DefaultQueue
		resource := rbi.ResourceBinding.Spec.Resource
		klog.V(3).Infof("Resource %s <%s/%s> didnt set the Queue name annotation, join the default queue <%s>.",
			resource.Kind, resource.Namespace, resource.Name, ssn.Snapshot.DefaultQueue)
	}
	return name
}
