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

package cache

import (
	"fmt"
	"sync"

	clusterv1alpha1 "github.com/karmada-io/karmada/pkg/apis/cluster/v1alpha1"
	workv1alpha2 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha2"
	karmadaclientset "github.com/karmada-io/karmada/pkg/generated/clientset/versioned"
	karmadainformerfactory "github.com/karmada-io/karmada/pkg/generated/informers/externalversions"
	informerclusterv1alpha1 "github.com/karmada-io/karmada/pkg/generated/informers/externalversions/cluster/v1alpha1"
	informerworkv1aplha2 "github.com/karmada-io/karmada/pkg/generated/informers/externalversions/work/v1alpha2"
	schedulingv1 "k8s.io/api/scheduling/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/informers"
	schedv1 "k8s.io/client-go/informers/scheduling/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	datadependencyv1alpha1 "volcano.sh/apis/pkg/apis/datadependency/v1alpha1"
	volcanoclientset "volcano.sh/apis/pkg/client/clientset/versioned"
	volcanoinformer "volcano.sh/apis/pkg/client/informers/externalversions"
	datadependencyinformers "volcano.sh/apis/pkg/client/informers/externalversions/datadependency/v1alpha1"
	schedulinginformer "volcano.sh/apis/pkg/client/informers/externalversions/scheduling/v1beta1"
	schedulingapi "volcano.sh/volcano/pkg/scheduler/api"

	"volcano.sh/volcano-global/pkg/dispatcher/api"
	"volcano.sh/volcano-global/pkg/dispatcher/cache/utils"
	"volcano.sh/volcano-global/pkg/utils/feature"
)

type DispatcherCacheOption struct {
	WorkerNum        uint32
	DefaultQueueName string
	RestConfig       *rest.Config
}

type DispatcherCache struct {
	mutex     sync.Mutex
	workerNum uint32

	kubeClient           kubernetes.Interface
	dynamicClient        dynamic.Interface
	vcClient             volcanoclientset.Interface
	karmadaClient        karmadaclientset.Interface
	dataDependencyClient volcanoclientset.Interface
	restMapper           meta.RESTMapper

	informerFactory        informers.SharedInformerFactory
	volcanoInformerFactory volcanoinformer.SharedInformerFactory
	karmadaInformerFactor  karmadainformerfactory.SharedInformerFactory
	dscInformerFactory     volcanoinformer.SharedInformerFactory

	queueInformer schedulinginformer.QueueInformer
	queues        map[string]*schedulingapi.QueueInfo
	defaultQueue  string

	priorityClassInformer schedv1.PriorityClassInformer
	priorityClasses       map[string]*schedulingv1.PriorityClass
	defaultPriorityClass  *schedulingv1.PriorityClass

	resourceBindingInformer informerworkv1aplha2.ResourceBindingInformer
	// resourceBindings[namespace][name] = target ResourceBinding.
	resourceBindings map[string]map[string]*workv1alpha2.ResourceBinding

	// The infos only save basic information like ResourceBinding, ResourceUID, Status in the cache,
	// Queue, and Priority will update when Snapshot.
	// resourceBindingInfos[namespace][name] = target ResourceBindingInfo.
	resourceBindingInfos map[string]map[string]*api.ResourceBindingInfo

	clusterInformer informerclusterv1alpha1.ClusterInformer
	// clusters[name] = target Cluster
	clusters map[string]*clusterv1alpha1.Cluster

	dataSourceClaimInformer datadependencyinformers.DataSourceClaimInformer
	// dataSourceClaims[WorkloadRef] = target DataSourceClaim
	// WorkloadRef is constructed as "apiVersion/kind/namespace/name"
	dataSourceClaims map[string]*datadependencyv1alpha1.DataSourceClaim

	// Its queue for unsuspend the ResourceBinding, when a ResourceBinding finish dispatch,
	// The Dispatcher will add a task to here, and update the ResourceBinding.spec.Suspend = false.
	unSuspendRBTaskQueue workqueue.Interface
}

func NewDispatcherCache(option *DispatcherCacheOption) (DispatcherCacheInterface, error) {
	config := option.RestConfig
	kubeClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to init kubeClient: %w", err)
	}
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to init dynamicClient: %w", err)
	}
	volcanoClient, err := volcanoclientset.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to init vcClient: %w", err)
	}
	karmadaClient, err := karmadaclientset.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to init karmadaClient: %w", err)
	}

	// Create the default queue
	utils.CreateDefaultQueue(volcanoClient, option.DefaultQueueName)

	grp, err := restmapper.GetAPIGroupResources(kubeClient.Discovery())
	if err != nil {
		return nil, fmt.Errorf("failed to init grp: %w", err)
	}

	sc := &DispatcherCache{
		workerNum: option.WorkerNum,

		kubeClient:    kubeClient,
		dynamicClient: dynamicClient,
		vcClient:      volcanoClient,
		karmadaClient: karmadaClient,
		restMapper:    restmapper.NewDiscoveryRESTMapper(grp),

		informerFactory:        informers.NewSharedInformerFactory(kubeClient, 0),
		volcanoInformerFactory: volcanoinformer.NewSharedInformerFactory(volcanoClient, 0),
		karmadaInformerFactor:  karmadainformerfactory.NewSharedInformerFactory(karmadaClient, 0),

		queues:           map[string]*schedulingapi.QueueInfo{},
		defaultQueue:     option.DefaultQueueName,
		priorityClasses:  map[string]*schedulingv1.PriorityClass{},
		resourceBindings: map[string]map[string]*workv1alpha2.ResourceBinding{},

		resourceBindingInfos: map[string]map[string]*api.ResourceBindingInfo{},

		clusters: map[string]*clusterv1alpha1.Cluster{},

		unSuspendRBTaskQueue: workqueue.New(),
	}

	sc.queueInformer = sc.volcanoInformerFactory.Scheduling().V1beta1().Queues()
	sc.queueInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    sc.addQueue,
		UpdateFunc: sc.updateQueue,
		DeleteFunc: sc.deleteQueue,
	})

	sc.priorityClassInformer = sc.informerFactory.Scheduling().V1().PriorityClasses()
	sc.priorityClassInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    sc.addPriorityClass,
		UpdateFunc: sc.updatePriorityClass,
		DeleteFunc: sc.deletePriorityClass,
	})

	sc.resourceBindingInformer = sc.karmadaInformerFactor.Work().V1alpha2().ResourceBindings()
	sc.resourceBindingInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    sc.addResourceBinding,
		UpdateFunc: sc.updateResourceBinding,
		DeleteFunc: sc.deleteResourceBinding,
	})
	sc.clusterInformer = sc.karmadaInformerFactor.Cluster().V1alpha1().Clusters()
	sc.clusterInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    sc.addCluster,
		UpdateFunc: sc.updateCluster,
		DeleteFunc: sc.deleteCluster,
	})

	// Initialize DSC client, informer factory and event handlers only if data dependency awareness is enabled
	if utilfeature.DefaultFeatureGate.Enabled(feature.DataDependencyAwareness) {
		// Initialize data structures
		sc.dataSourceClaims = map[string]*datadependencyv1alpha1.DataSourceClaim{}

		// Initialize DSC client
		dscClient, err := volcanoclientset.NewForConfig(config)
		if err != nil {
			return nil, fmt.Errorf("failed to init dscClient: %w", err)
		}
		sc.dataDependencyClient = dscClient

		// Initialize DSC informer factory
		sc.dscInformerFactory = volcanoinformer.NewSharedInformerFactory(dscClient, 0)

		// Initialize DataSourceClaim informer and event handlers
		sc.dataSourceClaimInformer = sc.dscInformerFactory.Datadependency().V1alpha1().DataSourceClaims()
		sc.dataSourceClaimInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc:    sc.addDataSourceClaim,
			UpdateFunc: sc.updateDataSourceClaim,
			DeleteFunc: sc.deleteDataSourceClaim,
		})
	}

	return sc, nil
}

func (dc *DispatcherCache) Run(stopCh <-chan struct{}) {
	// Start the factories, and wait for cache sync
	dc.informerFactory.Start(stopCh)
	dc.volcanoInformerFactory.Start(stopCh)
	dc.karmadaInformerFactor.Start(stopCh)
	for informerType, ok := range dc.informerFactory.WaitForCacheSync(stopCh) {
		if !ok {
			klog.Errorf("Caches failed to sync: %v", informerType)
		}
	}
	for informerType, ok := range dc.volcanoInformerFactory.WaitForCacheSync(stopCh) {
		if !ok {
			klog.Errorf("Caches failed to sync: %v", informerType)
		}
	}
	for informerType, ok := range dc.karmadaInformerFactor.WaitForCacheSync(stopCh) {
		if !ok {
			klog.Errorf("Caches failed to sync: %v", informerType)
		}
	}

	// Start DSC informer factory only if data dependency awareness is enabled
	// Wait for DSC informer cache sync only if data dependency awareness is enabled
	if utilfeature.DefaultFeatureGate.Enabled(feature.DataDependencyAwareness) {
		dc.dscInformerFactory.Start(stopCh)
		for informerType, ok := range dc.dscInformerFactory.WaitForCacheSync(stopCh) {
			if !ok {
				klog.Errorf("Caches failed to sync: %v", informerType)
			}
		}
	}

	for i := uint32(1); i <= dc.workerNum; i++ {
		go wait.Until(dc.unSuspendResourceBindingTaskWorker, 0, stopCh)
	}

	klog.V(2).Infof("DispatcherCache completes initialization and start to run.")
}
