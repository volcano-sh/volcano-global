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

package datadependency

import (
	"context"
	"fmt"
	"time"

	workv1alpha2 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha2"
	karmadaclientset "github.com/karmada-io/karmada/pkg/generated/clientset/versioned"
	karmadascheme "github.com/karmada-io/karmada/pkg/generated/clientset/versioned/scheme"
	karmadainformerfactory "github.com/karmada-io/karmada/pkg/generated/informers/externalversions"
	clusterlisters "github.com/karmada-io/karmada/pkg/generated/listers/cluster/v1alpha1"
	worklisters "github.com/karmada-io/karmada/pkg/generated/listers/work/v1alpha2"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"volcano.sh/apis/pkg/apis/datadependency/v1alpha1"
	"volcano.sh/apis/pkg/client/clientset/versioned"
	datadependencyscheme "volcano.sh/apis/pkg/client/clientset/versioned/scheme"
	externalversions "volcano.sh/apis/pkg/client/informers/externalversions"
	datadependencylisters "volcano.sh/apis/pkg/client/listers/datadependency/v1alpha1"
	"volcano.sh/volcano/pkg/controllers/framework"
)

const (
	controllerName = "datadependency-controller"
	maxRequeueNum  = 10
)

func init() {
	if err := framework.RegisterController(&DataDependencyController{}); err != nil {
		panic(fmt.Sprintf("failed to register datasourceclaim controller: %v", err))
	}
}

type DataDependencyController struct {
	datadependencyClient versioned.Interface
	dynamic              dynamic.Interface
	karmadaClient        karmadaclientset.Interface
	kubeClient           kubernetes.Interface

	Scheme *runtime.Scheme

	// Informer factories
	dataInformerFactory    externalversions.SharedInformerFactory
	karmadaInformerFactory karmadainformerfactory.SharedInformerFactory
	kubeInformerFactory    kubeinformers.SharedInformerFactory

	// dscLister can list/get datasource claims from the shared informer's store
	dscLister datadependencylisters.DataSourceClaimLister
	// dsLister can list/get datasources from the shared informer's store
	dsLister datadependencylisters.DataSourceLister
	// rbLister can list/get resourcebindings from the shared informer's store
	rbLister worklisters.ResourceBindingLister
	// cLister can list/get clusters from the shared informer's store
	cLister clusterlisters.ClusterLister
	// configMapLister can list/get configmaps from the shared informer's store
	configMapLister corelisters.ConfigMapLister

	// dscListerSynced is true if the datasourceclaim shared informer has synced once
	dscListerSynced cache.InformerSynced
	// dsListerSynced is true if the datasource shared informer has synced once
	dsListerSynced cache.InformerSynced
	// rbListerSynced is true if the resourcebinding shared informer has synced once
	rbListerSynced cache.InformerSynced
	// cListerSynced is true if the cluster shared informer has synced once
	cListerSynced cache.InformerSynced
	// configMapListerSynced is true if the configmap shared informer has synced once
	configMapListerSynced cache.InformerSynced

	queue workqueue.RateLimitingInterface

	recorder record.EventRecorder

	// pluginManager manages data source plugins for dynamic binding
	pluginManager PluginManagerInterface

	syncHandler func(key string) error
}

func (c *DataDependencyController) Name() string {
	return controllerName
}

func (c *DataDependencyController) Initialize(opt *framework.ControllerOption) error {
	config := opt.Config
	dscClient, err := versioned.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("failed to init datasource client: %v", err)
	}
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("failed to init dynamic client: %v", err)
	}
	karmadaClient, err := karmadaclientset.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("failed to init karmada client: %v", err)
	}
	kubeClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("failed to init kubernetes client: %v", err)
	}

	c.datadependencyClient = dscClient
	c.dynamic = dynamicClient
	c.karmadaClient = karmadaClient
	c.kubeClient = kubeClient

	// Create DataDependency InformerFactory for custom CRDs
	c.dataInformerFactory = externalversions.NewSharedInformerFactory(dscClient, 0)

	// Create Karmada InformerFactory for ResourceBinding
	c.karmadaInformerFactory = karmadainformerfactory.NewSharedInformerFactory(karmadaClient, 0)

	// Create Kubernetes InformerFactory for ConfigMap
	c.kubeInformerFactory = kubeinformers.NewSharedInformerFactory(kubeClient, 0)

	// DataSourceClaim informer
	dscInformer := c.dataInformerFactory.Datadependency().V1alpha1().DataSourceClaims()
	if err = dscInformer.Informer().AddIndexers(cache.Indexers{
		WorkloadRefIndex: func(obj interface{}) ([]string, error) {
			dsc, ok := obj.(*v1alpha1.DataSourceClaim)
			if !ok {
				return nil, fmt.Errorf("failed to convert to DataSourceClaim")
			}
			workloadRefKey, indexErr := GenerateWorkloadRefIndexKey(&dsc.Spec.Workload)
			if indexErr != nil {
				return nil, indexErr
			}
			return []string{workloadRefKey}, nil
		},
	}); err != nil {
		return fmt.Errorf("failed to add indexer to dsc informer: %v", err)
	}
	c.dscLister = dscInformer.Lister()
	c.dscListerSynced = dscInformer.Informer().HasSynced

	// DataSource informer
	dsInformer := c.dataInformerFactory.Datadependency().V1alpha1().DataSources()
	c.dsLister = dsInformer.Lister()
	c.dsListerSynced = dsInformer.Informer().HasSynced

	// ResourceBinding informer
	rbInformer := c.karmadaInformerFactory.Work().V1alpha2().ResourceBindings()
	if err = rbInformer.Informer().AddIndexers(cache.Indexers{
		WorkloadRefIndex: func(obj interface{}) ([]string, error) {
			rb, ok := obj.(*workv1alpha2.ResourceBinding)
			if !ok {
				return nil, fmt.Errorf("failed to convert to ResourceBinding")
			}
			workloadRefKey, indexErr := GenerateWorkloadRefIndexKeyFromResource(rb.Spec.Resource)
			if indexErr != nil {
				return nil, indexErr
			}
			return []string{workloadRefKey}, nil
		},
	}); err != nil {
		return fmt.Errorf("failed to add indexer to rb informer: %v", err)
	}
	rbInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: c.addResourceBinding,
	})
	c.rbLister = rbInformer.Lister()
	c.rbListerSynced = rbInformer.Informer().HasSynced

	// Cluster informer
	cLister := c.karmadaInformerFactory.Cluster().V1alpha1().Clusters()
	c.cLister = cLister.Lister()
	c.cListerSynced = cLister.Informer().HasSynced

	// ConfigMap informer
	configMapInformer := c.kubeInformerFactory.Core().V1().ConfigMaps()
	c.configMapLister = configMapInformer.Lister()
	c.configMapListerSynced = configMapInformer.Informer().HasSynced

	// Create workqueue
	c.queue = workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), controllerName)

	// Initialize plugin manager
	c.pluginManager = NewPluginManager(kubeClient, dscClient)

	// Register built-in plugins
	c.registerBuiltinPlugins()

	// Create scheme
	controllerScheme := runtime.NewScheme()
	if err := scheme.AddToScheme(controllerScheme); err != nil {
		return fmt.Errorf("failed to add kubernetes scheme: %v", err)
	}
	if err := karmadascheme.AddToScheme(controllerScheme); err != nil {
		return fmt.Errorf("failed to add karmada scheme: %v", err)
	}
	if err := datadependencyscheme.AddToScheme(controllerScheme); err != nil {
		return fmt.Errorf("failed to add datadependency scheme: %v", err)
	}
	c.Scheme = controllerScheme

	// Create event broadcaster
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(klog.Infof)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: kubeClient.CoreV1().Events("")})

	// Create event recorder
	c.recorder = eventBroadcaster.NewRecorder(c.Scheme, corev1.EventSource{Component: controllerName})

	// Add event handlers
	dscInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.addDataSourceClaim,
		UpdateFunc: c.updateDataSourceClaim,
		DeleteFunc: c.deleteDataSourceClaim,
	})

	dsInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.addDataSource,
		UpdateFunc: c.updateDataSource,
		DeleteFunc: c.deleteDataSource,
	})

	// Add ConfigMap event handler for plugin configuration changes
	configMapInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.addConfigMap,
		UpdateFunc: c.updateConfigMap,
		DeleteFunc: c.deleteConfigMap,
	})

	c.syncHandler = c.Reconcile
	return nil
}

// registerBuiltinPlugins registers all built-in data source plugins
func (c *DataDependencyController) registerBuiltinPlugins() {
	// Register Amoro plugin by name - actual plugin loading will be done dynamically
	c.pluginManager.RegisterPluginName("amoro")

	klog.V(3).Infof("Registered built-in data source plugins")
}

// initializePluginSystem initializes the plugin system by loading configurations and initializing plugins
func (c *DataDependencyController) initializePluginSystem(ctx context.Context) error {
	klog.Infof("Initializing plugin system for %s controller", controllerName)

	// Load plugin configurations
	if err := c.pluginManager.LoadConfigurations(ctx); err != nil {
		klog.Errorf("Failed to load plugin configurations: %v", err)
		return fmt.Errorf("failed to load plugin configurations: %w", err)
	}
	klog.Infof("Successfully loaded plugin configurations")

	// Initialize plugins
	if err := c.pluginManager.InitializePlugins(ctx); err != nil {
		klog.Errorf("Failed to initialize plugins: %v", err)
		return fmt.Errorf("failed to initialize plugins: %w", err)
	}
	klog.Infof("Successfully initialized plugins")

	return nil
}

func (c *DataDependencyController) Run(stopCh <-chan struct{}) {
	defer c.queue.ShutDown()

	klog.Infof("Starting %s controller", controllerName)
	defer klog.Infof("Shutting down %s controller", controllerName)

	// Start the informer factories
	c.dataInformerFactory.Start(stopCh)
	c.karmadaInformerFactory.Start(stopCh)
	c.kubeInformerFactory.Start(stopCh)

	// Wait for the caches to be synced before starting workers
	if !cache.WaitForCacheSync(stopCh, c.dscListerSynced, c.dsListerSynced, c.rbListerSynced, c.cListerSynced, c.configMapListerSynced) {
		klog.Errorf("Failed to wait for caches to sync")
		return
	}

	klog.Infof("Starting workers for %s controller", controllerName)

	// Initialize plugin system
	ctx := context.Background()
	if err := c.initializePluginSystem(ctx); err != nil {
		klog.Warningf("Plugin system initialization failed, continuing with degraded functionality: %v", err)
		// Continue anyway, plugins might be loaded later via periodic updates or manual reload
	}

	// Start periodic update of DataSource cluster information after caches are synced
	c.pluginManager.StartPeriodicUpdate(context.Background())

	// Start the worker goroutine
	go wait.Until(c.worker, time.Second, stopCh)

	// Wait until we receive a stop signal
	<-stopCh

	klog.Infof("Shutting down workers for %s controller", controllerName)

	// Stop the plugin manager's periodic update first
	c.pluginManager.StopPeriodicUpdate()
}

// DataSourceClaim event handlers

// worker runs a worker thread that just dequeues items, processes them, and marks them done.
func (c *DataDependencyController) worker() {
	for c.processNextWorkItem() {
	}
}

// processNextWorkItem will read a single work item off the workqueue and
// attempt to process it, by calling the syncHandler.
func (c *DataDependencyController) processNextWorkItem() bool {
	key, shutdown := c.queue.Get()

	if shutdown {
		return false
	}

	// We call Done here so the workqueue knows we have finished
	// processing this item.
	defer c.queue.Done(key)

	// Run the syncHandler, passing it the namespace/name string of the
	// DataSourceClaim resource to be synced.
	err := c.syncHandler(key.(string))
	c.handleErr(err, key)

	return true
}

// handleErr handles errors from processing work items
func (c *DataDependencyController) handleErr(err error, key interface{}) {
	if err == nil {
		c.queue.Forget(key)
		klog.Infof("Successfully synced '%s'", key)
		return
	}

	if c.queue.NumRequeues(key) < maxRequeueNum {
		klog.V(2).Infof("Error syncing DataSourceClaim '%s': %v", key, err)
		c.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	klog.V(2).InfoS("Dropping DataSourceClaim", "key", key)
	c.queue.Forget(key)
}

// syncHandler compares the actual state with the desired, and attempts to
// converge the two. It then updates the Status block of the DataSourceClaim resource
// with the current status of the resource.
func (c *DataDependencyController) Reconcile(key string) error {
	klog.V(4).Infof("Processing DataSourceClaim: %s", key)
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		klog.Errorf("Failed to split key: %v", err)
		return err
	}

	// Get the DataSourceClaim from the lister
	dsc, err := c.dscLister.DataSourceClaims(namespace).Get(name)
	if err != nil {
		// If the error is due to the object not found, don't return an error to avoid re-queuing
		if errors.IsNotFound(err) {
			klog.V(4).Infof("DataSourceClaim %s/%s not found, it may have been deleted", namespace, name)
			return nil
		}
		klog.Errorf("Failed to get DataSourceClaim: %v", err)
		return err
	}

	// Object was deleted and not in cache.
	if dsc == nil {
		return nil
	}

	// Handle deletion
	if dsc.DeletionTimestamp != nil {
		return c.handleDeletion(dsc)
	}

	// If the object is not being deleted, ensure our finalizer is present.
	if !controllerutil.ContainsFinalizer(dsc, DataSourceClaimFinalizer) {
		klog.V(4).Infof("Adding finalizer to DataSourceClaim %s/%s", dsc.Namespace, dsc.Name)
		dscToUpdate := dsc.DeepCopy()
		controllerutil.AddFinalizer(dscToUpdate, DataSourceClaimFinalizer)

		// Use the client to update the object, not just the status.
		_, err := c.datadependencyClient.DatadependencyV1alpha1().DataSourceClaims(dscToUpdate.Namespace).Update(context.TODO(), dscToUpdate, metav1.UpdateOptions{})
		if err != nil {
			klog.Errorf("Failed to add finalizer to DSC %s/%s: %v", dsc.Namespace, dsc.Name, err)
			return err
		}
		// Return here, as the update will trigger a new reconcile.
		return nil
	}

	// Initialize phase if empty (default to Pending)
	if dsc.Status.Phase == "" {
		return c.initializePhase(dsc)
	}

	// Handle different phases
	switch dsc.Status.Phase {
	case v1alpha1.DSCPhasePending:
		return c.handlePending(dsc)
	case v1alpha1.DSCPhaseBound:
		return c.handleBound(dsc)
	default:
		klog.Errorf("DataSourceClaim %s/%s has unknown phase: %s", dsc.Namespace, dsc.Name, dsc.Status.Phase)
		return fmt.Errorf("unknown phase: %s", dsc.Status.Phase)
	}
}
