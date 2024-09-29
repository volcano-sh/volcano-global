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

package deployment

import (
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	appinformers "k8s.io/client-go/informers/apps/v1"
	"k8s.io/client-go/kubernetes"
	applisters "k8s.io/client-go/listers/apps/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	volcanoclientset "volcano.sh/apis/pkg/client/clientset/versioned"
	informerfactory "volcano.sh/apis/pkg/client/informers/externalversions"
	volcanoinformer "volcano.sh/apis/pkg/client/informers/externalversions"
	schedulinginformer "volcano.sh/apis/pkg/client/informers/externalversions/scheduling/v1beta1"
	schedulinglister "volcano.sh/apis/pkg/client/listers/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/controllers/framework"
)

func init() {
	framework.RegisterController(&deploymentController{})
}

const controllerName = "deployment-controller"

type deploymentController struct {
	kubeClient kubernetes.Interface
	vcClient   volcanoclientset.Interface

	informerFactory        informers.SharedInformerFactory
	volcanoInformerFactory volcanoinformer.SharedInformerFactory

	deploymentInformer appinformers.DeploymentInformer
	deploymentLister   applisters.DeploymentLister

	podGroupInformer schedulinginformer.PodGroupInformer
	podGroupLister   schedulinglister.PodGroupLister

	queue       workqueue.RateLimitingInterface
	pgWorkerNum uint32
}

func (dc *deploymentController) Name() string {
	return controllerName
}

func (dc *deploymentController) Initialize(opt *framework.ControllerOption) error {
	dc.kubeClient = opt.KubeClient
	dc.informerFactory = opt.SharedInformerFactory
	dc.pgWorkerNum = opt.WorkerThreadsForPG
	dc.vcClient = opt.VolcanoClient
	dc.queue = workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())

	dc.deploymentInformer = opt.SharedInformerFactory.Apps().V1().Deployments()
	dc.deploymentLister = dc.deploymentInformer.Lister()

	dc.deploymentInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: dc.addDeploymentHandler,
	})

	dc.volcanoInformerFactory = informerfactory.NewSharedInformerFactory(dc.vcClient, 0)
	dc.podGroupInformer = dc.volcanoInformerFactory.Scheduling().V1beta1().PodGroups()
	dc.podGroupLister = dc.podGroupInformer.Lister()
	return nil
}

func (dc *deploymentController) Run(stopCh <-chan struct{}) {
	// Start the factories, and wait for cache sync
	dc.informerFactory.Start(stopCh)
	dc.volcanoInformerFactory.Start(stopCh)
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

	// Start the PodGroup controller workers
	for i := 1; i <= int(dc.pgWorkerNum); i++ {
		go wait.Until(dc.worker, 0, stopCh)
	}

	klog.Infof("%s is running, pgWorkerNum: %d ......", controllerName, dc.pgWorkerNum)
}

func (dc *deploymentController) worker() {
	for dc.processNext() {
	}
}

func (dc *deploymentController) processNext() bool {
	obj, shutdown := dc.queue.Get()
	if shutdown {
		klog.Errorf("Failed to get request from queue, queue is shutdown.")
		return false
	}

	req := obj.(types.NamespacedName)
	defer dc.queue.Done(req)

	deployment, err := dc.deploymentLister.Deployments(req.Namespace).Get(req.Name)
	if err != nil {
		klog.Errorf("Failed to get Deployment by <%s/%s> from cache: %v", req.Namespace, req.Name, err)
		return true
	}

	if deployment.Annotations != nil && deployment.Annotations[schedulingv1beta1.KubeGroupNameAnnotationKey] != "" {
		klog.V(5).Infof("Deployment <%s/%s> already had created PodGroup.", req.Namespace, req.Name)
		dc.queue.Forget(req)
		return true
	}

	if err = dc.createPodGroupForDeployment(deployment); err != nil {
		klog.Errorf("Failed to create PodGroup for Deployment <%s/%s>, err: %v", req.Namespace, req.Name, err)
		dc.queue.AddRateLimited(req)
		return true
	}

	dc.queue.Forget(req)
	return true
}
