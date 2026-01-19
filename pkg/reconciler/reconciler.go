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

package reconciler

import (
	"fmt"

	policyv1alpha1 "github.com/karmada-io/karmada/pkg/apis/policy/v1alpha1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/metrics/server"
	batchv1alpha1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	trainingv1alpha1 "volcano.sh/apis/pkg/apis/training/v1alpha1"
	"volcano.sh/volcano/pkg/controllers/framework"

	reconcileroptions "volcano.sh/volcano-global/pkg/reconciler/options"
	initializescheme "volcano.sh/volcano-global/pkg/reconciler/scheme"

	// Import all reconciler to register them.
	_ "volcano.sh/volcano-global/pkg/reconciler/hyperjob"
)

var (
	scheme = runtime.NewScheme()
)

const ControllerName = "reconciler"

func init() {
	utilruntime.Must(batchv1alpha1.AddToScheme(scheme))
	utilruntime.Must(trainingv1alpha1.AddToScheme(scheme))
	utilruntime.Must(policyv1alpha1.AddToScheme(scheme))

	utilruntime.Must(framework.RegisterController(&Controller{}))
}

// Controller is the shared controller manager to manage all reconcilers written by controller-runtime framework in volcano-global.
type Controller struct {
	mgr ctrl.Manager
}

func (c *Controller) Name() string {
	return ControllerName
}

func (c *Controller) Initialize(opt *framework.ControllerOption) error {
	mgr, err := ctrl.NewManager(opt.Config, ctrl.Options{
		Scheme:         scheme,
		LeaderElection: false, // LeaderElection is handled by volcano's framework
		Metrics: server.Options{
			BindAddress: "0",
		},
	})
	if err != nil {
		klog.Errorf("Failed to initialize controller manager framework: %v", err)
		return err
	}
	c.mgr = mgr

	enabledCount := 0
	for name, initFn := range initializescheme.ReconcilerInitializers {
		if !reconcileroptions.Opt.IsReconcilerEnabled(name) {
			klog.V(4).Infof("Reconciler %s is disabled, skipping", name)
			continue
		}

		if err = initFn(mgr); err != nil {
			return fmt.Errorf("failed to add reconciler %s to shared controller manager: %w", name, err)
		}
		klog.Infof("Registered reconciler %s to the shared controller manager", name)
		enabledCount++
	}

	if enabledCount == 0 {
		klog.Warning("No reconcilers enabled, controller-runtime manager will run but do nothing")
	}

	return nil
}

func (c *Controller) Run(stopCh <-chan struct{}) {
	klog.Info("Starting shared controller manager")
	defer klog.Info("Shared controller manager stopped")

	ctx := wait.ContextForChannel(stopCh)
	if err := c.mgr.Start(ctx); err != nil {
		klog.Fatalf("Shared controller manager stopped with error: %v", err)
	}
}
