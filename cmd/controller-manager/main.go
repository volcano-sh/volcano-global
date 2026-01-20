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

package main

import (
	"fmt"
	"os"
	"runtime"
	"sort"
	"time"

	"github.com/spf13/pflag"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	cliflag "k8s.io/component-base/cli/flag"
	componentbaseoptions "k8s.io/component-base/config/options"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"volcano.sh/volcano/cmd/controller-manager/app"
	"volcano.sh/volcano/cmd/controller-manager/app/options"
	"volcano.sh/volcano/pkg/controllers/framework"
	commonutil "volcano.sh/volcano/pkg/util"
	"volcano.sh/volcano/pkg/version"

	dispatcheroptions "volcano.sh/volcano-global/pkg/dispatcher/options"
	reconcileroptions "volcano.sh/volcano-global/pkg/reconciler/options"

	_ "volcano.sh/volcano-global/pkg/controller/datadependency"
	_ "volcano.sh/volcano-global/pkg/controller/datadependency/plugins/amoro"
	_ "volcano.sh/volcano-global/pkg/dispatcher"
	_ "volcano.sh/volcano-global/pkg/reconciler"
	_ "volcano.sh/volcano/pkg/controllers/garbagecollector"
)

const componentName = "volcano-global-controller-manager"

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	klog.InitFlags(nil)
	ctrl.SetLogger(klog.Background())

	fs := pflag.CommandLine
	s := options.NewServerOption()

	var knownControllers = func() []string {
		var controllerNames []string
		fn := func(controller framework.Controller) {
			controllerNames = append(controllerNames, controller.Name())
		}
		framework.ForeachController(fn)
		sort.Strings(controllerNames)
		return controllerNames
	}

	s.AddFlags(fs, knownControllers())
	utilfeature.DefaultMutableFeatureGate.AddFlag(fs)

	commonutil.LeaderElectionDefault(&s.LeaderElection)
	s.LeaderElection.ResourceName = componentName
	componentbaseoptions.BindLeaderElectionFlags(&s.LeaderElection, fs)
	// add dispatcher flag.
	fs.AddFlagSet(dispatcheroptions.RegisterDispatcherFlags())
	// add reconciler flag.
	fs.AddFlagSet(reconcileroptions.RegisterReconcilerFlags())
	cliflag.InitFlags()

	if s.PrintVersion {
		version.PrintVersionAndExit()
		return
	}
	if err := s.CheckOptionOrDie(); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
	if s.CaCertFile != "" && s.CertFile != "" && s.KeyFile != "" {
		if err := s.ParseCAFiles(nil); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to parse CA files: %v\n", err)
			os.Exit(1)
		}
	}

	klog.StartFlushDaemon(5 * time.Second)
	defer klog.Flush()

	if err := app.Run(s); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}
