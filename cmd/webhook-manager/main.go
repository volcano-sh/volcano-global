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
	"time"

	"github.com/spf13/pflag"
	cliflag "k8s.io/component-base/cli/flag"
	"k8s.io/klog/v2"
	"volcano.sh/volcano/cmd/webhook-manager/app"
	"volcano.sh/volcano/cmd/webhook-manager/app/options"
	"volcano.sh/volcano/pkg/version"

	_ "volcano.sh/volcano-global/pkg/webhooks/resourcebinding/mutating"
	_ "volcano.sh/volcano/pkg/webhooks/admission/jobs/mutate"
	_ "volcano.sh/volcano/pkg/webhooks/admission/jobs/validate"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	klog.InitFlags(nil)

	config := options.NewConfig()
	config.AddFlags(pflag.CommandLine)

	cliflag.InitFlags()

	if config.PrintVersion {
		version.PrintVersionAndExit()
		return
	}

	klog.StartFlushDaemon(5 * time.Second)
	defer klog.Flush()

	if err := config.CheckPortOrDie(); err != nil {
		klog.Fatalf("Configured port check failed: %v", err)
	}

	if err := config.ParseCAFiles(nil); err != nil {
		klog.Fatalf("Failed to parse CA files: %v", err)
	}

	if err := app.Run(config); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}
