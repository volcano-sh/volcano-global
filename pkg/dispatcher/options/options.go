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

package options

import (
	"time"

	"github.com/spf13/pflag"
)

var (
	Opt             = newOptions()
	dispatcherFlags = pflag.NewFlagSet("dispatcher", pflag.ContinueOnError)
)

const (
	DefaultDispatchPeriod = time.Second
	DefaultQueue          = "default"
)

func newOptions() *options {
	return &options{}
}

type options struct {
	// DispatchPeriod is the period of dispatcher controller processes resourceBinding.
	DispatchPeriod time.Duration
	// DefaultQueueName the default queue name of the job.
	DefaultQueueName string
}

// RegisterDispatcherFlags registers dispatcher related flags.
// We need to get some additional parameters from the command line.The dispatcher is actually a controller, but it also has functions similar to the volcano-scheduler.
// However, adding a new component seems to be a heavy-handed approach at the moment. Therefore, while getting the controllerOption,
// we need to get some additional parameters here, even though this approach isn't very elegant :)
func RegisterDispatcherFlags() *pflag.FlagSet {
	Opt.AddFlags(dispatcherFlags)
	return dispatcherFlags
}

// AddFlags add dispatcher controller related flags.
func (o *options) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&o.DefaultQueueName, "default-queue", DefaultQueue, "The default queue name of the workload")
	fs.DurationVar(&o.DispatchPeriod, "dispatch-period", DefaultDispatchPeriod, "The period between each scheduling cycle")
}
