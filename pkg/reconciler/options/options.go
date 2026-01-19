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
	"strings"

	"github.com/spf13/pflag"
)

var (
	Opt             = newOptions()
	reconcilerFlags = pflag.NewFlagSet("reconciler", pflag.ContinueOnError)
)

func newOptions() *Options {
	return &Options{
		EnabledReconcilers: []string{"*"}, // Default: enable all reconcilers
	}
}

type Options struct {
	// EnabledReconcilers specifies which reconcilers to enable.
	// Use "*" to enable all, or provide a comma-separated list of reconciler names.
	EnabledReconcilers []string
}

// RegisterReconcilerFlags registers reconciler related flags.
func RegisterReconcilerFlags() *pflag.FlagSet {
	Opt.AddFlags(reconcilerFlags)
	return reconcilerFlags
}

// AddFlags adds reconciler related flags.
func (o *Options) AddFlags(fs *pflag.FlagSet) {
	fs.StringSliceVar(&o.EnabledReconcilers, "reconcilers", o.EnabledReconcilers,
		"Comma-separated list of reconcilers to enable. Use '*' to enable all. Current Available reconcilers: hyperjob")
}

// IsReconcilerEnabled checks if a specific reconciler is enabled.
func (o *Options) IsReconcilerEnabled(name string) bool {
	// If "*" is in the list, all reconcilers are enabled
	for _, r := range o.EnabledReconcilers {
		if r == "*" {
			return true
		}
		if strings.TrimSpace(r) == name {
			return true
		}
	}
	return false
}
