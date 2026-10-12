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

package priority

import (
	"testing"

	workv1alpha2 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"volcano.sh/volcano-global/pkg/dispatcher/api"
)

func newRBI(name string, priority int32) *api.ResourceBindingInfo {
	return &api.ResourceBindingInfo{
		ResourceBinding: &workv1alpha2.ResourceBinding{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		},
		Priority: priority,
	}
}

func TestResourceBindingInfoOrderFunc(t *testing.T) {
	pp := New().(*priorityPlugin)

	tests := []struct {
		name string
		l, r *api.ResourceBindingInfo
		want int
	}{
		{name: "higher priority goes first", l: newRBI("a", 10), r: newRBI("b", 1), want: -1},
		{name: "lower priority goes last", l: newRBI("a", 1), r: newRBI("b", 10), want: 1},
		{name: "equal priority keeps the order", l: newRBI("a", 5), r: newRBI("b", 5), want: 0},
		{name: "negative priorities are compared numerically", l: newRBI("a", -1), r: newRBI("b", -5), want: -1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := pp.resourceBindingInfoOrderFunc(tt.l, tt.r); got != tt.want {
				t.Errorf("resourceBindingInfoOrderFunc() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestPluginName(t *testing.T) {
	if got := New().Name(); got != PluginName {
		t.Errorf("Name() = %q, want %q", got, PluginName)
	}
}
