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

package feature

import (
	"k8s.io/apimachinery/pkg/util/runtime"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/component-base/featuregate"
)

const (
	// DataDependencyAwareness enables data dependency awareness features in volcano-global.
	// This feature allows workloads to be scheduled based on data locality and availability.
	//
	// owner: @volcano-global-team
	// alpha: v1.0.0
	DataDependencyAwareness featuregate.Feature = "DataDependencyAwareness"
)

func init() {
	runtime.Must(utilfeature.DefaultMutableFeatureGate.Add(defaultVolcanoGlobalFeatureGates))
}

// defaultVolcanoGlobalFeatureGates consists of all known volcano-global-specific feature keys.
// To add a new feature, define a key for it above and add it here. The features will be
// available throughout volcano-global binaries.
var defaultVolcanoGlobalFeatureGates = map[featuregate.Feature]featuregate.FeatureSpec{
	DataDependencyAwareness: {Default: false, PreRelease: featuregate.Alpha},
}
