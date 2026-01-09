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

package scheme

import ctrl "sigs.k8s.io/controller-runtime"

// ReconcilerInitializers is a global map of reconciler initializers.
// Reconcilers should add their init functions to this map in their own init() functions.
// The key is the name of the reconciler
var ReconcilerInitializers = make(map[string]AddToManagerFunc)

// AddToManagerFunc defines a function type for initializing a new reconciler and adding it to a manager.
type AddToManagerFunc func(ctrl.Manager) error
