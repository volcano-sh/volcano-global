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

package decoder

import (
	"fmt"

	workv1alpha2 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha2"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/klog/v2"
)

func init() {
	addToScheme(scheme)
}

var scheme = runtime.NewScheme()

// codecs for retrieving serializers for the supported wire formats
// and conversion wrappers to define preferred internal and external versions.
var codecs = serializer.NewCodecFactory(scheme)

func addToScheme(scheme *runtime.Scheme) {
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(admissionv1.AddToScheme(scheme))
}

var ResourceBindingGVR = metav1.GroupVersionResource{
	Group:    workv1alpha2.GroupVersion.Group,
	Version:  workv1alpha2.GroupVersion.Version,
	Resource: workv1alpha2.ResourcePluralResourceBinding,
}

// DecodeResourceBinding decode the ResourceBinding use deserializer from the raw object.
func DecodeResourceBinding(object runtime.RawExtension, gvr metav1.GroupVersionResource) (*workv1alpha2.ResourceBinding, error) {
	if gvr != ResourceBindingGVR {
		return nil, fmt.Errorf("expect resource to be %s", ResourceBindingGVR)
	}

	deserializer := codecs.UniversalDeserializer()
	resourceBinding := &workv1alpha2.ResourceBinding{}
	if _, _, err := deserializer.Decode(object.Raw, nil, resourceBinding); err != nil {
		return nil, err
	}

	klog.V(5).Infof("The ResourceBinding struct is %+v", resourceBinding)
	return resourceBinding, nil
}
