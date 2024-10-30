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

package mutating

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/karmada-io/karmada/pkg/apis/work/v1alpha2"
	"gomodules.xyz/jsonpatch/v2"
	admissionv1 "k8s.io/api/admission/v1"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/json"

	"volcano.sh/volcano-global/pkg/utils"
	"volcano.sh/volcano-global/pkg/webhooks/decoder"
)

func TestResourceBindings(t *testing.T) {
	suspendedRBJson, err := json.Marshal(v1alpha2.ResourceBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: "rb-suspended",
		},
		Spec: v1alpha2.ResourceBindingSpec{
			Resource: v1alpha2.ObjectReference{
				APIVersion: appsv1.SchemeGroupVersion.String(),
				Kind:       "Deployment",
			},
			Suspend: true,
		},
	})
	if err != nil {
		t.Errorf("Failed to marshal suspended ResourceBinding json, err: %v", err)
	}

	notSuspendedRBJson, err := json.Marshal(v1alpha2.ResourceBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: "rb-not-suspended",
		},
		Spec: v1alpha2.ResourceBindingSpec{
			Resource: v1alpha2.ObjectReference{
				APIVersion: appsv1.SchemeGroupVersion.String(),
				Kind:       "Deployment",
			},
		},
	})
	if err != nil {
		t.Errorf("Failed to marshal not suspended ResourceBinding json, err: %v", err)
	}

	normalResponsePatch, err := json.Marshal([]jsonpatch.Operation{
		{Operation: "replace", Path: "/spec/suspend", Value: true},
	})
	if err != nil {
		t.Errorf("Failed to marshal normal response patch json, err: %v", err)
	}

	testCases := []struct {
		Name           string
		review         admissionv1.AdmissionReview
		expectResponse admissionv1.AdmissionResponse
	}{
		{
			Name: "Default action",
			review: admissionv1.AdmissionReview{
				Request: &admissionv1.AdmissionRequest{
					Resource:  decoder.ResourceBindingGVR,
					Operation: admissionv1.Create,
					Object:    runtime.RawExtension{Raw: notSuspendedRBJson},
				},
			},
			expectResponse: admissionv1.AdmissionResponse{
				Allowed:   true,
				Patch:     normalResponsePatch,
				PatchType: utils.ToPointer(admissionv1.PatchTypeJSONPatch),
			},
		},
		{
			Name: "Invalid ResourceBinding GVR",
			review: admissionv1.AdmissionReview{
				Request: &admissionv1.AdmissionRequest{
					Resource: metav1.GroupVersionResource{
						Group:    "invalid",
						Version:  "invalid",
						Resource: "invalid",
					},
					Operation: admissionv1.Create,
					Object:    runtime.RawExtension{Raw: notSuspendedRBJson},
				},
			},
			expectResponse: admissionv1.AdmissionResponse{
				Allowed: false,
				Result: &metav1.Status{
					Message: fmt.Sprintf("expect resource to be %s", decoder.ResourceBindingGVR),
				},
			},
		},
		{
			Name: "Invalid operation",
			review: admissionv1.AdmissionReview{
				Request: &admissionv1.AdmissionRequest{
					Resource:  decoder.ResourceBindingGVR,
					Operation: "invalid",
				},
			},
			expectResponse: admissionv1.AdmissionResponse{
				Allowed: false,
				Result: &metav1.Status{
					Message: fmt.Sprintf("expect operation to be '%s'", admissionv1.Create),
				},
			},
		},
		{
			Name: "Suspend=true, should not have patch",
			review: admissionv1.AdmissionReview{
				Request: &admissionv1.AdmissionRequest{
					Resource:  decoder.ResourceBindingGVR,
					Operation: admissionv1.Create,
					Object:    runtime.RawExtension{Raw: suspendedRBJson},
				},
			},
			expectResponse: admissionv1.AdmissionResponse{
				Allowed: true,
			},
		},
	}

	for _, tc := range testCases {
		response := ResourceBindings(tc.review)
		if !reflect.DeepEqual(*response, tc.expectResponse) {
			t.Errorf("Test case %s failed, got: %v expect: %v", tc.Name, response, tc.expectResponse)
		}
	}
}
