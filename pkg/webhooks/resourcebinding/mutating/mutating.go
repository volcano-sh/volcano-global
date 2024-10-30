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

	workv1alpha2 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha2"
	"gomodules.xyz/jsonpatch/v2"
	admissionv1 "k8s.io/api/admission/v1"
	registrationv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/klog/v2"
	"volcano.sh/volcano/pkg/webhooks/router"
	"volcano.sh/volcano/pkg/webhooks/util"

	"volcano.sh/volcano-global/pkg/utils"
	"volcano.sh/volcano-global/pkg/webhooks/decoder"
)

// Init the ResourceBinding mutate admissionWebhook, it will set the `suspend` to true when create a ResourceBinding.
func init() {
	router.RegisterAdmission(service)
}

var service = &router.AdmissionService{
	Path: "/resourcebindings/mutate",
	Func: ResourceBindings,
	MutatingConfig: &registrationv1.MutatingWebhookConfiguration{
		Webhooks: []registrationv1.MutatingWebhook{{
			Name: "mutateresourcebindings.volcano.sh",
			Rules: []registrationv1.RuleWithOperations{
				{
					Operations: []registrationv1.OperationType{registrationv1.Create},
					Rule: registrationv1.Rule{
						APIGroups:   []string{workv1alpha2.GroupVersion.Group},
						APIVersions: []string{workv1alpha2.GroupVersion.Version},
						Resources:   []string{workv1alpha2.ResourcePluralResourceBinding},
					},
				},
			},
		}},
	},
}

func ResourceBindings(ar admissionv1.AdmissionReview) *admissionv1.AdmissionResponse {
	if ar.Request == nil || ar.Request.Operation != admissionv1.Create {
		// This error should not be happened; We have set the rule for CREATE operation only.
		return util.ToAdmissionResponse(fmt.Errorf("expect operation is '%s'", admissionv1.Create))
	}
	klog.V(3).Infof("Mutating %s operation for ResourceBinding <%s/%s>.",
		ar.Request.Operation, ar.Request.Namespace, ar.Request.Name)

	rb, err := decoder.DecodeResourceBinding(ar.Request.Object, ar.Request.Resource)
	if err != nil {
		return util.ToAdmissionResponse(err)
	}

	response := &admissionv1.AdmissionResponse{Allowed: true}
	if rb.Spec.Suspend == true {
		// This should not be happened, the `suspend` will update to True by this webhook when create the resourceBinding.
		return response
	}

	// Check if its workload, skip suspend if not.
	isWorkload, err := utils.IsWorkload(rb.Spec.Resource)
	if err != nil {
		klog.Errorf("Failed to check ResourceBinding <%s/%s> if workload, stop suspend, err: %v",
			rb.Namespace, rb.Name, err)
		return util.ToAdmissionResponse(err)
	}
	if !isWorkload {
		klog.V(3).Infof("ResourceBinding <%s/%s> is not a workload, skip suspend it.",
			rb.Namespace, rb.Name)
		return response
	}

	// Create the patch, update the suspend field.
	response.Patch, err = json.Marshal([]jsonpatch.Operation{
		{Operation: "replace", Path: "/spec/suspend", Value: true},
	})
	if err != nil {
		return util.ToAdmissionResponse(err)
	}

	response.PatchType = utils.ToPointer(admissionv1.PatchTypeJSONPatch)
	return response
}
