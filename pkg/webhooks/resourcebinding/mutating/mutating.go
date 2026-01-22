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

	"github.com/karmada-io/karmada/pkg/apis/policy/v1alpha1"
	workv1alpha2 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha2"
	"gomodules.xyz/jsonpatch/v2"
	admissionv1 "k8s.io/api/admission/v1"
	registrationv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
	"volcano.sh/volcano/pkg/webhooks/router"
	"volcano.sh/volcano/pkg/webhooks/util"

	"volcano.sh/volcano-global/pkg/dispatcher/api"
	"volcano.sh/volcano-global/pkg/webhooks/decoder"
	"volcano.sh/volcano-global/pkg/workload"
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
	if ar.Request == nil || (ar.Request.Operation != admissionv1.Create && ar.Request.Operation != admissionv1.Update) {
		// This error should not be happened; We have set the rule for CREATE and UPDATE operation.
		return util.ToAdmissionResponse(fmt.Errorf("expect operation to be '%s', but got '%s'", admissionv1.Create+" or "+admissionv1.Update, ar.Request.Operation))
	}
	klog.V(3).Infof("Mutating %s operation for ResourceBinding <%s/%s>.",
		ar.Request.Operation, ar.Request.Namespace, ar.Request.Name)

	rb, err := decoder.DecodeResourceBinding(ar.Request.Object, ar.Request.Resource)
	if err != nil {
		return util.ToAdmissionResponse(err)
	}

	response := &admissionv1.AdmissionResponse{Allowed: true}

	// This should not happen, the `suspend` will update to true by this webhook when create the resourceBinding.
	if rb.Spec.Suspension != nil && rb.Spec.Suspension.Dispatching != nil && *rb.Spec.Suspension.Dispatching {
		klog.V(3).Infof("ResourceBinding <%s/%s> is alrady suspended.", rb.Namespace, rb.Name)
		return response
	}

	// Check if its workload, skip suspend if not.
	isWorkload, _, err := workload.TryGetNewWorkloadFunc(rb.Spec.Resource)
	if err != nil {
		klog.Errorf("Failed to check ResourceBinding <%s/%s> if workload, stop suspend, err: %v",
			rb.Namespace, rb.Name, err)
		return util.ToAdmissionResponse(err)
	}
	if !isWorkload {
		klog.Errorf("ResourceBinding <%s/%s> is not a workload, skip suspend it.",
			rb.Namespace, rb.Name)
		return response
	}

	if rb.Annotations != nil {
		if dc, ok := rb.Annotations[api.LastDispatchedContentAnnotationKey]; ok {
			// Check willDispatchContent is equal LastDispatchedContent, skip suspend if.
			willDispatchContent := &api.DispatchContent{
				ResourceRequest: rb.Spec.ReplicaRequirements.ResourceRequest,
			}
			for _, cluster := range rb.Spec.Clusters {
				willDispatchContent.Replicas += cluster.Replicas
			}
			lastDispatchedContent := &api.DispatchContent{}
			if err = json.Unmarshal([]byte(dc), lastDispatchedContent); err != nil {
				klog.Errorf("Failed to unmarshal ResourceBinding <%s/%s> last dispatched content, err: %v.", rb.Namespace, rb.Name, err)
				return util.ToAdmissionResponse(err)
			}
			if reflect.DeepEqual(lastDispatchedContent, willDispatchContent) {
				klog.V(3).Infof("ResourceBinding <%s/%s> will dispatch content is euqal last dispatched content, skip suspend it.", rb.Namespace, rb.Name)
				return response
			}
			klog.V(3).Infof("ResourceBinding <%s/%s> update first pass volcano-global dispatch.", rb.Namespace, rb.Name)
		}
	}

	// Create the patch, update the suspend field.
	if rb.Spec.Suspension != nil {
		rb.Spec.Suspension.Dispatching = ptr.To(true)
	} else {
		rb.Spec.Suspension = &workv1alpha2.Suspension{
			Suspension: v1alpha1.Suspension{
				Dispatching: ptr.To(true),
			},
		}
	}
	marshaledBytes, err := json.Marshal(rb)
	if err != nil {
		klog.Errorf("Failed to marshal ResourceBinding <%s/%s>, err: %v", rb.Namespace, rb.Name, err)
		return util.ToAdmissionResponse(err)
	}
	patches, err := jsonpatch.CreatePatch(ar.Request.Object.Raw, marshaledBytes)
	if err != nil {
		klog.Errorf("Failed to create patch for ResourceBinding <%s/%s>, err: %v", rb.Namespace, rb.Name, err)
		return util.ToAdmissionResponse(err)
	}
	response.Patch, err = json.Marshal(patches)
	if err != nil {
		klog.Errorf("Failed to marshal ResourceBinding patches <%s/%s>, err: %v", rb.Namespace, rb.Name, err)
		return util.ToAdmissionResponse(err)
	}
	klog.V(3).Infof("Patch content for ResourceBinding <%s/%s> is <%s>.", rb.Namespace, rb.Name, string(response.Patch))
	response.PatchType = ptr.To(admissionv1.PatchTypeJSONPatch)
	return response
}
