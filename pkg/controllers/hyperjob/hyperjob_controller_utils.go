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

package hyperjob

import (
	"fmt"
	"hash/fnv"
	"reflect"

	policyv1alpha1 "github.com/karmada-io/karmada/pkg/apis/policy/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/rand"
	hashutil "k8s.io/kubernetes/pkg/util/hash"
	batchv1alpha1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	trainingv1alpha1 "volcano.sh/apis/pkg/apis/training/v1alpha1"
)

// Conditions list
const (
	// HyperJobConditionCompleted indicates whether all child VCJobs in the HyperJob have completed successfully
	HyperJobConditionCompleted = "Completed"
	// HyperJobConditionFailed indicates whether any child VCJob has failed, aborted or terminated
	HyperJobConditionFailed = "Failed"
)

// Reasons list
const (
	// HyperJobReasonCompleted indicates that all child VCJobs in the HyperJob have completed successfully
	HyperJobReasonCompleted = "AllChildVCJobsCompleted"
	// HyperJobReasonFailed indicates that some child VCJobs in the HyperJob have failed, aborted or terminated
	HyperJobReasonFailed = "SomeChildVCJobsUnsuccessful"
)

// ComputeVCJobTemplateSpecHash computes the hash value of the vcjob templateSpec.
func ComputeVCJobTemplateSpecHash(templateSpec *batchv1alpha1.JobSpec) string {
	templateSpecHasher := fnv.New64a()
	hashutil.DeepHashObject(templateSpecHasher, *templateSpec)
	return rand.SafeEncodeString(fmt.Sprintf("%08x", templateSpecHasher.Sum64()))
}

// ComputePPSpecHash computes the hash value of the propagation policy spec.
func ComputePPSpecHash(ppSpec *policyv1alpha1.PropagationSpec) string {
	ppSpecHasher := fnv.New64a()
	hashutil.DeepHashObject(ppSpecHasher, *ppSpec)
	return rand.SafeEncodeString(fmt.Sprintf("%08x", ppSpecHasher.Sum64()))
}

// IsVCJobTemplateSpecChanged checks if the vcjob templateSpec has changed by comparing the hash values.
func IsVCJobTemplateSpecChanged(replicatedJob *trainingv1alpha1.ReplicatedJob, existingVCJob *batchv1alpha1.Job) bool {
	currentHash := ComputeVCJobTemplateSpecHash(&replicatedJob.TemplateSpec)

	previousHash, exists := existingVCJob.Labels[VCJobTemplateSpecHashLabelKey]
	if !exists {
		return true
	}

	return currentHash != previousHash
}

// IsPPSpecChanged checks if the propagation policy spec has changed by comparing the hash values.
func IsPPSpecChanged(desiredPP *policyv1alpha1.PropagationPolicy, existingPP *policyv1alpha1.PropagationPolicy) bool {
	currentHash := ComputePPSpecHash(&desiredPP.Spec)

	previousHash, exists := existingPP.Labels[PPSpecHashLabelKey]
	if !exists {
		return true
	}

	return currentHash != previousHash
}

// IsSliceEqual checks if two slices items are equal (using reflect.DeepEqual), ignoring the order of elements.
func IsSliceEqual[T any](oldList, newList []T, getKey func(T) string) bool {
	if len(oldList) != len(newList) {
		return false
	}

	oldMap := make(map[string]T)
	for _, item := range oldList {
		oldMap[getKey(item)] = item
	}

	newMap := make(map[string]T)
	for _, item := range newList {
		newMap[getKey(item)] = item
	}

	return reflect.DeepEqual(oldMap, newMap)
}

// PreserveDefaultsForVCJobSpec preserves fields in an existing VCJob spec that were set by admission webhooks.
//
// Why is this needed?
// 1. A mutating webhook adds default values (e.g., `maxRetry`, `minAvailable`) to VCJobs upon creation.
// 2. An update operation does NOT re-apply these defaults.
// 3. A validating webhook (or the API server) prevents changing some of these fields once set.
//
// This function ensures that if a user doesn't specify these fields in the HyperJob template (`desired`),
// we carry over the values from the `existing` VCJob. This prevents validation errors during an update.
// This function may be simplified or removed if the webhooks are deprecated in the future.
func PreserveDefaultsForVCJobSpec(desired, existing *batchv1alpha1.JobSpec) {
	// Inherit top-level fields from the existing spec if they are not set in the desired spec.
	if desired.Queue == "" && existing.Queue != "" {
		desired.Queue = existing.Queue
	}
	if desired.SchedulerName == "" && existing.SchedulerName != "" {
		desired.SchedulerName = existing.SchedulerName
	}
	if desired.MaxRetry == 0 && existing.MaxRetry > 0 {
		desired.MaxRetry = existing.MaxRetry
	}
	if desired.MinAvailable == 0 && existing.MinAvailable > 0 {
		desired.MinAvailable = existing.MinAvailable
	}

	existingTaskMap := make(map[string]batchv1alpha1.TaskSpec)
	for _, task := range existing.Tasks {
		existingTaskMap[task.Name] = task
	}
	// Inherit task-level fields.
	for i := range desired.Tasks {
		desiredTask := &desired.Tasks[i]
		if existingTask, ok := existingTaskMap[desiredTask.Name]; ok {
			if desiredTask.MaxRetry == 0 && existingTask.MaxRetry > 0 {
				desiredTask.MaxRetry = existingTask.MaxRetry
			}
			if desiredTask.MinAvailable == nil && existingTask.MinAvailable != nil {
				desiredTask.MinAvailable = existingTask.MinAvailable
			}
		}
	}
}

// IsHyperJobInTerminalState checks if the HyperJob is in a terminal state (Completed or Failed).
func IsHyperJobInTerminalState(hyperJob *trainingv1alpha1.HyperJob) bool {
	for _, condition := range hyperJob.Status.Conditions {
		if condition.Type == HyperJobConditionCompleted && condition.Status == metav1.ConditionTrue {
			return true
		}
		if condition.Type == HyperJobConditionFailed && condition.Status == metav1.ConditionTrue {
			return true
		}
	}

	return false
}
