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
	"context"
	"fmt"

	policyv1alpha1 "github.com/karmada-io/karmada/pkg/apis/policy/v1alpha1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	batchv1alpha1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	trainingv1alpha1 "volcano.sh/apis/pkg/apis/training/v1alpha1"

	"volcano.sh/volcano-global/pkg/reconciler/scheme"
)

const (
	HyperJobNameLabelKey = "volcano.sh/hyperjob-name"
	// ReplicatedJobNameLabelKey is used to identify which ReplicatedJob a VCJob belongs to.
	// It is convenient for controller to query the replicatedJob a VCJob belongs to and aggregate status
	ReplicatedJobNameLabelKey = "volcano.sh/replicatedjob-name"
	// Labels for storing the hash of the user's original vcjob templateSpec/ppSpec
	VCJobTemplateSpecHashLabelKey = "volcano.sh/vcjob-template-spec-hash"
	PPSpecHashLabelKey            = "volcano.sh/pp-spec-hash"
	ReconcilerName                = "hyperjob"
)

func init() {
	scheme.ReconcilerInitializers[ReconcilerName] = InitHyperJobController
}

// HyperJobController creates the corresponding number of vcjob and pp based on HyperJob, and aggregates the status of child vcjobs to HyperJob
type HyperJobController struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

func InitHyperJobController(mgr ctrl.Manager) error {
	recorder := mgr.GetEventRecorderFor(ReconcilerName)
	reconciler := NewHyperJobController(mgr.GetClient(), mgr.GetScheme(), recorder)
	return reconciler.SetupWithManager(mgr)
}

func NewHyperJobController(client client.Client, scheme *runtime.Scheme, recorder record.EventRecorder) *HyperJobController {
	return &HyperJobController{
		Client:   client,
		Scheme:   scheme,
		Recorder: recorder,
	}
}

// SetupWithManager sets up the controller with the Manager.
func (h *HyperJobController) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&trainingv1alpha1.HyperJob{}).
		Owns(&batchv1alpha1.Job{}).
		Owns(&policyv1alpha1.PropagationPolicy{}).
		Complete(h)
}

func (h *HyperJobController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)

	hyperJob := &trainingv1alpha1.HyperJob{}
	if err := h.Get(ctx, types.NamespacedName{Name: req.Name, Namespace: req.Namespace}, hyperJob); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if hyperJob.DeletionTimestamp != nil {
		// kube-controller-manager's gc-controller is responsible for deleting VCJobs and PPs
		return ctrl.Result{}, nil
	}

	splitCount, err := h.syncVCJobAndPP(ctx, hyperJob)
	if err != nil {
		log.Error(err, "Failed to sync VCJob and PropagationPolicy for HyperJob")
		return ctrl.Result{}, err
	}

	if err = h.syncHyperJobStatus(ctx, hyperJob, splitCount); err != nil {
		log.Error(err, "Failed to sync VCJob status for HyperJob")
		return ctrl.Result{}, err
	}

	log.V(4).Info("Successfully synced VCJob and PropagationPolicy for HyperJob", "SplitCount", splitCount)

	return ctrl.Result{}, nil
}

// syncVCJobAndPP ensures the desired number of VCJobs and PropagationPolicies are created/updated/deleted according to the HyperJob spec, and returns the total number of splits.
func (h *HyperJobController) syncVCJobAndPP(ctx context.Context, hyperJob *trainingv1alpha1.HyperJob) (splitCount int32, err error) {
	log := ctrl.LoggerFrom(ctx)

	childVCJobs := &batchv1alpha1.JobList{}
	selector := client.MatchingLabels(map[string]string{
		HyperJobNameLabelKey: hyperJob.Name,
	})
	if err = h.List(ctx, childVCJobs, client.InNamespace(hyperJob.Namespace), selector); err != nil {
		log.Error(err, "Failed to list child VCJobs for HyperJob")
		return 0, err
	}
	childVCJobMap := make(map[string]batchv1alpha1.Job)
	for _, job := range childVCJobs.Items {
		if metav1.IsControlledBy(&job, hyperJob) {
			childVCJobMap[job.Name] = job
		}
	}

	childPPs := &policyv1alpha1.PropagationPolicyList{}
	if err = h.List(ctx, childPPs, client.InNamespace(hyperJob.Namespace), selector); err != nil {
		log.Error(err, "Failed to list child PropagationPolicies for HyperJob")
		return 0, err
	}
	childPPMap := make(map[string]policyv1alpha1.PropagationPolicy)
	for _, pp := range childPPs.Items {
		if metav1.IsControlledBy(&pp, hyperJob) {
			childPPMap[pp.Name] = pp
		}
	}

	// Static splitting: create the specified number of vcjobs and pps according to the Replicas field of each ReplicatedJob
	for _, replicatedJob := range hyperJob.Spec.ReplicatedJobs {
		splitCount += replicatedJob.Replicas
		for i := 0; i < int(replicatedJob.Replicas); i++ {
			jobName := fmt.Sprintf("%s-%s-%d", hyperJob.Name, replicatedJob.Name, i)
			ppName := jobName

			// Reconcile VCJobs
			desiredVCJob, err := h.constructDesiredVCJob(hyperJob, &replicatedJob, jobName)
			if err != nil {
				log.Error(err, "Failed to construct desired VCJob for HyperJob")
				return 0, err
			}

			if existingVCJob, exists := childVCJobMap[jobName]; !exists {
				if err = h.Create(ctx, desiredVCJob); err != nil {
					h.Recorder.Eventf(hyperJob, "Warning", "FailedCreateVCJob",
						"Failed to create VCJob %s: %v", desiredVCJob.Name, err)
					log.Error(err, "Failed to create VolcanoJob", "VCJob.Name", desiredVCJob.Name, "VCJob.Namespace", desiredVCJob.Namespace)
					return 0, err
				}
				log.V(4).Info("Successfully created a new VCJob", "VCJob.Name", jobName, "VCJob.Namespace", desiredVCJob.Namespace)
				h.Recorder.Eventf(hyperJob, "Normal", "CreatedVCJob",
					"Successfully created VCJob %s", desiredVCJob.Name)
			} else {
				if IsVCJobTemplateSpecChanged(&replicatedJob, &existingVCJob) {
					// Preserve webhook-set default values from the existing job spec.
					PreserveDefaultsForVCJobSpec(&desiredVCJob.Spec, &existingVCJob.Spec)

					log.V(4).Info("Updating existing VolcanoJob", "VCJob.Name", jobName, "VCJob.Namespace", desiredVCJob.Namespace)
					existingVCJob.Spec = desiredVCJob.Spec
					if existingVCJob.Labels != nil {
						existingVCJob.Labels[VCJobTemplateSpecHashLabelKey] = desiredVCJob.Labels[VCJobTemplateSpecHashLabelKey]
					}
					if err = h.Update(ctx, &existingVCJob); err != nil {
						h.Recorder.Eventf(hyperJob, "Warning", "FailedUpdateVCJob",
							"Failed to update VCJob %s: %v", existingVCJob.Name, err)
						log.Error(err, "Failed to update VolcanoJob", "VCJob.Name", existingVCJob.Name, "VCJob.Namespace", existingVCJob.Namespace)
						return 0, err
					}
				}
				// Delete from map to mark as processed, then jobs left in the map are stale that need to be deleted
				delete(childVCJobMap, jobName)
			}

			// Reconcile PropagationPolicies
			desiredPP, err := h.constructDesiredPP(hyperJob, &replicatedJob, ppName)
			if err != nil {
				log.Error(err, "Failed to construct desired PropagationPolicy for HyperJob")
				return 0, err
			}

			if existingPP, exists := childPPMap[ppName]; !exists {
				if err = h.Create(ctx, desiredPP); err != nil {
					h.Recorder.Eventf(hyperJob, "Warning", "FailedCreatePP",
						"Failed to create PropagationPolicy %s: %v", desiredPP.Name, err)
					log.Error(err, "Failed to create PropagationPolicy", "PP.Name", desiredPP.Name, "PP.Namespace", desiredPP.Namespace)
					return 0, err
				}
				log.V(4).Info("Successfully created a new PropagationPolicy", "PP.Name", ppName, "PP.Namespace", desiredPP.Namespace)
				h.Recorder.Eventf(hyperJob, "Normal", "CreatedPP",
					"Successfully created PropagationPolicy %s", desiredPP.Name)
			} else {
				if IsPPSpecChanged(desiredPP, &existingPP) {
					log.V(4).Info("Updating existing PropagationPolicy", "PP.Name", ppName, "PP.Namespace", desiredPP.Namespace)
					existingPP.Spec = desiredPP.Spec
					if existingPP.Labels != nil {
						existingPP.Labels[PPSpecHashLabelKey] = desiredPP.Labels[PPSpecHashLabelKey]
					}
					if err = h.Update(ctx, &existingPP); err != nil {
						h.Recorder.Eventf(hyperJob, "Warning", "FailedUpdatePP",
							"Failed to update PropagationPolicy %s: %v", existingPP.Name, err)
						log.Error(err, "Failed to update PropagationPolicy", "PP.Name", existingPP.Name, "PP.Namespace", existingPP.Namespace)
						return 0, err
					}
				}
				// Delete from map to mark as processed, then PPs left in the map are stale that need to be deleted
				delete(childPPMap, ppName)
			}

		}
	}

	for _, staleJob := range childVCJobMap {
		log.V(4).Info("Deleting stale VolcanoJob", "VCJob.Name", staleJob.Name, "VCJob.Namespace", staleJob.Namespace)
		if err := h.Delete(ctx, &staleJob); err != nil {
			h.Recorder.Eventf(hyperJob, "Warning", "FailedDeleteVCJob",
				"Failed to delete stale VCJob %s: %v", staleJob.Name, err)
			log.Error(err, "Failed to delete stale VolcanoJob", "VCJob.Name", staleJob.Name)
			return 0, err
		}
	}
	for _, stalePP := range childPPMap {
		log.V(4).Info("Deleting stale PropagationPolicy", "PP.Name", stalePP.Name, "PP.Namespace", stalePP.Namespace)
		if err := h.Delete(ctx, &stalePP); err != nil {
			h.Recorder.Eventf(hyperJob, "Warning", "FailedDeletePP",
				"Failed to delete stale PropagationPolicy %s: %v", stalePP.Name, err)
			log.Error(err, "Failed to delete stale PropagationPolicy", "PP.Name", stalePP.Name)
			return 0, err
		}
	}

	return splitCount, nil
}

func (h *HyperJobController) constructDesiredVCJob(hyperJob *trainingv1alpha1.HyperJob, replicatedJob *trainingv1alpha1.ReplicatedJob, jobName string) (*batchv1alpha1.Job, error) {
	templateSpecHash := ComputeVCJobTemplateSpecHash(&replicatedJob.TemplateSpec)

	desiredVCJob := &batchv1alpha1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: hyperJob.Namespace,
			Labels: map[string]string{
				HyperJobNameLabelKey:          hyperJob.Name,
				ReplicatedJobNameLabelKey:     replicatedJob.Name,
				VCJobTemplateSpecHashLabelKey: templateSpecHash,
			},
			Annotations: make(map[string]string),
		},
		Spec: replicatedJob.TemplateSpec,
	}

	if err := controllerutil.SetControllerReference(hyperJob, desiredVCJob, h.Scheme); err != nil {
		return nil, err
	}

	return desiredVCJob, nil
}

func (h *HyperJobController) constructDesiredPP(hyperJob *trainingv1alpha1.HyperJob, replicatedJob *trainingv1alpha1.ReplicatedJob, ppName string) (*policyv1alpha1.PropagationPolicy, error) {
	desiredPP := &policyv1alpha1.PropagationPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ppName,
			Namespace: hyperJob.Namespace,
			Labels: map[string]string{
				HyperJobNameLabelKey: hyperJob.Name,
			},
			Annotations: make(map[string]string),
		},
		Spec: policyv1alpha1.PropagationSpec{
			PropagateDeps: true,
			ResourceSelectors: []policyv1alpha1.ResourceSelector{
				{
					APIVersion: batchv1alpha1.SchemeGroupVersion.String(),
					Kind:       "Job",
					Name:       ppName, // jobName is the same as ppName
				},
			},
			Placement: policyv1alpha1.Placement{
				ReplicaScheduling: &policyv1alpha1.ReplicaSchedulingStrategy{
					ReplicaSchedulingType:     policyv1alpha1.ReplicaSchedulingTypeDivided,
					ReplicaDivisionPreference: policyv1alpha1.ReplicaDivisionPreferenceAggregated,
				},
				SpreadConstraints: []policyv1alpha1.SpreadConstraint{
					{
						SpreadByField: policyv1alpha1.SpreadByFieldCluster,
						MinGroups:     1,
						MaxGroups:     1,
					},
				},
			},
		},
	}

	if replicatedJob.ClusterNames != nil {
		desiredPP.Spec.Placement.ClusterAffinity = &policyv1alpha1.ClusterAffinity{
			ClusterNames: replicatedJob.ClusterNames,
		}
	}

	ppSpecHash := ComputePPSpecHash(&desiredPP.Spec)
	desiredPP.Labels[PPSpecHashLabelKey] = ppSpecHash

	if err := controllerutil.SetControllerReference(hyperJob, desiredPP, h.Scheme); err != nil {
		return nil, err
	}

	return desiredPP, nil
}

// syncHyperJobStatus aggregates the status of child VCJobs and updates the HyperJob status accordingly.
func (h *HyperJobController) syncHyperJobStatus(ctx context.Context, hyperJob *trainingv1alpha1.HyperJob, splitCount int32) error {
	log := ctrl.LoggerFrom(ctx)

	if IsHyperJobInTerminalState(hyperJob) {
		log.V(4).Info("HyperJob is already in terminal state, skipping status sync")
		return nil
	}

	var newReplicatedJobsStatus []trainingv1alpha1.ReplicatedJobStatus
	for _, replicatedJob := range hyperJob.Spec.ReplicatedJobs {
		childVCJobs := &batchv1alpha1.JobList{}
		selector := client.MatchingLabels(map[string]string{
			HyperJobNameLabelKey:      hyperJob.Name,
			ReplicatedJobNameLabelKey: replicatedJob.Name,
		})
		if err := h.List(ctx, childVCJobs, client.InNamespace(hyperJob.Namespace), selector); err != nil {
			log.Error(err, "Failed to list child VCJobs for HyperJob")
			return err
		}
		newReplicatedJobStatus := trainingv1alpha1.ReplicatedJobStatus{
			Name:      replicatedJob.Name,
			JobStates: make(map[string]batchv1alpha1.JobState),
		}
		for _, job := range childVCJobs.Items {
			newReplicatedJobStatus.JobStates[job.Name] = job.Status.State
			newReplicatedJobStatus.Pending += job.Status.Pending
			newReplicatedJobStatus.Running += job.Status.Running
			newReplicatedJobStatus.Succeeded += job.Status.Succeeded
			newReplicatedJobStatus.Failed += job.Status.Failed
			newReplicatedJobStatus.Terminating += job.Status.Terminating
			newReplicatedJobStatus.Unknown += job.Status.Unknown
		}
		newReplicatedJobsStatus = append(newReplicatedJobsStatus, newReplicatedJobStatus)
	}

	// Create a deep copy only if we need to make changes
	var updatedHyperJob *trainingv1alpha1.HyperJob
	var statusChanged bool

	// 1. Update ReplicatedJobsStatus if changed
	if !IsSliceEqual(hyperJob.Status.ReplicatedJobsStatus, newReplicatedJobsStatus, func(item trainingv1alpha1.ReplicatedJobStatus) string {
		return item.Name
	}) {
		updatedHyperJob = hyperJob.DeepCopy()
		updatedHyperJob.Status.ReplicatedJobsStatus = newReplicatedJobsStatus
		statusChanged = true
	}

	// 2. Update Conditions if changed
	newConditions := h.constructConditions(newReplicatedJobsStatus, splitCount)
	if len(newConditions) > 0 {
		if updatedHyperJob == nil {
			updatedHyperJob = hyperJob.DeepCopy()
		}

		for _, newCondition := range newConditions {
			if meta.SetStatusCondition(&updatedHyperJob.Status.Conditions, newCondition) {
				statusChanged = true
			}
		}
	}

	// 3. Update SplitCount if changed
	if hyperJob.Status.SplitCount == nil || *hyperJob.Status.SplitCount != splitCount {
		if updatedHyperJob == nil {
			updatedHyperJob = hyperJob.DeepCopy()
		}
		updatedHyperJob.Status.SplitCount = &splitCount
		statusChanged = true
	}

	// 4. Update ObservedGeneration if changed
	if hyperJob.Generation != hyperJob.Status.ObservedGeneration {
		if updatedHyperJob == nil {
			updatedHyperJob = hyperJob.DeepCopy()
		}
		updatedHyperJob.Status.ObservedGeneration = hyperJob.Generation
		statusChanged = true
	}

	if statusChanged {
		if err := h.Status().Update(ctx, updatedHyperJob); err != nil {
			log.Error(err, "Failed to update HyperJob status")
			return err
		}
		log.V(4).Info("Successfully updated HyperJob status")
	}

	return nil
}

func (h *HyperJobController) constructConditions(replicatedJobStatus []trainingv1alpha1.ReplicatedJobStatus, splitCount int32) []metav1.Condition {
	if len(replicatedJobStatus) == 0 {
		return nil
	}

	var completed, failed, aborted, terminated, running, pending, other int32

	for _, status := range replicatedJobStatus {
		for _, jobState := range status.JobStates {
			switch jobState.Phase {
			case batchv1alpha1.Completed:
				completed++
			case batchv1alpha1.Failed:
				failed++
			case batchv1alpha1.Aborted:
				aborted++
			case batchv1alpha1.Terminated:
				terminated++
			case batchv1alpha1.Running:
				running++
			case batchv1alpha1.Pending:
				pending++
			default:
				// Aborting, Restarting, Completing, Terminating
				other++
			}
		}
	}

	finishedJobs := completed + failed + aborted + terminated
	// If not all jobs are finished, the HyperJob is still in progress.
	// Following the K8s Job pattern, we don't set any terminal condition yet.
	if finishedJobs != splitCount {
		return nil
	}

	// The HyperJob has reached a terminal state. We add one condition, either Completed or Failed.
	if failed+aborted+terminated > 0 {
		condition := metav1.Condition{
			Type:               HyperJobConditionFailed,
			Status:             metav1.ConditionTrue,
			LastTransitionTime: metav1.Now(),
			Reason:             HyperJobReasonFailed,
			Message: fmt.Sprintf("HyperJob failed: %d completed, %d failed, %d aborted, %d terminated",
				completed, failed, aborted, terminated),
		}
		return []metav1.Condition{condition}
	}

	condition := metav1.Condition{
		Type:               HyperJobConditionCompleted,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             HyperJobReasonCompleted,
		Message:            fmt.Sprintf("All %d child vcJobs completed", splitCount),
	}
	return []metav1.Condition{condition}
}
