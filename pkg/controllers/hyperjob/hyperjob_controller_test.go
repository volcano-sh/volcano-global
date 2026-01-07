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
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	policyv1alpha1 "github.com/karmada-io/karmada/pkg/apis/policy/v1alpha1"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	batchv1alpha1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	trainingv1alpha1 "volcano.sh/apis/pkg/apis/training/v1alpha1"
)

func setupTestController() (*HyperJobController, client.Client) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = trainingv1alpha1.AddToScheme(scheme)
	_ = batchv1alpha1.AddToScheme(scheme)
	_ = policyv1alpha1.AddToScheme(scheme)

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&trainingv1alpha1.HyperJob{}).
		Build()

	eventRecorder := record.NewFakeRecorder(100)
	controller := NewHyperJobController(fakeClient, scheme, eventRecorder)

	return controller, fakeClient
}

func createTestHyperJob(name, namespace string, replicatedJobs []trainingv1alpha1.ReplicatedJob) *trainingv1alpha1.HyperJob {
	return &trainingv1alpha1.HyperJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:       name,
			Namespace:  namespace,
			Generation: 1,
			UID:        "12345678-1234-1234-1234-123456789abc",
		},
		Spec: trainingv1alpha1.HyperJobSpec{
			ReplicatedJobs: replicatedJobs,
		},
	}
}

func createTestReplicatedJob(name string, replicas int32, clusterNames []string) trainingv1alpha1.ReplicatedJob {
	return trainingv1alpha1.ReplicatedJob{
		Name:         name,
		Replicas:     replicas,
		ClusterNames: clusterNames,
		TemplateSpec: batchv1alpha1.JobSpec{
			Tasks: []batchv1alpha1.TaskSpec{
				{
					Name:     "worker",
					Replicas: 1,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:  "worker",
									Image: "test-image",
									Resources: corev1.ResourceRequirements{
										Requests: corev1.ResourceList{
											corev1.ResourceCPU:    resource.MustParse("100m"),
											corev1.ResourceMemory: resource.MustParse("128Mi"),
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func createTestVCJob(name, namespace, hyperJobName, replicatedJobName string, status batchv1alpha1.JobStatus) *batchv1alpha1.Job {
	return &batchv1alpha1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				HyperJobNameLabelKey:      hyperJobName,
				ReplicatedJobNameLabelKey: replicatedJobName,
			},
		},
		Status: status,
	}
}

// Helper to create expected VCJob
func expectedVCJob(name, namespace, hyperJobName, replicatedJobName string, spec batchv1alpha1.JobSpec) *batchv1alpha1.Job {
	templateSpecHash := ComputeVCJobTemplateSpecHash(&spec)

	return &batchv1alpha1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				HyperJobNameLabelKey:          hyperJobName,
				ReplicatedJobNameLabelKey:     replicatedJobName,
				VCJobTemplateSpecHashLabelKey: templateSpecHash,
			},
		},
		Spec: spec,
	}
}

// Helper to create expected PropagationPolicy
func expectedPP(name, namespace, hyperJobName string, clusterNames []string) *policyv1alpha1.PropagationPolicy {
	pp := &policyv1alpha1.PropagationPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				HyperJobNameLabelKey: hyperJobName,
			},
		},
		Spec: policyv1alpha1.PropagationSpec{
			PropagateDeps: true,
			ResourceSelectors: []policyv1alpha1.ResourceSelector{
				{
					APIVersion: batchv1alpha1.SchemeGroupVersion.String(),
					Kind:       "Job",
					Name:       name,
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

	if clusterNames != nil {
		pp.Spec.Placement.ClusterAffinity = &policyv1alpha1.ClusterAffinity{
			ClusterNames: clusterNames,
		}
	}

	ppSpecHash := ComputePPSpecHash(&pp.Spec)
	pp.Labels[PPSpecHashLabelKey] = ppSpecHash

	return pp
}

func TestSyncVCJobAndPP(t *testing.T) {
	namespace := "test-ns"
	specA := createTestReplicatedJob("worker-a", 2, []string{"cluster-1"}).TemplateSpec
	specB := createTestReplicatedJob("worker-b", 1, []string{"cluster-2"}).TemplateSpec

	// specC is a modification of specA (different image) to test template updates.
	specC := specA.DeepCopy()
	specC.Tasks[0].Template.Spec.Containers[0].Image = "new-test-image"

	tests := []struct {
		name               string
		hyperJob           *trainingv1alpha1.HyperJob
		existingVCJobs     []*batchv1alpha1.Job
		existingPPs        []*policyv1alpha1.PropagationPolicy
		expectedVCJobs     []*batchv1alpha1.Job
		expectedPPs        []*policyv1alpha1.PropagationPolicy
		expectedSplitCount int32
	}{
		{
			name: "Create: new HyperJob with two ReplicatedJobs",
			hyperJob: createTestHyperJob("test-create", namespace, []trainingv1alpha1.ReplicatedJob{
				{Name: "worker-a", Replicas: 2, TemplateSpec: specA, ClusterNames: []string{"cluster-1"}},
				{Name: "worker-b", Replicas: 1, TemplateSpec: specB, ClusterNames: []string{"cluster-2"}},
			}),
			existingVCJobs: []*batchv1alpha1.Job{},
			existingPPs:    []*policyv1alpha1.PropagationPolicy{},
			expectedVCJobs: []*batchv1alpha1.Job{
				expectedVCJob("test-create-worker-a-0", namespace, "test-create", "worker-a", specA),
				expectedVCJob("test-create-worker-a-1", namespace, "test-create", "worker-a", specA),
				expectedVCJob("test-create-worker-b-0", namespace, "test-create", "worker-b", specB),
			},
			expectedPPs: []*policyv1alpha1.PropagationPolicy{
				expectedPP("test-create-worker-a-0", namespace, "test-create", []string{"cluster-1"}),
				expectedPP("test-create-worker-a-1", namespace, "test-create", []string{"cluster-1"}),
				expectedPP("test-create-worker-b-0", namespace, "test-create", []string{"cluster-2"}),
			},
			expectedSplitCount: 3,
		},
		{
			name: "Update: scale up a ReplicatedJob",
			hyperJob: createTestHyperJob("test-update", namespace, []trainingv1alpha1.ReplicatedJob{
				{Name: "worker-a", Replicas: 3, TemplateSpec: specA, ClusterNames: []string{"cluster-1"}}, // Scaled up
				{Name: "worker-b", Replicas: 1, TemplateSpec: specB, ClusterNames: []string{"cluster-2"}},
			}),
			existingVCJobs: []*batchv1alpha1.Job{
				expectedVCJob("test-update-worker-a-0", namespace, "test-update", "worker-a", specA),
				expectedVCJob("test-update-worker-a-1", namespace, "test-update", "worker-a", specA),
				expectedVCJob("test-update-worker-b-0", namespace, "test-update", "worker-b", specB),
			},
			existingPPs: []*policyv1alpha1.PropagationPolicy{
				expectedPP("test-update-worker-a-0", namespace, "test-update", []string{"cluster-1"}),
				expectedPP("test-update-worker-a-1", namespace, "test-update", []string{"cluster-1"}),
				expectedPP("test-update-worker-b-0", namespace, "test-update", []string{"cluster-2"}),
			},
			expectedVCJobs: []*batchv1alpha1.Job{
				expectedVCJob("test-update-worker-a-0", namespace, "test-update", "worker-a", specA),
				expectedVCJob("test-update-worker-a-1", namespace, "test-update", "worker-a", specA),
				expectedVCJob("test-update-worker-a-2", namespace, "test-update", "worker-a", specA),
				expectedVCJob("test-update-worker-b-0", namespace, "test-update", "worker-b", specB),
			},
			expectedPPs: []*policyv1alpha1.PropagationPolicy{
				expectedPP("test-update-worker-a-0", namespace, "test-update", []string{"cluster-1"}),
				expectedPP("test-update-worker-a-1", namespace, "test-update", []string{"cluster-1"}),
				expectedPP("test-update-worker-a-2", namespace, "test-update", []string{"cluster-1"}),
				expectedPP("test-update-worker-b-0", namespace, "test-update", []string{"cluster-2"}),
			},
			expectedSplitCount: 4,
		},
		{
			name: "Update: template spec changed",
			hyperJob: createTestHyperJob("test-spec-update", namespace, []trainingv1alpha1.ReplicatedJob{
				{Name: "worker-a", Replicas: 2, TemplateSpec: *specC, ClusterNames: []string{"cluster-1"}}, // Spec updated
				{Name: "worker-b", Replicas: 1, TemplateSpec: specB, ClusterNames: []string{"cluster-2"}},
			}),
			existingVCJobs: []*batchv1alpha1.Job{
				// These jobs were created with the old spec (specA)
				expectedVCJob("test-spec-update-worker-a-0", namespace, "test-spec-update", "worker-a", specA),
				expectedVCJob("test-spec-update-worker-a-1", namespace, "test-spec-update", "worker-a", specA),
				expectedVCJob("test-spec-update-worker-b-0", namespace, "test-spec-update", "worker-b", specB),
			},
			existingPPs: []*policyv1alpha1.PropagationPolicy{
				expectedPP("test-spec-update-worker-a-0", namespace, "test-spec-update", []string{"cluster-1"}),
				expectedPP("test-spec-update-worker-a-1", namespace, "test-spec-update", []string{"cluster-1"}),
				expectedPP("test-spec-update-worker-b-0", namespace, "test-spec-update", []string{"cluster-2"}),
			},
			expectedVCJobs: []*batchv1alpha1.Job{
				// The worker-a jobs should now have the new spec (specC)
				expectedVCJob("test-spec-update-worker-a-0", namespace, "test-spec-update", "worker-a", *specC),
				expectedVCJob("test-spec-update-worker-a-1", namespace, "test-spec-update", "worker-a", *specC),
				expectedVCJob("test-spec-update-worker-b-0", namespace, "test-spec-update", "worker-b", specB),
			},
			// PPs are not affected by template spec changes
			expectedPPs: []*policyv1alpha1.PropagationPolicy{
				expectedPP("test-spec-update-worker-a-0", namespace, "test-spec-update", []string{"cluster-1"}),
				expectedPP("test-spec-update-worker-a-1", namespace, "test-spec-update", []string{"cluster-1"}),
				expectedPP("test-spec-update-worker-b-0", namespace, "test-spec-update", []string{"cluster-2"}),
			},
			expectedSplitCount: 3,
		},
		{
			name: "Delete: scale down a ReplicatedJob",
			hyperJob: createTestHyperJob("test-delete", namespace, []trainingv1alpha1.ReplicatedJob{
				{Name: "worker-a", Replicas: 1, TemplateSpec: specA, ClusterNames: []string{"cluster-1"}}, // Scaled down
				{Name: "worker-b", Replicas: 1, TemplateSpec: specB, ClusterNames: []string{"cluster-2"}},
			}),
			existingVCJobs: []*batchv1alpha1.Job{
				expectedVCJob("test-delete-worker-a-0", namespace, "test-delete", "worker-a", specA),
				expectedVCJob("test-delete-worker-a-1", namespace, "test-delete", "worker-a", specA),
				expectedVCJob("test-delete-worker-b-0", namespace, "test-delete", "worker-b", specB),
			},
			existingPPs: []*policyv1alpha1.PropagationPolicy{
				expectedPP("test-delete-worker-a-0", namespace, "test-delete", []string{"cluster-1"}),
				expectedPP("test-delete-worker-a-1", namespace, "test-delete", []string{"cluster-1"}),
				expectedPP("test-delete-worker-b-0", namespace, "test-delete", []string{"cluster-2"}),
			},
			expectedVCJobs: []*batchv1alpha1.Job{
				expectedVCJob("test-delete-worker-a-0", namespace, "test-delete", "worker-a", specA),
				expectedVCJob("test-delete-worker-b-0", namespace, "test-delete", "worker-b", specB),
			},
			expectedPPs: []*policyv1alpha1.PropagationPolicy{
				expectedPP("test-delete-worker-a-0", namespace, "test-delete", []string{"cluster-1"}),
				expectedPP("test-delete-worker-b-0", namespace, "test-delete", []string{"cluster-2"}),
			},
			expectedSplitCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			controller, fakeClient := setupTestController()
			ctx := context.Background()

			err := fakeClient.Create(ctx, tt.hyperJob)
			assert.NoError(t, err)

			for _, vcjob := range tt.existingVCJobs {
				// We need to manually set these because the fake client doesn't run the controller logic
				// to add owner references for existing objects.
				vcjob.OwnerReferences = []metav1.OwnerReference{
					*metav1.NewControllerRef(tt.hyperJob, trainingv1alpha1.SchemeGroupVersion.WithKind("HyperJob")),
				}
				err = fakeClient.Create(ctx, vcjob)
				assert.NoError(t, err)
			}
			for _, pp := range tt.existingPPs {
				pp.OwnerReferences = []metav1.OwnerReference{
					*metav1.NewControllerRef(tt.hyperJob, trainingv1alpha1.SchemeGroupVersion.WithKind("HyperJob")),
				}
				err = fakeClient.Create(ctx, pp)
				assert.NoError(t, err)
			}

			splitCount, err := controller.syncVCJobAndPP(ctx, tt.hyperJob)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedSplitCount, splitCount)

			vcJobList := &batchv1alpha1.JobList{}
			err = fakeClient.List(ctx, vcJobList, client.InNamespace(tt.hyperJob.Namespace),
				client.MatchingLabels{HyperJobNameLabelKey: tt.hyperJob.Name})
			assert.NoError(t, err)

			actualVCJobs := make([]*batchv1alpha1.Job, len(vcJobList.Items))
			for i := range vcJobList.Items {
				actualVCJobs[i] = &vcJobList.Items[i]
			}

			// Manually add controller-managed fields to our expected objects for a fair comparison.
			for _, vcjob := range tt.expectedVCJobs {
				vcjob.OwnerReferences = []metav1.OwnerReference{
					*metav1.NewControllerRef(tt.hyperJob, trainingv1alpha1.SchemeGroupVersion.WithKind("HyperJob")),
				}
			}
			for _, pp := range tt.expectedPPs {
				pp.OwnerReferences = []metav1.OwnerReference{
					*metav1.NewControllerRef(tt.hyperJob, trainingv1alpha1.SchemeGroupVersion.WithKind("HyperJob")),
				}
			}

			// Sort slices for consistent comparison
			sort.Slice(tt.expectedVCJobs, func(i, j int) bool { return tt.expectedVCJobs[i].Name < tt.expectedVCJobs[j].Name })
			sort.Slice(actualVCJobs, func(i, j int) bool { return actualVCJobs[i].Name < actualVCJobs[j].Name })

			if diff := cmp.Diff(tt.expectedVCJobs, actualVCJobs, cmpopts.IgnoreFields(metav1.ObjectMeta{}, "ResourceVersion")); diff != "" {
				t.Errorf("VCJobs mismatch (-expected +actual):\n%s", diff)
			}

			ppList := &policyv1alpha1.PropagationPolicyList{}
			err = fakeClient.List(ctx, ppList, client.InNamespace(tt.hyperJob.Namespace),
				client.MatchingLabels{HyperJobNameLabelKey: tt.hyperJob.Name})
			assert.NoError(t, err)

			actualPPs := make([]*policyv1alpha1.PropagationPolicy, len(ppList.Items))
			for i := range ppList.Items {
				actualPPs[i] = &ppList.Items[i]
			}

			// Sort slices for consistent comparison
			sort.Slice(tt.expectedPPs, func(i, j int) bool { return tt.expectedPPs[i].Name < tt.expectedPPs[j].Name })
			sort.Slice(actualPPs, func(i, j int) bool { return actualPPs[i].Name < actualPPs[j].Name })

			if diff := cmp.Diff(tt.expectedPPs, actualPPs, cmpopts.IgnoreFields(metav1.ObjectMeta{}, "ResourceVersion")); diff != "" {
				t.Errorf("PropagationPolicies mismatch (-expected +actual):\n%s", diff)
			}
		})
	}
}

func TestSyncHyperJobStatus(t *testing.T) {
	namespace := "test-ns"
	specA := createTestReplicatedJob("worker-a", 2, []string{"cluster-1"}).TemplateSpec

	tests := []struct {
		name                       string
		hyperJob                   *trainingv1alpha1.HyperJob
		existingVCJobs             []*batchv1alpha1.Job
		splitCount                 int32
		expectedReplicatedJobCount int
		expectedCondition          *metav1.Condition
		expectedRunning            int32
		expectedSucceeded          int32
		expectedFailed             int32
	}{
		{
			name: "in progress: one running, one completed",
			hyperJob: createTestHyperJob("status-test", namespace, []trainingv1alpha1.ReplicatedJob{
				{Name: "worker", Replicas: 2, TemplateSpec: specA},
			}),
			existingVCJobs: []*batchv1alpha1.Job{
				createTestVCJob("status-test-worker-0", namespace, "status-test", "worker", batchv1alpha1.JobStatus{
					State:   batchv1alpha1.JobState{Phase: batchv1alpha1.Running},
					Running: 2,
				}),
				createTestVCJob("status-test-worker-1", namespace, "status-test", "worker", batchv1alpha1.JobStatus{
					State:     batchv1alpha1.JobState{Phase: batchv1alpha1.Completed},
					Succeeded: 2,
				}),
			},
			splitCount:                 2,
			expectedReplicatedJobCount: 1,
			expectedCondition:          nil, // In-progress jobs will not have a terminal condition
			expectedRunning:            2,
			expectedSucceeded:          2,
			expectedFailed:             0,
		},
		{
			name: "succeeded: all vcjobs completed successfully",
			hyperJob: createTestHyperJob("all-completed", namespace, []trainingv1alpha1.ReplicatedJob{
				{Name: "worker", Replicas: 2, TemplateSpec: specA},
			}),
			existingVCJobs: []*batchv1alpha1.Job{
				createTestVCJob("all-completed-worker-0", namespace, "all-completed", "worker", batchv1alpha1.JobStatus{
					State:     batchv1alpha1.JobState{Phase: batchv1alpha1.Completed},
					Succeeded: 2,
				}),
				createTestVCJob("all-completed-worker-1", namespace, "all-completed", "worker", batchv1alpha1.JobStatus{
					State:     batchv1alpha1.JobState{Phase: batchv1alpha1.Completed},
					Succeeded: 2,
				}),
			},
			splitCount:                 2,
			expectedReplicatedJobCount: 1,
			expectedCondition: &metav1.Condition{
				Type:   HyperJobConditionCompleted,
				Status: metav1.ConditionTrue,
				Reason: HyperJobReasonCompleted,
			},
			expectedRunning:   0,
			expectedSucceeded: 4,
			expectedFailed:    0,
		},
		{
			name: "failed: one job failed",
			hyperJob: createTestHyperJob("one-failed", namespace, []trainingv1alpha1.ReplicatedJob{
				{Name: "worker", Replicas: 2, TemplateSpec: specA},
			}),
			existingVCJobs: []*batchv1alpha1.Job{
				createTestVCJob("one-failed-worker-0", namespace, "one-failed", "worker", batchv1alpha1.JobStatus{
					State:     batchv1alpha1.JobState{Phase: batchv1alpha1.Completed},
					Succeeded: 2,
				}),
				createTestVCJob("one-failed-worker-1", namespace, "one-failed", "worker", batchv1alpha1.JobStatus{
					State:  batchv1alpha1.JobState{Phase: batchv1alpha1.Failed},
					Failed: 1,
				}),
			},
			splitCount:                 2,
			expectedReplicatedJobCount: 1,
			expectedCondition: &metav1.Condition{
				Type:   HyperJobConditionFailed,
				Status: metav1.ConditionTrue,
				Reason: HyperJobReasonFailed,
			},
			expectedRunning:   0,
			expectedSucceeded: 2,
			expectedFailed:    1,
		},
		{
			name: "in progress: no child vcjobs created yet",
			hyperJob: createTestHyperJob("no-vcjobs", namespace, []trainingv1alpha1.ReplicatedJob{
				{Name: "worker", Replicas: 1, TemplateSpec: specA},
			}),
			existingVCJobs:             []*batchv1alpha1.Job{},
			splitCount:                 1,
			expectedReplicatedJobCount: 1,
			expectedCondition:          nil,
			expectedRunning:            0,
			expectedSucceeded:          0,
			expectedFailed:             0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			controller, fakeClient := setupTestController()
			ctx := context.Background()

			err := fakeClient.Create(ctx, tt.hyperJob)
			assert.NoError(t, err)

			for _, vcjob := range tt.existingVCJobs {
				err := fakeClient.Create(ctx, vcjob)
				assert.NoError(t, err)
			}

			liveHyperJob := &trainingv1alpha1.HyperJob{}
			err = fakeClient.Get(ctx, client.ObjectKeyFromObject(tt.hyperJob), liveHyperJob)
			assert.NoError(t, err)

			err = controller.syncHyperJobStatus(ctx, liveHyperJob, tt.splitCount)
			assert.NoError(t, err)

			updatedHyperJob := &trainingv1alpha1.HyperJob{}
			err = fakeClient.Get(ctx, client.ObjectKey{Name: tt.hyperJob.Name, Namespace: tt.hyperJob.Namespace}, updatedHyperJob)
			assert.NoError(t, err)
			status := updatedHyperJob.Status

			// Assertions on status aggregation
			assert.Len(t, status.ReplicatedJobsStatus, tt.expectedReplicatedJobCount)
			assert.NotNil(t, status.SplitCount)
			assert.Equal(t, tt.splitCount, *status.SplitCount)
			assert.Equal(t, tt.hyperJob.Generation, status.ObservedGeneration)

			var totalRunning, totalSucceeded, totalFailed int32
			for _, rjs := range status.ReplicatedJobsStatus {
				totalRunning += rjs.Running
				totalSucceeded += rjs.Succeeded
				totalFailed += rjs.Failed
			}
			assert.Equal(t, tt.expectedRunning, totalRunning, "Total running count mismatch")
			assert.Equal(t, tt.expectedSucceeded, totalSucceeded, "Total succeeded count mismatch")
			assert.Equal(t, tt.expectedFailed, totalFailed, "Total failed count mismatch")

			// Assertions on conditions
			if tt.expectedCondition == nil {
				assert.Empty(t, status.Conditions, "Expected no conditions, but got some")
			} else {
				assert.NotEmpty(t, status.Conditions, "Expected condition, but got none")
				if len(status.Conditions) > 0 {
					condition := status.Conditions[0]
					assert.Equal(t, tt.expectedCondition.Type, condition.Type)
					assert.Equal(t, tt.expectedCondition.Status, condition.Status)
					assert.Equal(t, tt.expectedCondition.Reason, condition.Reason)
					assert.NotEmpty(t, condition.Message)
				}
			}
		})
	}
}
