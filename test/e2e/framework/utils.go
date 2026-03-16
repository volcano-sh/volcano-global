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

package framework

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"

	policyv1alpha1 "github.com/karmada-io/karmada/pkg/apis/policy/v1alpha1"
	workv1alpha2 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha2"
	batchv1alpha1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
)

// CreateQueue creates a Volcano queue in Karmada control plane
func CreateQueue(name string, weight int32) (*schedulingv1beta1.Queue, error) {
	ctx := context.Background()

	queue := &schedulingv1beta1.Queue{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: schedulingv1beta1.QueueSpec{
			Weight: weight,
		},
	}

	By(fmt.Sprintf("Creating queue: %s", name))
	createdQueue, err := KarmadaVolcanoClient.SchedulingV1beta1().Queues().Create(ctx, queue, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to create queue: %w", err)
	}

	return createdQueue, nil
}

// DeleteQueue deletes a Volcano queue
func DeleteQueue(name string) error {
	ctx := context.Background()

	By(fmt.Sprintf("Deleting queue: %s", name))
	err := KarmadaVolcanoClient.SchedulingV1beta1().Queues().Delete(ctx, name, metav1.DeleteOptions{})
	if err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("failed to delete queue: %w", err)
	}

	return nil
}

// CreateVolcanoJob creates a Volcano Job in Karmada control plane
func CreateVolcanoJob(namespace, name, queueName string, replicas int32) (*batchv1alpha1.Job, error) {
	ctx := context.Background()

	job := &batchv1alpha1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: batchv1alpha1.JobSpec{
			MinAvailable: replicas,
			Queue:        queueName,
			Tasks: []batchv1alpha1.TaskSpec{
				{
					Name:     "worker",
					Replicas: replicas,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							RestartPolicy: corev1.RestartPolicyNever,
							Containers: []corev1.Container{
								{
									Name:    "worker",
									Image:   "busybox:1.36",
									Command: []string{"sh", "-c", "echo 'Hello from Volcano Job' && sleep 30"},
									Resources: corev1.ResourceRequirements{
										Requests: corev1.ResourceList{
											corev1.ResourceCPU:    mustParseQuantity("100m"),
											corev1.ResourceMemory: mustParseQuantity("64Mi"),
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

	By(fmt.Sprintf("Creating Volcano Job: %s/%s", namespace, name))
	createdJob, err := KarmadaVolcanoClient.BatchV1alpha1().Jobs(namespace).Create(ctx, job, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to create Volcano Job: %w", err)
	}

	return createdJob, nil
}

// DeleteVolcanoJob deletes a Volcano Job
func DeleteVolcanoJob(namespace, name string) error {
	ctx := context.Background()

	By(fmt.Sprintf("Deleting Volcano Job: %s/%s", namespace, name))
	err := KarmadaVolcanoClient.BatchV1alpha1().Jobs(namespace).Delete(ctx, name, metav1.DeleteOptions{})
	if err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("failed to delete Volcano Job: %w", err)
	}

	return nil
}

// CreatePropagationPolicy creates a PropagationPolicy for distributing resources
func CreatePropagationPolicy(namespace, name, resourceName, resourceKind, resourceAPIVersion string, clusterNames []string) (*policyv1alpha1.PropagationPolicy, error) {
	ctx := context.Background()

	placement := policyv1alpha1.Placement{}
	if len(clusterNames) > 0 {
		var clusterAffinities []policyv1alpha1.ClusterAffinityTerm
		for _, cluster := range clusterNames {
			clusterAffinities = append(clusterAffinities, policyv1alpha1.ClusterAffinityTerm{
				AffinityName: cluster,
				ClusterAffinity: policyv1alpha1.ClusterAffinity{
					ClusterNames: []string{cluster},
				},
			})
		}
		placement.ClusterAffinities = clusterAffinities
	}

	pp := &policyv1alpha1.PropagationPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: policyv1alpha1.PropagationSpec{
			ResourceSelectors: []policyv1alpha1.ResourceSelector{
				{
					APIVersion: resourceAPIVersion,
					Kind:       resourceKind,
					Name:       resourceName,
				},
			},
			Placement: placement,
		},
	}

	By(fmt.Sprintf("Creating PropagationPolicy: %s/%s", namespace, name))
	createdPP, err := KarmadaKarmadaClient.PolicyV1alpha1().PropagationPolicies(namespace).Create(ctx, pp, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to create PropagationPolicy: %w", err)
	}

	return createdPP, nil
}

// DeletePropagationPolicy deletes a PropagationPolicy
func DeletePropagationPolicy(namespace, name string) error {
	ctx := context.Background()

	By(fmt.Sprintf("Deleting PropagationPolicy: %s/%s", namespace, name))
	err := KarmadaKarmadaClient.PolicyV1alpha1().PropagationPolicies(namespace).Delete(ctx, name, metav1.DeleteOptions{})
	if err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("failed to delete PropagationPolicy: %w", err)
	}

	return nil
}

// WaitForVolcanoJobStatus waits for a Volcano Job to reach a specific status
func WaitForVolcanoJobStatus(namespace, name string, expectedPhase batchv1alpha1.JobPhase, timeout time.Duration) error {
	By(fmt.Sprintf("Waiting for Volcano Job %s/%s to reach status: %s", namespace, name, expectedPhase))

	return WaitForCondition(timeout, func() (bool, error) {
		job, err := KarmadaVolcanoClient.BatchV1alpha1().Jobs(namespace).Get(context.Background(), name, metav1.GetOptions{})
		if err != nil {
			if errors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}

		klog.V(4).Infof("Job %s/%s current status: %s", namespace, name, job.Status.State.Phase)
		return job.Status.State.Phase == expectedPhase, nil
	})
}

// WaitForResourceBinding waits for a ResourceBinding to be created
func WaitForResourceBinding(namespace, name string, timeout time.Duration) (*workv1alpha2.ResourceBinding, error) {
	By(fmt.Sprintf("Waiting for ResourceBinding %s/%s", namespace, name))

	var rb *workv1alpha2.ResourceBinding
	err := WaitForCondition(timeout, func() (bool, error) {
		var err error
		rb, err = KarmadaKarmadaClient.WorkV1alpha2().ResourceBindings(namespace).Get(context.Background(), name, metav1.GetOptions{})
		if err != nil {
			if errors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
		return true, nil
	})

	return rb, err
}

// WaitForPodsOnMemberCluster waits for pods to be running on a member cluster
func WaitForPodsOnMemberCluster(member, namespace string, labelSelector string, expectedCount int, timeout time.Duration) error {
	client, ok := MemberClients[member]
	if !ok {
		return fmt.Errorf("member cluster %s not found", member)
	}

	By(fmt.Sprintf("Waiting for %d pods on %s in namespace %s", expectedCount, member, namespace))

	return WaitForCondition(timeout, func() (bool, error) {
		pods, err := client.CoreV1().Pods(namespace).List(context.Background(), metav1.ListOptions{
			LabelSelector: labelSelector,
		})
		if err != nil {
			return false, err
		}

		runningCount := 0
		for _, pod := range pods.Items {
			if pod.Status.Phase == corev1.PodRunning || pod.Status.Phase == corev1.PodSucceeded {
				runningCount++
			}
		}

		klog.V(4).Infof("Found %d/%d running/succeeded pods on %s", runningCount, expectedCount, member)
		return runningCount >= expectedCount, nil
	})
}

// GetMemberClusters returns the list of member cluster names
func GetMemberClusters() []string {
	clusters := make([]string, 0, len(MemberClients))
	for name := range MemberClients {
		clusters = append(clusters, name)
	}
	return clusters
}

// GetKarmadaClusters returns the Karmada cluster list
func GetKarmadaClusters() ([]string, error) {
	ctx := context.Background()
	clusters, err := KarmadaKarmadaClient.ClusterV1alpha1().Clusters().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	names := make([]string, 0, len(clusters.Items))
	for _, cluster := range clusters.Items {
		names = append(names, cluster.Name)
	}
	return names, nil
}

// mustParseQuantity parses a quantity string and panics on error
func mustParseQuantity(s string) resource.Quantity {
	q, err := resource.ParseQuantity(s)
	if err != nil {
		panic(fmt.Sprintf("failed to parse quantity %s: %v", s, err))
	}
	return q
}
