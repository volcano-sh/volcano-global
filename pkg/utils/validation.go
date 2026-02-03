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

package utils

import (
	"fmt"

	clusterv1alpha1 "github.com/karmada-io/karmada/pkg/apis/cluster/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ValidateClusterCapacity checks if a cluster has sufficient capacity for workload.
// Returns true if cluster can accommodate the workload, and an error message if not.
func ValidateClusterCapacity(cluster *clusterv1alpha1.Cluster, requiredResources map[string]string) (bool, string) {
	if cluster == nil {
		return false, "cluster object is nil"
	}

	if cluster.Status.Allocatable == nil {
		return false, fmt.Sprintf("cluster %s has no allocatable resources", cluster.Name)
	}

	if ready, msg := CheckClusterReady(cluster); !ready {
		return false, fmt.Sprintf("cluster %s is not ready for scheduling, details: %s", cluster.Name, msg)
	}

	return true, ""
}

// IsClusterSchedulable checks if a cluster is schedulable based on taints and conditions.
// A cluster is schedulable if it has no unschedulable taints and is in Ready condition.
func IsClusterSchedulable(cluster *clusterv1alpha1.Cluster) bool {
	if cluster == nil {
		return false
	}

	if cluster.Spec.Taints != nil {
		for _, taint := range cluster.Spec.Taints {
			if taint.Key == "node.kubernetes.io/unschedulable" {
				return false
			}
		}
	}

	return IsClusterReady(cluster)
}

// GetClusterScheduleScore calculates a scheduling score for a cluster based on resource utilization.
// Higher score = more suitable for scheduling. Range: 0-100.
func GetClusterScheduleScore(cluster *clusterv1alpha1.Cluster) int {
	if cluster == nil || !IsClusterSchedulable(cluster) {
		return 0
	}
	score := 100
	if cluster.Spec.Taints != nil && len(cluster.Spec.Taints) > 0 {
		score -= len(cluster.Spec.Taints) * 10
	}

	if score < 0 {
		score = 0
	}
	return score
}

// GetClusterReadyTime returns the time when cluster became ready.
// Returns nil if cluster is not ready.
func GetClusterReadyTime(cluster *clusterv1alpha1.Cluster) *metav1.Time {
	if cluster == nil {
		return nil
	}

	condition := GetClusterCondition(cluster, clusterv1alpha1.ClusterConditionReady)
	if condition != nil && condition.Status == metav1.ConditionTrue {
		return &condition.LastTransitionTime
	}
	return nil
}
