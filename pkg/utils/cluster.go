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

// GetClusterCondition returns the condition with the specified type from cluster status.
// Returns nil if the condition is not found.
func GetClusterCondition(cluster *clusterv1alpha1.Cluster, conditionType clusterv1alpha1.ClusterConditionType) *metav1.Condition {
	for i := range cluster.Status.Conditions {
		if cluster.Status.Conditions[i].Type == string(conditionType) {
			return &cluster.Status.Conditions[i]
		}
	}
	return nil
}

// IsClusterReady returns true if the cluster is in Ready condition with status True.
// This is a convenience function for simple boolean checks where error messages are not needed.
func IsClusterReady(cluster *clusterv1alpha1.Cluster) bool {
	condition := GetClusterCondition(cluster, clusterv1alpha1.ClusterConditionReady)
	return condition != nil && condition.Status == metav1.ConditionTrue
}

// CheckClusterReady checks if the cluster is ready and returns a detailed message if not.
// Returns (true, "") if the cluster is ready, or (false, message) with details if not ready.
func CheckClusterReady(cluster *clusterv1alpha1.Cluster) (bool, string) {
	condition := GetClusterCondition(cluster, clusterv1alpha1.ClusterConditionReady)
	if condition == nil {
		return false, fmt.Sprintf("Cluster<%s> has not %s Condition", cluster.Name, clusterv1alpha1.ClusterConditionReady)
	}
	if condition.Status == metav1.ConditionTrue {
		return true, ""
	}
	return false, fmt.Sprintf("Cluster<%s> is not ready, reason: %s, message: %s", cluster.Name, condition.Reason, condition.Message)
}
