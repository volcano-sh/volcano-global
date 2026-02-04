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
	"strings"
	"testing"

	clusterv1alpha1 "github.com/karmada-io/karmada/pkg/apis/cluster/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestValidateClusterCapacity(t *testing.T) {
	tests := []struct {
		name             string
		cluster          *clusterv1alpha1.Cluster
		required         map[string]string
		expectedValid    bool
		expectedContains string
	}{
		{
			name:             "nil cluster",
			cluster:          nil,
			expectedValid:    false,
			expectedContains: "nil",
		},
		{
			name: "cluster with no allocatable resources",
			cluster: &clusterv1alpha1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Name: "test-cluster"},
			},
			expectedValid:    false,
			expectedContains: "no allocatable resources",
		},
		{
			name: "insufficient cpu",
			cluster: &clusterv1alpha1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Name: "test-cluster"},
				Status: clusterv1alpha1.ClusterStatus{
					Allocatable: corev1.ResourceList{
						corev1.ResourceCPU: resource.MustParse("500m"),
					},
					Conditions: []metav1.Condition{
						{
							Type:   string(clusterv1alpha1.ClusterConditionReady),
							Status: metav1.ConditionTrue,
						},
					},
				},
			},
			required:         map[string]string{"cpu": "1000m"},
			expectedValid:    false,
			expectedContains: "insufficient cpu",
		},
		{
			name: "healthy cluster with sufficient resources",
			cluster: &clusterv1alpha1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Name: "test-cluster"},
				Status: clusterv1alpha1.ClusterStatus{
					Allocatable: corev1.ResourceList{
						corev1.ResourceCPU: resource.MustParse("2000m"),
					},
					Conditions: []metav1.Condition{
						{
							Type:   string(clusterv1alpha1.ClusterConditionReady),
							Status: metav1.ConditionTrue,
						},
					},
				},
			},
			required:      map[string]string{"cpu": "1000m"},
			expectedValid: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			valid, msg := ValidateClusterCapacity(tt.cluster, tt.required)
			if valid != tt.expectedValid {
				t.Errorf("expected valid=%v, got=%v, msg=%v", tt.expectedValid, valid, msg)
			}
			if !tt.expectedValid && tt.expectedContains != "" && !strings.Contains(strings.ToLower(msg), strings.ToLower(tt.expectedContains)) {
				t.Errorf("expected message to contain %q, got=%q", tt.expectedContains, msg)
			}
		})
	}
}

func TestIsClusterSchedulable(t *testing.T) {
	tests := []struct {
		name              string
		cluster           *clusterv1alpha1.Cluster
		expectedScheduled bool
	}{
		{
			name:              "nil cluster",
			cluster:           nil,
			expectedScheduled: false,
		},
		{
			name: "unschedulable cluster by taint",
			cluster: &clusterv1alpha1.Cluster{
				Spec: clusterv1alpha1.ClusterSpec{
					Taints: []corev1.Taint{
						{Key: ClusterUnschedulableTaintKey},
					},
				},
			},
			expectedScheduled: false,
		},
		{
			name: "schedulable healthy cluster",
			cluster: &clusterv1alpha1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Name: "healthy-cluster"},
				Status: clusterv1alpha1.ClusterStatus{
					Conditions: []metav1.Condition{
						{
							Type:   string(clusterv1alpha1.ClusterConditionReady),
							Status: metav1.ConditionTrue,
						},
					},
				},
			},
			expectedScheduled: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if IsClusterSchedulable(tt.cluster) != tt.expectedScheduled {
				t.Errorf("expected=%v, got=%v", tt.expectedScheduled, IsClusterSchedulable(tt.cluster))
			}
		})
	}
}
