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
	"testing"
	clusterv1alpha1 "github.com/karmada-io/karmada/pkg/apis/cluster/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestGetClusterCondition(t *testing.T) {
	readyType := clusterv1alpha1.ClusterConditionReady

	tests := []struct {
		name       string
		cluster    *clusterv1alpha1.Cluster
		condType   clusterv1alpha1.ClusterConditionType
		wantNil    bool
		wantStatus metav1.ConditionStatus
	}{
		{
			name: "ready condition exists and is true",
			cluster: &clusterv1alpha1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Name: "test-cluster"},
				Status: clusterv1alpha1.ClusterStatus{
					Conditions: []metav1.Condition{{Type: string(readyType), Status: metav1.ConditionTrue}},
				},
			},
			condType: readyType, wantNil: false, wantStatus: metav1.ConditionTrue,
		},
		{
			name: "ready condition exists and is false",
			cluster: &clusterv1alpha1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Name: "test-cluster"},
				Status: clusterv1alpha1.ClusterStatus{
					Conditions: []metav1.Condition{{Type: string(readyType), Status: metav1.ConditionFalse}},
				},
			},
			condType: readyType, wantNil: false, wantStatus: metav1.ConditionFalse,
		},
		{
			name: "condition does not exist",
			cluster: &clusterv1alpha1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Name: "test-cluster"},
			},
			condType: readyType, wantNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetClusterCondition(tt.cluster, tt.condType)
			if tt.wantNil && got != nil {
				t.Errorf("GetClusterCondition() = %v, want nil", got)
			}
			if !tt.wantNil && got == nil {
				t.Errorf("GetClusterCondition() = nil, want non-nil")
			} else if !tt.wantNil && got.Status != tt.wantStatus {
				t.Errorf("GetClusterCondition().Status = %v, want %v", got.Status, tt.wantStatus)
			}
		})
	}
}

func TestIsClusterReady(t *testing.T) {
	readyType := clusterv1alpha1.ClusterConditionReady

	tests := []struct {
		name    string
		cluster *clusterv1alpha1.Cluster
		want    bool
	}{
		{
			name: "cluster is ready",
			cluster: &clusterv1alpha1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Name: "ready-cluster"},
				Status: clusterv1alpha1.ClusterStatus{
					Conditions: []metav1.Condition{{Type: string(readyType), Status: metav1.ConditionTrue}},
				},
			},
			want: true,
		},
		{
			name: "cluster is not ready",
			cluster: &clusterv1alpha1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Name: "not-ready-cluster"},
				Status: clusterv1alpha1.ClusterStatus{
					Conditions: []metav1.Condition{{Type: string(readyType), Status: metav1.ConditionFalse}},
				},
			},
			want: false,
		},
		{
			name:    "cluster has no ready condition",
			cluster: &clusterv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "no-condition"}},
			want:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsClusterReady(tt.cluster); got != tt.want {
				t.Errorf("IsClusterReady() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCheckClusterReady(t *testing.T) {
	readyType:=clusterv1alpha1.ClusterConditionReady
	tests:=[]struct {
		name string
		cluster *clusterv1alpha1.Cluster
		ready bool
		message string
	}{
		{
			name: "cluster is ready",
			cluster: &clusterv1alpha1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Name: "ready-cluster"},
				Status: clusterv1alpha1.ClusterStatus{
					Conditions: []metav1.Condition{{Type: string(readyType), Status: metav1.ConditionTrue}},
				},
			},
			ready: true, message: "",
		},
		{
			name: "cluster is not ready with reason",
			cluster: &clusterv1alpha1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Name: "not-ready-cluster"},
				Status: clusterv1alpha1.ClusterStatus{
					Conditions: []metav1.Condition{
						{Type: string(readyType), Status: metav1.ConditionFalse, Reason: "NetworkUnavailable", Message: "Network is down"},
					},
				},
			},
			ready:   false,
			message: "Cluster<not-ready-cluster> is not ready, reason: NetworkUnavailable, message: Network is down",
		},
		{
			name:    "cluster has no ready condition",
			cluster: &clusterv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "no-condition"}},
			ready:   false,
			message: "Cluster<no-condition> has not Ready Condition",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotReady, gotMessage := CheckClusterReady(tt.cluster)
			if gotReady != tt.ready {
				t.Errorf("CheckClusterReady() ready = %v, want %v", gotReady, tt.ready)
			}
			if gotMessage != tt.message {
				t.Errorf("CheckClusterReady() message = %v, want %v", gotMessage, tt.message)
			}
		})
	}
}