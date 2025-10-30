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

package amoro

import (
	"reflect"
	"testing"

	"volcano.sh/volcano-global/pkg/controller/datadependency"
)

func TestMapLocationToClusters(t *testing.T) {
	plugin := &AmoroPlugin{
		config: datadependency.PluginConfig{
			LocationMapping: map[string][]string{
				"s3://warehouse-dc1/": {"member1", "member2"},
				"s3://warehouse-dc2/": {"member3"},
				"s3://warehouse-dc3/": {"member1", "member3"},
			},
		},
	}

	tests := []struct {
		name     string
		location string
		expected []string
	}{
		{
			name:     "DC1 warehouse location",
			location: "s3://warehouse-dc1/db1/orders",
			expected: []string{"member1", "member2"},
		},
		{
			name:     "DC2 warehouse location",
			location: "s3://warehouse-dc2/db1/products",
			expected: []string{"member3"},
		},
		{
			name:     "DC3 warehouse location",
			location: "s3://warehouse-dc3/analytics/reports",
			expected: []string{"member1", "member3"},
		},
		{
			name:     "Unknown location",
			location: "s3://unknown-warehouse/data",
			expected: nil,
		},
		{
			name:     "Exact prefix match",
			location: "s3://warehouse-dc1/",
			expected: []string{"member1", "member2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := plugin.mapLocationToClusters(tt.location)
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("mapLocationToClusters() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

func TestMapLocationToClustersPrefixPriority(t *testing.T) {
	// Test longest prefix matching
	plugin := &AmoroPlugin{
		config: datadependency.PluginConfig{
			LocationMapping: map[string][]string{
				"s3://warehouse/":          {"default-cluster"},
				"s3://warehouse/prod/":     {"prod-cluster1", "prod-cluster2"},
				"s3://warehouse/prod/db1/": {"db1-cluster"},
			},
		},
	}

	tests := []struct {
		name     string
		location string
		expected []string
	}{
		{
			name:     "Most specific prefix wins",
			location: "s3://warehouse/prod/db1/table1",
			expected: []string{"db1-cluster"},
		},
		{
			name:     "Second most specific prefix",
			location: "s3://warehouse/prod/db2/table1",
			expected: []string{"prod-cluster1", "prod-cluster2"},
		},
		{
			name:     "Least specific prefix",
			location: "s3://warehouse/test/table1",
			expected: []string{"default-cluster"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := plugin.mapLocationToClusters(tt.location)
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("mapLocationToClusters() = %v, expected %v", result, tt.expected)
			}
		})
	}
}
