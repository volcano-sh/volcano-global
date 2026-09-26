/*
Copyright 2026 The Volcano Authors.

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

package topologyfeasibility

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

type fixture struct {
	Clusters map[string]struct {
		Nodes []struct {
			Name  string `json:"name"`
			Leaf  string `json:"leaf"`
			Slots int    `json:"slots"`
		} `json:"nodes"`
	} `json:"clusters"`
	Cases []struct {
		ID                 string   `json:"id"`
		Cluster            string   `json:"cluster"`
		Mode               string   `json:"mode"`
		HighestTierAllowed int      `json:"highestTierAllowed"`
		Pods               int      `json:"pods"`
		ExcludedNodes      []string `json:"excludedNodes"`
		WantFit            bool     `json:"wantFit"`
	} `json:"cases"`
}

func readFixture(t *testing.T) fixture {
	t.Helper()
	data, err := os.ReadFile("fixtures/cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var f fixture
	if err := json.Unmarshal(data, &f); err != nil {
		t.Fatal(err)
	}
	return f
}

// This hand-sized oracle applies only to identical one-slot workers. It is not
// an estimator for heterogeneous Pods or general HyperNode graphs.
func TestHandCalculatedFixtures(t *testing.T) {
	f := readFixture(t)
	seen := map[string]bool{}
	for _, tc := range f.Cases {
		t.Run(tc.ID, func(t *testing.T) {
			if seen[tc.ID] {
				t.Fatal("duplicate case ID")
			}
			seen[tc.ID] = true
			cluster, ok := f.Clusters[tc.Cluster]
			if !ok || tc.Pods <= 0 {
				t.Fatal("invalid cluster or pod count")
			}
			excluded := make(map[string]bool, len(tc.ExcludedNodes))
			for _, name := range tc.ExcludedNodes {
				excluded[name] = true
			}
			leaves := map[string]int{}
			names := map[string]bool{}
			total := 0
			for _, node := range cluster.Nodes {
				if names[node.Name] || node.Slots < 0 || node.Leaf == "" {
					t.Fatalf("invalid node %q", node.Name)
				}
				names[node.Name] = true
				if excluded[node.Name] {
					continue
				}
				leaves[node.Leaf] += node.Slots
				total += node.Slots
			}
			for name := range excluded {
				if !names[name] {
					t.Fatalf("excluded node %q does not exist", name)
				}
			}
			fit := total >= tc.Pods
			switch tc.Mode {
			case "none", "soft":
				if tc.HighestTierAllowed != 0 {
					t.Fatal("tier is only meaningful for hard mode")
				}
			case "hard":
				switch tc.HighestTierAllowed {
				case 1:
					fit = false
					for _, slots := range leaves {
						fit = fit || slots >= tc.Pods
					}
				case 2:
				default:
					t.Fatal("unsupported fixture tier")
				}
			default:
				t.Fatal("unsupported fixture mode")
			}
			if fit != tc.WantFit {
				t.Fatalf("hand calculation: got fit=%v, want %v", fit, tc.WantFit)
			}
		})
	}
}

func TestClusterIdentityAndStableInputs(t *testing.T) {
	f := readFixture(t)
	a, b := f.Clusters["fragmented"], f.Clusters["feasible"]
	if len(a.Nodes) != 4 || len(b.Nodes) != 4 {
		t.Fatal("fixtures require four workers per member")
	}
	for i := range a.Nodes {
		if a.Nodes[i].Name != b.Nodes[i].Name {
			t.Fatal("members must reuse node names to test cluster identity")
		}
	}
	if reflect.DeepEqual(a, b) || !reflect.DeepEqual(f, readFixture(t)) {
		t.Fatal("member topology or fixture decoding changed")
	}
}
