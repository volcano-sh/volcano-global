/*
Copyright 2024 The Volcano Authors.

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

package cache

import (
	"testing"
	"time"

	workv1alpha2 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha2"
	karmadafake "github.com/karmada-io/karmada/pkg/generated/clientset/versioned/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/workqueue"

	"volcano.sh/volcano-global/pkg/dispatcher/api"
)

// newTestCache creates a minimal DispatcherCache suitable for testing the
// unSuspendResourceBindingTaskWorker. A fake Karmada client is provided so
// that the goroutine spawned for cache-hit items does not panic.
func newTestCache() *DispatcherCache {
	return &DispatcherCache{
		resourceBindingInfos: map[string]map[string]*api.ResourceBindingInfo{},
		unSuspendRBTaskQueue: workqueue.New(),
		karmadaClient:        karmadafake.NewSimpleClientset(),
	}
}

func buildTestRBI(namespace, name string) *api.ResourceBindingInfo {
	return &api.ResourceBindingInfo{
		ResourceBinding: &workv1alpha2.ResourceBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
			},
		},
		DispatchStatus: api.UnSuspending,
	}
}

func TestWorkerContinuesAfterCacheMiss(t *testing.T) {
	dc := newTestCache()

	// Add a key that does NOT exist in the cache.
	missingKey := types.NamespacedName{Namespace: "ns-missing", Name: "rb-missing"}
	dc.unSuspendRBTaskQueue.Add(missingKey)

	// Add a sentinel key that DOES exist in the cache. If the worker
	// correctly continues past the cache miss, it will reach this item.
	presentKey := types.NamespacedName{Namespace: "ns-present", Name: "rb-present"}
	dc.resourceBindingInfos["ns-present"] = map[string]*api.ResourceBindingInfo{
		"rb-present": buildTestRBI("ns-present", "rb-present"),
	}
	dc.unSuspendRBTaskQueue.Add(presentKey)

	// Run the worker in a goroutine. It will process both items then block
	// waiting for the next Get(). We use ShutDown to unblock it after a
	// timeout.
	done := make(chan struct{})
	go func() {
		dc.unSuspendResourceBindingTaskWorker()
		close(done)
	}()

	// Wait until the worker has processed both items.
	err := wait.PollImmediate(100*time.Millisecond, 5*time.Second, func() (bool, error) {
		return dc.unSuspendRBTaskQueue.Len() == 0, nil
	})
	if err != nil {
		t.Fatalf("Worker did not process all items from queue in time: %v", err)
	}

	// Now that the queue is empty, the worker is blocked on Get(). Shut it down.
	dc.unSuspendRBTaskQueue.ShutDown()

	select {
	case <-done:
		// Worker exited cleanly after shutdown — good.
	case <-time.After(5 * time.Second):
		t.Fatal("Worker did not exit after ShutDown; it is likely stuck")
	}
}

func TestWorkerReleasesWorkqueueItemOnCacheMiss(t *testing.T) {
	dc := newTestCache()

	key := types.NamespacedName{Namespace: "ns", Name: "rb"}

	// First pass: add the key without populating the cache. The worker
	// should call Done(key) so the item leaves the processing set.
	dc.unSuspendRBTaskQueue.Add(key)

	done := make(chan struct{})
	go func() {
		dc.unSuspendResourceBindingTaskWorker()
		close(done)
	}()

	// Wait for the worker to process the missing item.
	if err := wait.PollImmediate(100*time.Millisecond, 2*time.Second, func() (bool, error) {
		return dc.unSuspendRBTaskQueue.Len() == 0, nil
	}); err != nil {
		t.Fatalf("Worker did not process the first item in time: %v", err)
	}

	// Now add the same key again. If Done() was called correctly in the
	// first pass, the item will be re-queued and served by Get(). If Done()
	// was NOT called, the item stays in the "processing" set and Add() puts
	// it in "dirty" — it will never be served, and the test will time out.
	dc.resourceBindingInfos["ns"] = map[string]*api.ResourceBindingInfo{
		"rb": buildTestRBI("ns", "rb"),
	}
	dc.unSuspendRBTaskQueue.Add(key)

	// Give the worker time to process the re-added item, then shut down.
	time.Sleep(500 * time.Millisecond)
	dc.unSuspendRBTaskQueue.ShutDown()

	select {
	case <-done:
		// Worker exited cleanly — the item was re-queued and processed.
	case <-time.After(5 * time.Second):
		t.Fatal("Worker did not process re-added item; Done() was likely not called on cache miss")
	}
}

func TestWorkerHandlesNilNamespaceMap(t *testing.T) {
	dc := newTestCache()
	// resourceBindingInfos is initialized but has no "ns-gone" key, so
	// dc.resourceBindingInfos["ns-gone"] returns nil. The worker must
	// not panic on the nil sub-map.

	key := types.NamespacedName{Namespace: "ns-gone", Name: "rb"}
	dc.unSuspendRBTaskQueue.Add(key)

	done := make(chan struct{})
	go func() {
		dc.unSuspendResourceBindingTaskWorker()
		close(done)
	}()

	time.Sleep(300 * time.Millisecond)
	dc.unSuspendRBTaskQueue.ShutDown()

	select {
	case <-done:
		// No panic, worker handled the nil namespace map gracefully.
	case <-time.After(5 * time.Second):
		t.Fatal("Worker did not exit; possible panic or hang on nil namespace map")
	}
}
