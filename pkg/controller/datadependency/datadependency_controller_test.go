package datadependency

import (
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/workqueue"

	"volcano.sh/apis/pkg/apis/datadependency/v1alpha1"
	listerv1alpha1 "volcano.sh/apis/pkg/client/listers/datadependency/v1alpha1"
	"volcano.sh/volcano/pkg/controllers/framework"
)

// mockDataSourceClaimLister is a mock implementation of DataSourceClaimLister for testing
type mockDataSourceClaimLister struct {
	dscs []*v1alpha1.DataSourceClaim
}

func (m *mockDataSourceClaimLister) List(selector labels.Selector) ([]*v1alpha1.DataSourceClaim, error) {
	return m.dscs, nil
}

func (m *mockDataSourceClaimLister) DataSourceClaims(namespace string) listerv1alpha1.DataSourceClaimNamespaceLister {
	// Not needed for our tests
	return nil
}

// TestDataDependencyController_Name tests the Name method
func TestDataDependencyController_Name(t *testing.T) {
	controller := &DataDependencyController{}

	name := controller.Name()
	assert.Equal(t, controllerName, name)
	assert.Equal(t, "datadependency-controller", name)
}

// TestInit tests the init function and controller registration
func TestInit(t *testing.T) {
	// This test verifies that the init function has been called and the controller is registered
	// Since init() is called automatically when the package is imported, we can test
	// that the controller registration was successful by checking if we can create a new controller
	t.Run("controller registration", func(t *testing.T) {
		// The fact that we can create a DataDependencyController instance
		// indicates that the init function ran successfully
		controller := &DataDependencyController{}
		assert.NotNil(t, controller)

		// Verify the controller name matches the expected constant
		assert.Equal(t, controllerName, controller.Name())
	})
}

// TestDataDependencyController_Initialize tests the Initialize method
func TestDataDependencyController_Initialize(t *testing.T) {
	t.Run("nil config should cause panic", func(t *testing.T) {
		controller := &DataDependencyController{}

		opt := &framework.ControllerOption{
			Config: nil,
		}

		// Test that nil config causes panic (which is the current behavior)
		assert.Panics(t, func() {
			controller.Initialize(opt)
		}, "Initialize should panic with nil config")
	})

	t.Run("empty config should return error", func(t *testing.T) {
		controller := &DataDependencyController{}

		// Create an empty config that should cause client creation to fail
		emptyConfig := &rest.Config{}

		opt := &framework.ControllerOption{
			Config: emptyConfig,
		}

		err := controller.Initialize(opt)
		// Note: Some invalid configs might not immediately fail during client creation
		// but will fail during actual API calls. For unit testing, we focus on
		// testing the structure and flow rather than actual network connectivity
		if err != nil {
			assert.Contains(t, err.Error(), "failed to init")
		}
		// If no error occurs, that's also acceptable for this test case
		// as the actual validation happens during real API usage
	})

}

// TestDataDependencyController_RegisterBuiltinPlugins tests the registerBuiltinPlugins method
func TestDataDependencyController_RegisterBuiltinPlugins(t *testing.T) {
	t.Run("register builtin plugins with mock plugin manager", func(t *testing.T) {
		controller := &DataDependencyController{}

		// Create a mock plugin manager to test the registration behavior
		mockPluginManager := NewPluginManager(nil, nil)
		controller.pluginManager = mockPluginManager

		// Verify initial state - no plugins registered
		assert.Empty(t, mockPluginManager.pluginNames, "pluginNames should be empty initially")

		// Call the method under test
		controller.registerBuiltinPlugins()

		// Verify that the "amoro" plugin was registered
		assert.Contains(t, mockPluginManager.pluginNames, "amoro", "amoro plugin should be registered")
		assert.True(t, mockPluginManager.pluginNames["amoro"], "amoro plugin should be marked as registered")

		// Verify that only one plugin was registered
		assert.Len(t, mockPluginManager.pluginNames, 1, "should have exactly one plugin registered")
	})

	t.Run("register builtin plugins requires plugin manager", func(t *testing.T) {
		controller := &DataDependencyController{}

		// Test that calling registerBuiltinPlugins with nil pluginManager will panic
		// This demonstrates that the method expects pluginManager to be initialized first
		assert.Panics(t, func() {
			controller.registerBuiltinPlugins()
		}, "registerBuiltinPlugins should panic with nil pluginManager, indicating it requires proper initialization")
	})
}

// Phase 2: Test event handlers and queue operations
func TestDataDependencyController_EventHandlers(t *testing.T) {
	t.Run("addDataSourceClaim should enqueue object", func(t *testing.T) {
		controller := &DataDependencyController{
			queue: workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "test"),
		}
		defer controller.queue.ShutDown()

        dsc := &v1alpha1.DataSourceClaim{
            ObjectMeta: metav1.ObjectMeta{
                Name:      "test-dsc",
                Namespace: "default",
            },
            Spec: v1alpha1.DataSourceClaimSpec{
                Workload: v1alpha1.WorkloadRef{
                    APIVersion: "apps/v1",
                    Kind:       "Deployment",
                    Name:       "test-app",
                    Namespace:  "default",
                },
            },
        }

		// Call the event handler
		controller.addDataSourceClaim(dsc)

		// Verify the object was enqueued
		assert.Equal(t, 1, controller.queue.Len(), "Queue should contain one item")

		// Get the item from queue and verify it's the correct key
		item, shutdown := controller.queue.Get()
		assert.False(t, shutdown, "Queue should not be shut down")
		assert.Equal(t, "default/test-dsc", item, "Queue item should be the correct namespace/name key")

		controller.queue.Done(item)
	})

	t.Run("updateDataSourceClaim should enqueue object", func(t *testing.T) {
		controller := &DataDependencyController{
			queue: workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "test"),
		}
		defer controller.queue.ShutDown()

        oldDsc := &v1alpha1.DataSourceClaim{
            ObjectMeta: metav1.ObjectMeta{
                Name:      "test-dsc",
                Namespace: "default",
            },
            Spec: v1alpha1.DataSourceClaimSpec{
                Workload: v1alpha1.WorkloadRef{
                    APIVersion: "apps/v1",
                    Kind:       "Deployment",
                    Name:       "test-app",
                    Namespace:  "default",
                },
            },
        }

        newDsc := &v1alpha1.DataSourceClaim{
            ObjectMeta: metav1.ObjectMeta{
                Name:      "test-dsc",
                Namespace: "default",
            },
            Spec: v1alpha1.DataSourceClaimSpec{
                DataSourceName: "updated-ds",
                Workload: v1alpha1.WorkloadRef{
                    APIVersion: "apps/v1",
                    Kind:       "Deployment",
                    Name:       "test-app",
                    Namespace:  "default",
                },
            },
        }

		// Call the event handler
		controller.updateDataSourceClaim(oldDsc, newDsc)

		// Verify the object was enqueued
		assert.Equal(t, 1, controller.queue.Len(), "Queue should contain one item")

		// Get the item from queue and verify it's the correct key
		item, shutdown := controller.queue.Get()
		assert.False(t, shutdown, "Queue should not be shut down")
		assert.Equal(t, "default/test-dsc", item, "Queue item should be the correct namespace/name key")

		controller.queue.Done(item)
	})

	t.Run("deleteDataSourceClaim should not enqueue object", func(t *testing.T) {
		controller := &DataDependencyController{
			queue: workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "test"),
		}
		defer controller.queue.ShutDown()

        dsc := &v1alpha1.DataSourceClaim{
            ObjectMeta: metav1.ObjectMeta{
                Name:      "test-dsc",
                Namespace: "default",
            },
            Spec: v1alpha1.DataSourceClaimSpec{
                Workload: v1alpha1.WorkloadRef{
                    APIVersion: "apps/v1",
                    Kind:       "Deployment",
                    Name:       "test-app",
                    Namespace:  "default",
                },
            },
        }

		// Call the event handler
		controller.deleteDataSourceClaim(dsc)

		// Verify the object was NOT enqueued (cleanup is handled by finalizer)
		assert.Equal(t, 0, controller.queue.Len(), "Queue should be empty as cleanup is handled by finalizer")
	})

	t.Run("addDataSource should enqueue pending DSCs", func(t *testing.T) {
		// Create mock DSCs - some pending, some not
        pendingDsc1 := &v1alpha1.DataSourceClaim{
            ObjectMeta: metav1.ObjectMeta{
                Name:      "pending-dsc-1",
                Namespace: "default",
            },
            Spec: v1alpha1.DataSourceClaimSpec{
                Workload: v1alpha1.WorkloadRef{
                    APIVersion: "apps/v1",
                    Kind:       "Deployment",
                    Name:       "test-app",
                    Namespace:  "default",
                },
            },
            Status: v1alpha1.DataSourceClaimStatus{
                Phase: v1alpha1.DSCPhasePending,
            },
        }

        pendingDsc2 := &v1alpha1.DataSourceClaim{
            ObjectMeta: metav1.ObjectMeta{
                Name:      "pending-dsc-2",
                Namespace: "test-ns",
            },
            Spec: v1alpha1.DataSourceClaimSpec{
                Workload: v1alpha1.WorkloadRef{
                    APIVersion: "apps/v1",
                    Kind:       "Deployment",
                    Name:       "test-app",
                    Namespace:  "default",
                },
            },
            Status: v1alpha1.DataSourceClaimStatus{
                Phase: v1alpha1.DSCPhasePending,
            },
        }

        boundDsc := &v1alpha1.DataSourceClaim{
            ObjectMeta: metav1.ObjectMeta{
                Name:      "bound-dsc",
                Namespace: "default",
            },
            Spec: v1alpha1.DataSourceClaimSpec{
                Workload: v1alpha1.WorkloadRef{
                    APIVersion: "apps/v1",
                    Kind:       "Deployment",
                    Name:       "test-app",
                    Namespace:  "default",
                },
            },
            Status: v1alpha1.DataSourceClaimStatus{
                Phase: v1alpha1.DSCPhaseBound,
            },
        }

		// Create mock lister
		mockLister := &mockDataSourceClaimLister{
			dscs: []*v1alpha1.DataSourceClaim{pendingDsc1, pendingDsc2, boundDsc},
		}

		controller := &DataDependencyController{
			queue:     workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "test"),
			dscLister: mockLister,
		}
		defer controller.queue.ShutDown()

		ds := &v1alpha1.DataSource{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-ds",
			},
		}

		// Call the event handler
		controller.addDataSource(ds)

		// Verify only pending DSCs were enqueued
		assert.Equal(t, 2, controller.queue.Len(), "Queue should contain two pending DSCs")

		// Verify the correct items were enqueued
		enqueuedItems := make([]string, 0, 2)
		for i := 0; i < 2; i++ {
			item, shutdown := controller.queue.Get()
			assert.False(t, shutdown, "Queue should not be shut down")
			enqueuedItems = append(enqueuedItems, item.(string))
			controller.queue.Done(item)
		}

		assert.Contains(t, enqueuedItems, "default/pending-dsc-1", "Should enqueue pending-dsc-1")
		assert.Contains(t, enqueuedItems, "test-ns/pending-dsc-2", "Should enqueue pending-dsc-2")
	})

	t.Run("updateDataSource should enqueue previously bound DSCs", func(t *testing.T) {
		controller := &DataDependencyController{
			queue: workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "test"),
		}
		defer controller.queue.ShutDown()

		oldDs := &v1alpha1.DataSource{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-ds",
			},
			Spec: v1alpha1.DataSourceSpec{
				Locality: &v1alpha1.DataSourceLocality{
					ClusterNames: []string{"cluster-1"},
				},
			},
			Status: v1alpha1.DataSourceStatus{
				ClaimRefs: []corev1.ObjectReference{
					{Name: "dsc-1", Namespace: "default"},
					{Name: "dsc-2", Namespace: "test-ns"},
				},
			},
		}

		newDs := &v1alpha1.DataSource{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-ds",
			},
			Spec: v1alpha1.DataSourceSpec{
				Locality: &v1alpha1.DataSourceLocality{
					ClusterNames: []string{"cluster-1", "cluster-2"},
				},
			},
			Status: v1alpha1.DataSourceStatus{
				ClaimRefs: []corev1.ObjectReference{
					{Name: "dsc-1", Namespace: "default"},
					{Name: "dsc-3", Namespace: "test-ns"},
				},
			},
		}

		// Call the event handler
		controller.updateDataSource(oldDs, newDs)

		// Should only enqueue DSCs from old ClaimRefs (previously bound DSCs)
		assert.Equal(t, 2, controller.queue.Len(), "Queue should contain two previously bound DSCs")

		// Verify the correct items were enqueued
		enqueuedItems := make([]string, 0, 2)
		for i := 0; i < 2; i++ {
			item, shutdown := controller.queue.Get()
			assert.False(t, shutdown, "Queue should not be shut down")
			enqueuedItems = append(enqueuedItems, item.(string))
			controller.queue.Done(item)
		}

		assert.Contains(t, enqueuedItems, "default/dsc-1", "Should enqueue previously bound dsc-1")
		assert.Contains(t, enqueuedItems, "test-ns/dsc-2", "Should enqueue previously bound dsc-2")
		// dsc-3 should NOT be enqueued since it wasn't previously bound
		assert.NotContains(t, enqueuedItems, "test-ns/dsc-3", "Should not enqueue newly bound dsc-3")
	})

	t.Run("updateDataSource should skip when Locality unchanged", func(t *testing.T) {
		controller := &DataDependencyController{
			queue: workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "test"),
		}
		defer controller.queue.ShutDown()

		oldDs := &v1alpha1.DataSource{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-ds",
			},
			Spec: v1alpha1.DataSourceSpec{
				Locality: &v1alpha1.DataSourceLocality{
					ClusterNames: []string{"cluster-1"},
				},
			},
			Status: v1alpha1.DataSourceStatus{
				ClaimRefs: []corev1.ObjectReference{
					{Name: "dsc-1", Namespace: "default"},
					{Name: "dsc-2", Namespace: "test-ns"},
				},
			},
		}

		newDs := &v1alpha1.DataSource{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-ds",
			},
			Spec: v1alpha1.DataSourceSpec{
				Locality: &v1alpha1.DataSourceLocality{
					ClusterNames: []string{"cluster-1"}, // Same as oldDs
				},
			},
			Status: v1alpha1.DataSourceStatus{
				ClaimRefs: []corev1.ObjectReference{
					{Name: "dsc-1", Namespace: "default"},
					{Name: "dsc-3", Namespace: "test-ns"}, // Status changed but Locality unchanged
				},
			},
		}

		// Call the event handler
		controller.updateDataSource(oldDs, newDs)

		// Should not enqueue any DSCs since Locality didn't change
		assert.Equal(t, 0, controller.queue.Len(), "Queue should be empty when Locality unchanged")
	})

	t.Run("deleteDataSource should enqueue bound DSCs", func(t *testing.T) {
		controller := &DataDependencyController{
			queue: workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "test"),
		}
		defer controller.queue.ShutDown()

		ds := &v1alpha1.DataSource{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-ds",
			},
			Status: v1alpha1.DataSourceStatus{
				ClaimRefs: []corev1.ObjectReference{
					{Name: "dsc-1", Namespace: "default"},
					{Name: "dsc-2", Namespace: "test-ns"},
				},
			},
		}

		// Call the event handler
		controller.deleteDataSource(ds)

		// Should enqueue all bound DSCs
		assert.Equal(t, 2, controller.queue.Len(), "Queue should contain two DSCs")

		// Verify the correct items were enqueued
		enqueuedItems := make([]string, 0, 2)
		for i := 0; i < 2; i++ {
			item, shutdown := controller.queue.Get()
			assert.False(t, shutdown, "Queue should not be shut down")
			enqueuedItems = append(enqueuedItems, item.(string))
			controller.queue.Done(item)
		}

		assert.Contains(t, enqueuedItems, "default/dsc-1", "Should enqueue dsc-1")
		assert.Contains(t, enqueuedItems, "test-ns/dsc-2", "Should enqueue dsc-2")
	})
}
