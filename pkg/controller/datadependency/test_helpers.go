package datadependency

import (
	"context"
	"fmt"
	"time"

	workv1alpha2 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha2"
	karmadaclientset "github.com/karmada-io/karmada/pkg/generated/clientset/versioned/fake"
	karmadainformers "github.com/karmada-io/karmada/pkg/generated/informers/externalversions"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic/fake"
	kubeinformers "k8s.io/client-go/informers"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"volcano.sh/apis/pkg/apis/datadependency/v1alpha1"
	dscfake "volcano.sh/apis/pkg/client/clientset/versioned/fake"
	datadependencyinformers "volcano.sh/apis/pkg/client/informers/externalversions"
)

// MockPluginManager is a mock implementation of PluginManagerInterface for testing
type MockPluginManager struct {
	selectClustersFunc func(ctx context.Context, dsc *v1alpha1.DataSourceClaim) ([]string, error)
}

// NewMockPluginManager creates a new MockPluginManager with the given selectClustersFunc
func NewMockPluginManager(selectClustersFunc func(ctx context.Context, dsc *v1alpha1.DataSourceClaim) ([]string, error)) *MockPluginManager {
	return &MockPluginManager{
		selectClustersFunc: selectClustersFunc,
	}
}

// Implement PluginManagerInterface methods
func (m *MockPluginManager) RegisterPlugin(name string, builder PluginBuilder) {}
func (m *MockPluginManager) RegisterPluginName(name string)                    {}
func (m *MockPluginManager) LoadConfigurations(ctx context.Context) error      { return nil }
func (m *MockPluginManager) InitializePlugins(ctx context.Context) error       { return nil }
func (m *MockPluginManager) GetPlugin(name string) (DataSourcePlugin, error)   { return nil, nil }
func (m *MockPluginManager) ListPlugins() []string                             { return []string{} }
func (m *MockPluginManager) ReloadConfiguration(ctx context.Context, pluginName string) error {
	return nil
}
func (m *MockPluginManager) SetUpdateInterval(interval time.Duration) {}
func (m *MockPluginManager) GetUpdateInterval() time.Duration         { return time.Minute }
func (m *MockPluginManager) SetConfigMapInfo(name, namespace string)  {}
func (m *MockPluginManager) HealthCheck(ctx context.Context) map[string]error {
	return map[string]error{}
}

func (m *MockPluginManager) SelectClusters(ctx context.Context, dsc *v1alpha1.DataSourceClaim) ([]string, error) {
	if m.selectClustersFunc != nil {
		return m.selectClustersFunc(ctx, dsc)
	}
	return []string{}, nil
}

func (m *MockPluginManager) StartPeriodicUpdate(ctx context.Context) {
	// Mock implementation - do nothing
}

func (m *MockPluginManager) StopPeriodicUpdate() {
	// Mock implementation - do nothing
}

// setupTestController creates a test controller with the given DataSourceClaim
func setupTestController(ctx context.Context, dsc *v1alpha1.DataSourceClaim) *DataDependencyController {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		klog.Fatalf("Failed to add scheme: %v", err)
	}

	// Create fake clients
	kubeClient := kubefake.NewSimpleClientset()
	dscClient := dscfake.NewSimpleClientset(dsc)
	karmadaClient := karmadaclientset.NewSimpleClientset()
	dynamicClient := fake.NewSimpleDynamicClient(scheme)

	// Create informers
	dscInformerFactory := datadependencyinformers.NewSharedInformerFactory(dscClient, 0)
	karmadaInformerFactory := karmadainformers.NewSharedInformerFactory(karmadaClient, 0)
	kubeInformerFactory := kubeinformers.NewSharedInformerFactory(kubeClient, 0)

	// Set up DataSourceClaim informer with workloadRef indexer
	dscInformer := dscInformerFactory.Datadependency().V1alpha1().DataSourceClaims()
	if err := dscInformer.Informer().AddIndexers(cache.Indexers{
		WorkloadRefIndex: func(obj interface{}) ([]string, error) {
			dsc, ok := obj.(*v1alpha1.DataSourceClaim)
			if !ok {
				return nil, fmt.Errorf("failed to convert to DataSourceClaim")
			}
			workloadRefKey, indexErr := GenerateWorkloadRefIndexKey(&dsc.Spec.Workload)
			if indexErr != nil {
				// For test purposes, if WorkloadRef is invalid, return empty slice instead of error
				return []string{}, nil
			}
			return []string{workloadRefKey}, nil
		},
	}); err != nil {
		klog.Fatalf("Failed to add indexer to dsc informer: %v", err)
	}

	// Set up ResourceBinding informer with workloadRef indexer
	rbInformer := karmadaInformerFactory.Work().V1alpha2().ResourceBindings()
	if err := rbInformer.Informer().AddIndexers(cache.Indexers{
		WorkloadRefIndex: func(obj interface{}) ([]string, error) {
			rb, ok := obj.(*workv1alpha2.ResourceBinding)
			if !ok {
				return nil, fmt.Errorf("failed to convert to ResourceBinding")
			}
			workloadRefKey, indexErr := GenerateWorkloadRefIndexKeyFromResource(rb.Spec.Resource)
			if indexErr != nil {
				return nil, indexErr
			}
			return []string{workloadRefKey}, nil
		},
	}); err != nil {
		klog.Fatalf("Failed to add indexer to rb informer: %v", err)
	}

	// Create controller
	controller := &DataDependencyController{
		kubeClient:             kubeClient,
		datadependencyClient:   dscClient,
		karmadaClient:          karmadaClient,
		dynamic:                dynamicClient,
		Scheme:                 scheme,
		dataInformerFactory:    dscInformerFactory,
		karmadaInformerFactory: karmadaInformerFactory,
		kubeInformerFactory:    kubeInformerFactory,
		dscLister:              dscInformer.Lister(),
		dscListerSynced:        dscInformer.Informer().HasSynced,
		dsLister:               dscInformerFactory.Datadependency().V1alpha1().DataSources().Lister(),
		dsListerSynced:         dscInformerFactory.Datadependency().V1alpha1().DataSources().Informer().HasSynced,
		rbLister:               rbInformer.Lister(),
		rbListerSynced:         rbInformer.Informer().HasSynced,
		cLister:                karmadaInformerFactory.Cluster().V1alpha1().Clusters().Lister(),
		cListerSynced:          karmadaInformerFactory.Cluster().V1alpha1().Clusters().Informer().HasSynced,
		configMapLister:        kubeInformerFactory.Core().V1().ConfigMaps().Lister(),
		configMapListerSynced:  kubeInformerFactory.Core().V1().ConfigMaps().Informer().HasSynced,
		queue:                  workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "DataDependency"),
		recorder:               record.NewFakeRecorder(100),
		pluginManager:          NewMockPluginManager(nil),
	}

	// Set syncHandler
	controller.syncHandler = controller.Reconcile

	// Start informers
	dscInformerFactory.Start(ctx.Done())
	karmadaInformerFactory.Start(ctx.Done())
	kubeInformerFactory.Start(ctx.Done())

	return controller
}
