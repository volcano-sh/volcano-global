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

package datadependency

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

    "volcano.sh/apis/pkg/apis/datadependency/v1alpha1"
    "volcano.sh/apis/pkg/client/clientset/versioned"
)

const (
	// DefaultConfigMapName is the default name of the ConfigMap containing plugin configurations
	DefaultConfigMapName = "volcano-global-datasource-plugins"
	// DefaultConfigMapNamespace is the default namespace for the plugin ConfigMap
	DefaultConfigMapNamespace = "volcano-global"
	// DefaultUpdateInterval is the default interval for updating DataSource cache
	DefaultUpdateInterval = 5 * time.Minute
)

// PluginManagerInterface defines the minimal set required by the controller.
// Keeps the abstraction focused and avoids exposing manager-internal helpers.
type PluginManagerInterface interface {
    // RegisterPluginName registers a plugin name for dynamic loading
    RegisterPluginName(name string)
    // LoadConfigurations loads plugin configurations from ConfigMap or local files
    LoadConfigurations(ctx context.Context) error
    // InitializePlugins initializes plugins using the loaded configuration
    InitializePlugins(ctx context.Context) error
    // SelectClusters selects clusters for a DataSourceClaim via the appropriate plugin
    SelectClusters(ctx context.Context, dsc *v1alpha1.DataSourceClaim) ([]string, error)
    // StartPeriodicUpdate starts periodic updates of DataSource objects
    StartPeriodicUpdate(ctx context.Context)
    // StopPeriodicUpdate stops the periodic updates
    StopPeriodicUpdate()
}

// PluginManager manages the lifecycle of data source plugins
type PluginManager struct {
	mu sync.RWMutex

	// pluginBuilders stores the registered plugin builders
	pluginBuilders map[string]PluginBuilder

	// pluginNames stores registered plugin names for dynamic loading
	pluginNames map[string]bool

	// plugins stores the initialized plugin instances
	plugins map[string]DataSourcePlugin

	// configs stores the configuration for each plugin
	configs map[string]PluginConfig

	// kubeClient for accessing ConfigMaps
	kubeClient kubernetes.Interface

	// datadependencyClient for updating DataSource objects
	datadependencyClient versioned.Interface

	// configMapName and namespace for plugin configurations
	configMapName      string
	configMapNamespace string

	// updateInterval for periodic DataSource updates
	updateInterval time.Duration

	// stopCh for stopping the update goroutine
	stopCh chan struct{}
}

// NewPluginManager creates a new plugin manager
func NewPluginManager(kubeClient kubernetes.Interface, datadependencyClient versioned.Interface) *PluginManager {
	pm := &PluginManager{
		pluginBuilders:       make(map[string]PluginBuilder),
		pluginNames:          make(map[string]bool),
		plugins:              make(map[string]DataSourcePlugin),
		configs:              make(map[string]PluginConfig),
		kubeClient:           kubeClient,
		datadependencyClient: datadependencyClient,
		configMapName:        DefaultConfigMapName,
		configMapNamespace:   DefaultConfigMapNamespace,
		updateInterval:       DefaultUpdateInterval,
		stopCh:               make(chan struct{}),
	}

	// Load plugin builders from the global plugin registry.
	for name, builder := range GetRegisteredPluginBuilders() {
		pm.pluginBuilders[name] = builder
		klog.V(3).Infof("Loaded plugin builder: %s", name)
	}

	return pm
}

// RegisterPlugin registers a plugin builder with the manager
func (pm *PluginManager) RegisterPlugin(name string, builder PluginBuilder) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	pm.pluginBuilders[name] = builder
	klog.V(3).Infof("Registered data source plugin: %s", name)
}

// RegisterPluginName registers a plugin name for dynamic loading
func (pm *PluginManager) RegisterPluginName(name string) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.pluginNames[name] = true
}

// LoadConfigurations loads plugin configurations from ConfigMap or mounted files
func (pm *PluginManager) LoadConfigurations(ctx context.Context) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// Try to load from mounted files first (if PLUGIN_CONFIG_PATH is set)
	if configPath := os.Getenv("PLUGIN_CONFIG_PATH"); configPath != "" {
		if err := pm.loadFromFiles(configPath); err == nil {
			klog.V(3).Infof("Successfully loaded plugin configurations from files: %s", configPath)
			return nil
		} else {
			klog.V(3).Infof("Failed to load from files, falling back to ConfigMap: %v", err)
		}
	}

	// Fallback to ConfigMap API
	return pm.loadFromConfigMap(ctx)
}

// loadFromFiles loads configurations from mounted ConfigMap files
func (pm *PluginManager) loadFromFiles(configPath string) error {
	files, err := os.ReadDir(configPath)
	if err != nil {
		return fmt.Errorf("failed to read config directory %s: %w", configPath, err)
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		// Skip Kubernetes ConfigMap mount artifacts
		fileName := file.Name()
		if strings.HasPrefix(fileName, "..") {
			klog.V(4).Infof("Skipping ConfigMap mount artifact: %s", fileName)
			continue
		}

		pluginName := fileName
		filePath := filepath.Join(configPath, pluginName)
		
		configData, err := os.ReadFile(filePath)
		if err != nil {
			klog.Errorf("Failed to read config file %s: %v", filePath, err)
			continue
		}

		var config PluginConfig
		if err := json.Unmarshal(configData, &config); err != nil {
			klog.Errorf("Failed to parse configuration for plugin %s: %v", pluginName, err)
			continue
		}

		// Set default retry config if not specified
		if config.RetryConfig.MaxRetries == 0 {
			config.RetryConfig = DefaultRetryConfig()
		}

		pm.configs[pluginName] = config
		klog.V(3).Infof("Loaded configuration for plugin: %s from file", pluginName)
	}

	return nil
}

// loadFromConfigMap loads configurations from Kubernetes ConfigMap (original implementation)
func (pm *PluginManager) loadFromConfigMap(ctx context.Context) error {
	// Get the ConfigMap
	configMap, err := pm.kubeClient.CoreV1().ConfigMaps(pm.configMapNamespace).Get(
		ctx, pm.configMapName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get plugin ConfigMap %s/%s: %w",
			pm.configMapNamespace, pm.configMapName, err)
	}

	// Parse configurations
	for pluginName, configData := range configMap.Data {
		var config PluginConfig
		if err := json.Unmarshal([]byte(configData), &config); err != nil {
			klog.Errorf("Failed to parse configuration for plugin %s: %v", pluginName, err)
			continue
		}

		// Set default retry config if not specified
		if config.RetryConfig.MaxRetries == 0 {
			config.RetryConfig = DefaultRetryConfig()
		}

		pm.configs[pluginName] = config
		klog.V(3).Infof("Loaded configuration for plugin: %s from ConfigMap", pluginName)
	}

	return nil
}

// InitializePlugins initializes all registered plugins with their configurations
func (pm *PluginManager) InitializePlugins(ctx context.Context) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	for pluginName, builder := range pm.pluginBuilders {
		config, exists := pm.configs[pluginName]
		if !exists {
			klog.Warningf("No configuration found for plugin %s, skipping initialization", pluginName)
			continue
		}

		// Create plugin instance
		plugin := builder()
		if plugin.Name() != pluginName {
			klog.Errorf("Plugin name mismatch: expected %s, got %s", pluginName, plugin.Name())
			continue
		}

		// Initialize the plugin
		if err := plugin.Init(config); err != nil {
			klog.Errorf("Failed to initialize plugin %s: %v", pluginName, err)
			continue
		}

		// Test plugin health
		if err := plugin.IsHealthy(ctx); err != nil {
			klog.Warningf("Plugin %s is not healthy: %v", pluginName, err)
			// Continue anyway, the plugin might recover later
		}

		pm.plugins[pluginName] = plugin
		klog.V(2).Infof("Successfully initialized plugin: %s", pluginName)
	}

	return nil
}

// GetPlugin returns a plugin instance by name
func (pm *PluginManager) GetPlugin(name string) (DataSourcePlugin, error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	plugin, exists := pm.plugins[name]
	if !exists {
		return nil, fmt.Errorf("plugin %s not found or not initialized", name)
	}

	return plugin, nil
}

// ListPlugins returns a list of all initialized plugin names
func (pm *PluginManager) ListPlugins() []string {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	var names []string
	for name := range pm.plugins {
		names = append(names, name)
	}
	return names
}

// SelectClusters uses the appropriate plugin to select clusters for a DataSourceClaim
func (pm *PluginManager) SelectClusters(ctx context.Context, dsc *v1alpha1.DataSourceClaim) ([]string, error) {
	plugin, err := pm.GetPlugin(dsc.Spec.System)
	if err != nil {
		return nil, fmt.Errorf("failed to get plugin for system %s: %w", dsc.Spec.System, err)
	}

	// Check plugin health before using it
	if healthErr := plugin.IsHealthy(ctx); healthErr != nil {
		return nil, fmt.Errorf("plugin %s is not healthy: %w", dsc.Spec.System, healthErr)
	}

	// Call the plugin's Select method
	clusters, err := plugin.Select(ctx, dsc)
	if err != nil {
		return nil, fmt.Errorf("plugin %s failed to select clusters: %w", dsc.Spec.System, err)
	}

	klog.V(4).Infof("Plugin %s selected clusters %v for DSC %s/%s",
		dsc.Spec.System, clusters, dsc.Namespace, dsc.Name)

	return clusters, nil
}

// ReloadConfiguration reloads the configuration for a specific plugin
func (pm *PluginManager) ReloadConfiguration(ctx context.Context, pluginName string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// Get the ConfigMap
	configMap, err := pm.kubeClient.CoreV1().ConfigMaps(pm.configMapNamespace).Get(
		ctx, pm.configMapName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get plugin ConfigMap: %w", err)
	}

	// Parse the specific plugin configuration
	configData, exists := configMap.Data[pluginName]
	if !exists {
		return fmt.Errorf("configuration for plugin %s not found", pluginName)
	}

	var config PluginConfig
	if err := json.Unmarshal([]byte(configData), &config); err != nil {
		return fmt.Errorf("failed to parse configuration for plugin %s: %w", pluginName, err)
	}

	// Set default retry config if not specified
	if config.RetryConfig.MaxRetries == 0 {
		config.RetryConfig = DefaultRetryConfig()
	}

	pm.configs[pluginName] = config

	// Reinitialize the plugin if it exists
	if builder, exists := pm.pluginBuilders[pluginName]; exists {
		plugin := builder()
		if err := plugin.Init(config); err != nil {
			return fmt.Errorf("failed to reinitialize plugin %s: %w", pluginName, err)
		}
		pm.plugins[pluginName] = plugin
		klog.V(2).Infof("Successfully reloaded configuration for plugin: %s", pluginName)
	}

	return nil
}

// SetUpdateInterval sets the update interval for periodic DataSource updates
func (pm *PluginManager) SetUpdateInterval(interval time.Duration) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.updateInterval = interval
}

// GetUpdateInterval returns the current update interval
func (pm *PluginManager) GetUpdateInterval() time.Duration {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return pm.updateInterval
}

// SetConfigMapInfo sets the ConfigMap name and namespace for plugin configurations
func (pm *PluginManager) SetConfigMapInfo(name, namespace string) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.configMapName = name
	pm.configMapNamespace = namespace
}

// HealthCheck performs health checks on all initialized plugins
func (pm *PluginManager) HealthCheck(ctx context.Context) map[string]error {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	results := make(map[string]error)
	for name, plugin := range pm.plugins {
		results[name] = plugin.IsHealthy(ctx)
	}

	return results
}

// StartPeriodicUpdate starts the periodic update of DataSource cluster information
func (pm *PluginManager) StartPeriodicUpdate(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(pm.GetUpdateInterval())
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				pm.updateDataSources(ctx)
			case <-pm.stopCh:
				klog.V(2).Info("Stopping periodic DataSource update")
				return
			case <-ctx.Done():
				klog.V(2).Info("Context cancelled, stopping periodic DataSource update")
				return
			}
		}
	}()
	klog.V(2).Infof("Started periodic DataSource update with interval: %v", pm.GetUpdateInterval())
}

// StopPeriodicUpdate stops the periodic update
func (pm *PluginManager) StopPeriodicUpdate() {
	close(pm.stopCh)
}

// updateDataSources updates all DataSource objects with latest cluster information
func (pm *PluginManager) updateDataSources(ctx context.Context) {
	klog.V(4).Info("Starting periodic DataSource update")

	// Get all DataSource objects
	dataSources, err := pm.datadependencyClient.DatadependencyV1alpha1().DataSources().List(ctx, metav1.ListOptions{})
	if err != nil {
		klog.Errorf("Failed to list DataSources: %v", err)
		return
	}

	for _, ds := range dataSources.Items {
		pm.updateSingleDataSource(ctx, &ds)
	}

	klog.V(4).Infof("Completed periodic DataSource update, processed %d DataSources", len(dataSources.Items))
}

// updateSingleDataSource updates a single DataSource object
func (pm *PluginManager) updateSingleDataSource(ctx context.Context, ds *v1alpha1.DataSource) {
	plugin, err := pm.GetPlugin(ds.Spec.System)
	if err != nil {
		klog.V(4).Infof("Plugin %s not found for DataSource %s/%s, skipping", ds.Spec.System, ds.Namespace, ds.Name)
		return
	}

	// Check plugin health
	if healthErr := plugin.IsHealthy(ctx); healthErr != nil {
		klog.Warningf("Plugin %s is not healthy, skipping DataSource %s/%s: %v", ds.Spec.System, ds.Namespace, ds.Name, healthErr)
		return
	}

	// Create a mock DataSourceClaim to query the plugin
	mockDSC := &v1alpha1.DataSourceClaim{
		Spec: v1alpha1.DataSourceClaimSpec{
			System:         ds.Spec.System,
			DataSourceType: ds.Spec.Type,
			DataSourceName: ds.Spec.Name,
			Attributes:     ds.Spec.Attributes,
		},
	}

    // Get updated cluster information
    clusters, err := plugin.Select(ctx, mockDSC)
    if err != nil {
        // Prefer structured error detection (HTTP status or sentinel) over string matching
        if IsNotFoundErr(err) {
            klog.Warningf("DataSource %s/%s no longer exists in external system (404), initiating deletion", ds.Namespace, ds.Name)
            // Delete the DataSource since it no longer exists in the external system
            deleteErr := pm.datadependencyClient.DatadependencyV1alpha1().DataSources().Delete(ctx, ds.Name, metav1.DeleteOptions{})
            if deleteErr != nil {
                klog.Errorf("Failed to delete DataSource %s: %v", ds.Name, deleteErr)
            } else {
                klog.V(3).Infof("Successfully initiated deletion of DataSource %s", ds.Name)
            }
            return
        }

        klog.Errorf("Failed to get clusters for DataSource %s: %v", ds.Name, err)
        return
    }

	// Update DataSource spec if clusters have changed
	if !pm.clustersEqual(ds.Spec.Locality.ClusterNames, clusters) {
		ds.Spec.Locality.ClusterNames = clusters

		_, err := pm.datadependencyClient.DatadependencyV1alpha1().DataSources().Update(ctx, ds, metav1.UpdateOptions{})
		if err != nil {
			klog.Errorf("Failed to update DataSource %s: %v", ds.Name, err)
			return
		}

		klog.V(3).Infof("Updated DataSource %s with clusters: %v", ds.Name, clusters)
	}
}

// clustersEqual compares two cluster slices for equality
func (pm *PluginManager) clustersEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	// Create maps for comparison
	mapA := make(map[string]bool)
	mapB := make(map[string]bool)

	for _, cluster := range a {
		mapA[cluster] = true
	}

	for _, cluster := range b {
		mapB[cluster] = true
	}

	// Compare maps
	for cluster := range mapA {
		if !mapB[cluster] {
			return false
		}
	}

	for cluster := range mapB {
		if !mapA[cluster] {
			return false
		}
	}

	return true
}
