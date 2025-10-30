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
    "sync"
    "time"

    "volcano.sh/apis/pkg/apis/datadependency/v1alpha1"
)

// DataSourcePlugin defines the interface that all data source plugins must implement.
// Each plugin corresponds to a specific data storage system (e.g., Amoro, Iceberg, etc.)
type DataSourcePlugin interface {
	// Name returns the unique name of the plugin, which should match the System field in DataSourceClaim
	Name() string

	// Init initializes the plugin with configuration data.
	// The config contains connection information, cluster mappings, and other plugin-specific settings.
	Init(config PluginConfig) error

	// Select queries the backend data storage system to determine which clusters contain the requested data.
	// It takes a DataSourceClaim as input and returns a list of cluster names where the data is located.
	// Returns an empty list if no data is found, or an error if the query fails.
	Select(ctx context.Context, dsc *v1alpha1.DataSourceClaim) ([]string, error)

	// IsHealthy checks if the plugin can successfully communicate with its backend system.
	// This is used for health monitoring and error handling.
	IsHealthy(ctx context.Context) error
}

// PluginConfig contains the configuration data for a plugin.
type PluginConfig struct {
	// System is the name of the data storage system (e.g., "amoro", "iceberg")
	System string `json:"system"`

	// Endpoint contains connection information for the metadata server
	Endpoint EndpointConfig `json:"endpoint"`

	// LocationMapping maps storage location prefixes to Kubernetes cluster names
	// Each location prefix can map to multiple clusters for data replication scenarios
	LocationMapping map[string][]string `json:"locationMapping"`

	// RetryConfig contains retry and timeout settings
	RetryConfig RetryConfig `json:"retryConfig"`

	// Additional plugin-specific configuration
	Extra map[string]interface{} `json:"extra,omitempty"`
}

// EndpointConfig contains connection information for the metadata server
type EndpointConfig struct {
	// URL is the metadata server endpoint
	URL string `json:"url"`

	// Port is the server port
	Port int `json:"port"`

	// Authentication contains auth credentials
	Auth AuthConfig `json:"auth,omitempty"`

	// TLS configuration
	TLS TLSConfig `json:"tls,omitempty"`
}

// AuthConfig contains authentication credentials
type AuthConfig struct {
	// Type of authentication (e.g., "basic", "token", "oauth")
	Type string `json:"type"`

	// Username for basic auth
	Username string `json:"username,omitempty"`

	// Password for basic auth
	Password string `json:"password,omitempty"`

	// Token for token-based auth
	Token string `json:"token,omitempty"`

	// Additional auth parameters
	Extra map[string]string `json:"extra,omitempty"`
}

// TLSConfig contains TLS configuration
type TLSConfig struct {
	// Enabled indicates whether TLS is enabled
	Enabled bool `json:"enabled"`

	// InsecureSkipVerify skips certificate verification
	InsecureSkipVerify bool `json:"insecureSkipVerify,omitempty"`

	// CertFile path to client certificate
	CertFile string `json:"certFile,omitempty"`

	// KeyFile path to client private key
	KeyFile string `json:"keyFile,omitempty"`

	// CAFile path to CA certificate
	CAFile string `json:"caFile,omitempty"`
}

// RetryConfig contains retry and timeout settings
type RetryConfig struct {
	// MaxRetries is the maximum number of retry attempts
	MaxRetries int `json:"maxRetries"`

	// InitialBackoff is the initial backoff duration
	InitialBackoff time.Duration `json:"initialBackoff"`

	// MaxBackoff is the maximum backoff duration
	MaxBackoff time.Duration `json:"maxBackoff"`

	// BackoffMultiplier is the backoff multiplier for exponential backoff
	BackoffMultiplier float64 `json:"backoffMultiplier"`

	// Timeout is the timeout for each request
	Timeout time.Duration `json:"timeout"`
}

// retryConfigJSON is a helper struct for JSON unmarshaling with string durations
type retryConfigJSON struct {
	MaxRetries        int     `json:"maxRetries"`
	InitialBackoff    string  `json:"initialBackoff"`
	MaxBackoff        string  `json:"maxBackoff"`
	BackoffMultiplier float64 `json:"backoffMultiplier"`
	Timeout           string  `json:"timeout"`
}

// UnmarshalJSON implements custom JSON unmarshaling for RetryConfig
func (rc *RetryConfig) UnmarshalJSON(data []byte) error {
	var helper retryConfigJSON
	if err := json.Unmarshal(data, &helper); err != nil {
		return err
	}

	rc.MaxRetries = helper.MaxRetries
	rc.BackoffMultiplier = helper.BackoffMultiplier

	// Parse duration strings
	if helper.InitialBackoff != "" {
		duration, err := time.ParseDuration(helper.InitialBackoff)
		if err != nil {
			return fmt.Errorf("invalid initialBackoff duration: %w", err)
		}
		rc.InitialBackoff = duration
	}

	if helper.MaxBackoff != "" {
		duration, err := time.ParseDuration(helper.MaxBackoff)
		if err != nil {
			return fmt.Errorf("invalid maxBackoff duration: %w", err)
		}
		rc.MaxBackoff = duration
	}

	if helper.Timeout != "" {
		duration, err := time.ParseDuration(helper.Timeout)
		if err != nil {
			return fmt.Errorf("invalid timeout duration: %w", err)
		}
		rc.Timeout = duration
	}

	return nil
}

// DefaultRetryConfig returns a default retry configuration
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxRetries:        3,
		InitialBackoff:    1 * time.Second,
		MaxBackoff:        30 * time.Second,
		BackoffMultiplier: 2.0,
		Timeout:           30 * time.Second,
	}
}

// PluginBuilder is a function that creates a new instance of a plugin
type PluginBuilder func() DataSourcePlugin

// Global registry for plugin builders to allow plugins to self-register at package init time
var registryMu sync.RWMutex
var pluginBuildersRegistry = make(map[string]PluginBuilder)

// RegisterPluginBuilder registers a plugin builder into the global registry.
// Plugins should call this in their init() function to avoid import cycles.
func RegisterPluginBuilder(name string, builder PluginBuilder) {
	registryMu.Lock()
	defer registryMu.Unlock()
	pluginBuildersRegistry[name] = builder
}

// GetRegisteredPluginBuilders returns a copy of the registered plugin builders.
// The copy avoids external mutation and ensures thread-safety for readers.
func GetRegisteredPluginBuilders() map[string]PluginBuilder {
	registryMu.RLock()
	defer registryMu.RUnlock()
	copy := make(map[string]PluginBuilder, len(pluginBuildersRegistry))
	for k, v := range pluginBuildersRegistry {
		copy[k] = v
	}
	return copy
}
