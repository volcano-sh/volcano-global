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
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"time"

	"k8s.io/klog/v2"

    "volcano.sh/apis/pkg/apis/datadependency/v1alpha1"
	"volcano.sh/volcano-global/pkg/controller/datadependency"
)

const (
	// PluginName is the name of the Amoro plugin
	PluginName = "amoro"
	// DefaultTimeout for HTTP requests
	DefaultTimeout = 30 * time.Second
)

// AmoroPlugin implements the DataSourcePlugin interface for Apache Amoro
type AmoroPlugin struct {
	config     datadependency.PluginConfig
	httpClient *http.Client
	baseURL    string
}

// AmoroTableMetadata represents the metadata section of Amoro table response
type AmoroTableMetadata struct {
	Location string `json:"location"`
}

// AmoroTableResponse represents the actual response from Amoro REST API
type AmoroTableResponse struct {
	MetadataLocation string                 `json:"metadata-location"`
	Metadata         AmoroTableMetadata     `json:"metadata"`
	Config           map[string]interface{} `json:"config"`
}

// LocationClusterMapping represents the mapping from location prefixes to clusters
type LocationClusterMapping map[string][]string

// NewAmoroPlugin creates a new instance of the Amoro plugin
func NewAmoroPlugin() datadependency.DataSourcePlugin {
	return &AmoroPlugin{}
}

// Name returns the name of the plugin
func (p *AmoroPlugin) Name() string {
	return PluginName
}

// Init initializes the plugin with the given configuration
func (p *AmoroPlugin) Init(config datadependency.PluginConfig) error {
	p.config = config

	// Validate required configuration
	if config.Endpoint.URL == "" {
		return fmt.Errorf("endpoint URL is required for Amoro plugin")
	}

	// Build base URL
	baseURL := config.Endpoint.URL
	if config.Endpoint.Port > 0 {
		baseURL = fmt.Sprintf("%s:%d", baseURL, config.Endpoint.Port)
	}
	p.baseURL = strings.TrimSuffix(baseURL, "/")

	// Create HTTP client with timeout
	timeout := DefaultTimeout
	if config.RetryConfig.Timeout > 0 {
		timeout = config.RetryConfig.Timeout
	}

	p.httpClient = &http.Client{
		Timeout: timeout,
	}

	klog.V(3).Infof("Amoro plugin initialized with endpoint: %s", p.baseURL)
	return nil
}

// Select queries Amoro and returns a list of cluster names where the data is available
func (p *AmoroPlugin) Select(ctx context.Context, dsc *v1alpha1.DataSourceClaim) ([]string, error) {
	// Extract table information from DSC
	// For Amoro, we expect the DataSourceName to be in format "catalog.database.table"
	dataSourceName := dsc.Spec.DataSourceName
	parts := strings.Split(dataSourceName, ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("for Amoro plugin, dataSourceName must be in format 'catalog.database.table', got: %s", dataSourceName)
	}

	catalog := parts[0]
	database := parts[1]
	tableName := parts[2]

	if catalog == "" || database == "" || tableName == "" {
		return nil, fmt.Errorf("catalog, database, and table cannot be empty in dataSourceName: %s", dataSourceName)
	}

	// Query Amoro API with retry logic
	clusters, err := p.queryWithRetry(ctx, catalog, database, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query Amoro: %w", err)
	}

	klog.V(4).Infof("Amoro plugin found clusters %v for table %s.%s.%s",
		clusters, catalog, database, tableName)

	return clusters, nil
}

// IsHealthy checks if the plugin can connect to Amoro
func (p *AmoroPlugin) IsHealthy(ctx context.Context) error {
	// Simple health check by calling a lightweight API endpoint
	healthURL := fmt.Sprintf("%s/api/v1/health", p.baseURL)

	req, err := http.NewRequestWithContext(ctx, "GET", healthURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create health check request: %w", err)
	}

	// Add authentication if configured
	if err := p.addAuthentication(req); err != nil {
		return fmt.Errorf("failed to add authentication: %w", err)
	}

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("health check request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health check failed with status: %d", resp.StatusCode)
	}

	return nil
}

// queryWithRetry queries Amoro API with exponential backoff retry
func (p *AmoroPlugin) queryWithRetry(ctx context.Context, catalog, database, tableName string) ([]string, error) {
	var lastErr error
	delay := p.config.RetryConfig.InitialBackoff

	for attempt := 0; attempt <= p.config.RetryConfig.MaxRetries; attempt++ {
		if attempt > 0 {
			// Apply jitter to delay
			actualDelay := delay
			if rand.Float64() < 0.1 { // 10% jitter
				actualDelay = time.Duration(float64(delay) * (0.9 + 0.2*rand.Float64()))
			}

			klog.V(4).Infof("Retrying Amoro query (attempt %d/%d) after %v",
				attempt, p.config.RetryConfig.MaxRetries, actualDelay)

			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(actualDelay):
			}
		}

		clusters, err := p.queryAmoro(ctx, catalog, database, tableName)
		if err == nil {
			return clusters, nil
		}

		lastErr = err

		// Check if this is a retryable error
		if !p.isRetryableError(err) {
			return nil, err
		}

		// Calculate next delay with exponential backoff
		delay = time.Duration(float64(delay) * p.config.RetryConfig.BackoffMultiplier)
		if delay > p.config.RetryConfig.MaxBackoff {
			delay = p.config.RetryConfig.MaxBackoff
		}
	}

	return nil, fmt.Errorf("max retries exceeded, last error: %w", lastErr)
}

// queryAmoro queries the Amoro REST API for table information and extracts cluster mapping
func (p *AmoroPlugin) queryAmoro(ctx context.Context, catalog, database, tableName string) ([]string, error) {
	// Build Iceberg REST API URL
	apiURL := fmt.Sprintf("%s/api/iceberg/rest/v1/catalogs/%s/namespaces/%s/tables/%s",
		p.baseURL, url.PathEscape(catalog), url.PathEscape(database), url.PathEscape(tableName))

	req, err := http.NewRequestWithContext(ctx, "GET", apiURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Add authentication
	if err := p.addAuthentication(req); err != nil {
		return nil, fmt.Errorf("failed to add authentication: %w", err)
	}

	// Set headers
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "volcano-global-datasource-plugin/1.0")

	// Make the request
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	// Handle HTTP errors
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusNotFound {
			return nil, datadependency.NewNotFoundError(fmt.Sprintf("table %s.%s.%s not found: %s", catalog, database, tableName, string(body)))
		}
		return nil, &datadependency.HTTPError{
			Code:    resp.StatusCode,
			Message: string(body),
		}
	}

	// Parse response
	var tableResp AmoroTableResponse
	if err := json.Unmarshal(body, &tableResp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	// Extract location from metadata
	location := tableResp.Metadata.Location
	if location == "" {
		return nil, fmt.Errorf("no location found in table metadata")
	}

	klog.V(4).Infof("Found table location: %s", location)

	// Map location to clusters using configuration
	clusters := p.mapLocationToClusters(location)
	if len(clusters) == 0 {
		return nil, fmt.Errorf("no clusters found for location: %s", location)
	}

	return clusters, nil
}

// addAuthentication adds authentication headers to the request
func (p *AmoroPlugin) addAuthentication(req *http.Request) error {
	auth := p.config.Endpoint.Auth

	switch strings.ToLower(auth.Type) {
	case "basic":
		if auth.Username == "" || auth.Password == "" {
			return fmt.Errorf("username and password are required for basic auth")
		}
		req.SetBasicAuth(auth.Username, auth.Password)

	case "bearer", "token":
		if auth.Token == "" {
			return fmt.Errorf("token is required for bearer auth")
		}
		req.Header.Set("Authorization", "Bearer "+auth.Token)

	case "apikey":
		if auth.Token == "" {
			return fmt.Errorf("token is required for API key auth")
		}
		// Check if custom header is specified
		headerName := "X-API-Key"
		if customHeader, exists := auth.Extra["header"]; exists {
			headerName = customHeader
		}
		req.Header.Set(headerName, auth.Token)

	case "", "none":
		// No authentication
		return nil

	default:
		return fmt.Errorf("unsupported authentication type: %s", auth.Type)
	}

	return nil
}

// mapLocationToClusters maps a storage location to cluster names based on configuration
func (p *AmoroPlugin) mapLocationToClusters(location string) []string {
	// Get location mapping from plugin configuration
	if len(p.config.LocationMapping) == 0 {
		klog.Warningf("No locationMapping found in plugin configuration")
		return nil
	}

	// Find matching prefix (longest match)
	var matchedClusters []string
	var longestPrefix string

	for prefix, clusters := range p.config.LocationMapping {
		if strings.HasPrefix(location, prefix) && len(prefix) > len(longestPrefix) {
			longestPrefix = prefix
			matchedClusters = clusters
		}
	}

	if len(matchedClusters) == 0 {
		klog.Warningf("No cluster mapping found for location: %s", location)
		return nil
	}

	klog.V(4).Infof("Mapped location %s (prefix: %s) to clusters: %v", location, longestPrefix, matchedClusters)

	return matchedClusters
}

// isRetryableError determines if an error is retryable based on Apache Iceberg REST API specification
func (p *AmoroPlugin) isRetryableError(err error) bool {
	// Use the centralized retryable error checking
	return datadependency.IsRetryableError(err)
}

// Ensure the Amoro plugin builder is registered at package initialization time
func init() {
	// Register builder into the global plugin registry
	datadependency.RegisterPluginBuilder(PluginName, NewAmoroPlugin)
}
