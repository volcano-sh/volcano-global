# Amoro Plugin for Volcano Global

The Amoro plugin provides integration capabilities between Volcano Global and the Apache Amoro data lake platform, supporting dynamic cluster selection based on Iceberg table location information.

## Features

- **Iceberg REST API Integration**: Query table metadata through Iceberg REST API
- **Location Mapping**: Automatically map to corresponding compute clusters based on table storage location
- **Longest Prefix Matching**: Support multi-level location prefix configuration with automatic selection of the best matching prefix
- **Cluster Mapping**: Support mapping logical cluster names to actual Kubernetes clusters
- **Error Retry**: Built-in retry mechanism to improve system stability

## Configuration

### Basic Configuration

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: volcano-global-datasource-plugins
  namespace: volcano-global
data:
  amoro: |
    {
      "system": "amoro",
      "endpoint": {
        "url": "http://211.65.102.51",
        "port": 1630,
        "auth": {
          "type": "basic",
          "username": "admin",
          "password": "admin123"
        }
      },
      "locationMapping": {
        "s3://warehouse-dc1/": ["member1", "member2"],
        "s3://warehouse-dc2/": ["member3"],
        "s3://warehouse-dc3/": ["member1", "member3"]
      },
      "retryConfig": {
        "maxRetries": 3,
        "initialBackoff": "1s",
        "maxBackoff": "30s",
        "backoffMultiplier": 2.0,
        "timeout": "30s"
      }
    }
```

### Configuration Fields

#### Basic Settings
- `system`: Plugin system name, fixed as "amoro"
- `endpoint`: Amoro service connection information
  - `url`: Amoro service URL
  - `port`: Service port
  - `auth`: Authentication information (optional)

#### Location Mapping
- `locationMapping`: Mapping from storage location prefixes to clusters
  - Key: Prefix paths for S3/HDFS and other storage systems
  - Value: List of corresponding cluster names

#### Retry Configuration
- `retryConfig`: Retry strategy for API calls
  - `maxRetries`: Maximum number of retries
  - `initialBackoff`: Initial backoff time
  - `maxBackoff`: Maximum backoff time
  - `backoffMultiplier`: Backoff multiplier
  - `timeout`: Request timeout

## Usage Examples

### DataSourceClaim Configuration

```yaml
apiVersion: datadependency.volcano.sh/v1alpha1
kind: DataSourceClaim
metadata:
  name: orders-table-claim
  namespace: default
spec:
  dataSource:
    system: amoro
    catalog: dc1
    namespace: db1
    table: orders
  selector:
    matchLabels:
      app: spark-job
```

### Workflow

1. **Query Table Information**: The plugin queries table metadata through Iceberg REST API
   ```
   GET http://211.65.102.51:1630/api/iceberg/rest/v1/catalogs/dc1/namespaces/db1/tables/orders
   ```

2. **Extract Location**: Extract the `metadata.location` field from API response
   ```json
   {
     "metadata": {
       "location": "s3://warehouse-dc1/db1/orders"
     }
   }
   ```

3. **Map Clusters**: Find matching prefix based on configured `locationMapping`
   - `s3://warehouse-dc1/db1/orders` matches prefix `s3://warehouse-dc1/`
   - Returns corresponding cluster list `["member1", "member2"]`

## Prefix Matching Rules

The plugin supports longest prefix matching, for example:

```json
{
  "s3://warehouse/": ["default-cluster"],
  "s3://warehouse/prod/": ["prod-cluster1", "prod-cluster2"],
  "s3://warehouse/prod/db1/": ["db1-cluster"]
}
```

For location `s3://warehouse/prod/db1/table1`:
- Matches all three prefixes
- Selects the longest prefix `s3://warehouse/prod/db1/`
- Returns `["db1-cluster"]`

## Error Handling

- **Network Errors**: Automatic retry with exponential backoff support
- **Authentication Failure**: Log error and return empty cluster list
- **Table Not Found**: Log warning and return empty cluster list
- **Configuration Error**: Validate configuration at startup and log errors

## Monitoring and Logging

The plugin provides detailed logging:
- API requests and responses
- Location mapping process
- Cluster selection results
- Error and retry information

## Testing

Run unit tests:
```bash
go test -v ./pkg/controller/datadependency/plugins/amoro/
```

Run specific tests:
```bash
go test -v ./pkg/controller/datadependency/plugins/amoro/ -run TestMapLocationToClusters
```