# Fuse Helm Chart

Deploy [Fuse](https://github.com/seraphjiang/fuse) — a federated query engine — on Kubernetes.

## Install

```bash
helm install fuse deploy/helm/fuse \
  --set image.tag=2.0.0
```

## Values

| Key | Default | Description |
|-----|---------|-------------|
| `replicaCount` | `2` | Number of replicas |
| `image.repository` | `ghcr.io/seraphjiang/fuse-server` | Container image |
| `image.tag` | `""` (appVersion) | Image tag |
| `image.pullPolicy` | `IfNotPresent` | Pull policy |
| `service.type` | `ClusterIP` | Service type |
| `service.port` | `9400` | Service port |
| `ingress.enabled` | `false` | Enable ingress |
| `resources.requests.cpu` | `250m` | CPU request |
| `resources.requests.memory` | `256Mi` | Memory request |
| `resources.limits.cpu` | `1` | CPU limit |
| `resources.limits.memory` | `512Mi` | Memory limit |
| `autoscaling.enabled` | `true` | Enable HPA |
| `autoscaling.minReplicas` | `2` | Min replicas |
| `autoscaling.maxReplicas` | `10` | Max replicas |
| `config.max_concurrent_queries` | `64` | Max concurrent queries |
| `config.default_timeout` | `30s` | Default query timeout |
| `config.rate_limit_global` | `1000` | Global rate limit (req/min) |
| `config.rate_limit_per_ip` | `100` | Per-IP rate limit (req/min) |
| `connectors` | `[]` | Connector configurations |
| `redis.enabled` | `false` | Enable Redis for shared state |
| `redis.url` | `redis://redis:6379` | Redis URL |
| `tenants.enabled` | `false` | Enable multi-tenant mode |
| `networkPolicy.enabled` | `false` | Enable NetworkPolicy |
| `plugins.enabled` | `false` | Enable WASM plugin loading |
| `telemetry.enabled` | `false` | Enable OpenTelemetry export |

### Monitoring

| Key | Default | Description |
|-----|---------|-------------|
| `metrics.podAnnotations` | `false` | Add `prometheus.io/*` pod annotations |
| `metrics.serviceMonitor.enabled` | `false` | Create Prometheus ServiceMonitor |
| `metrics.serviceMonitor.interval` | `30s` | Scrape interval |
| `metrics.serviceMonitor.scrapeTimeout` | `10s` | Scrape timeout |
| `metrics.serviceMonitor.path` | `/metrics` | Metrics path |
| `metrics.serviceMonitor.labels` | `{}` | Extra labels (match Prometheus selector) |
| `metrics.serviceMonitor.relabelings` | `[]` | Relabeling rules |
| `metrics.serviceMonitor.metricRelabelings` | `[]` | Metric relabeling rules |
| `metrics.serviceMonitor.namespaceSelector` | `{}` | Namespace selector override |

### Operations

| Key | Default | Description |
|-----|---------|-------------|
| `configValidation.enabled` | `false` | Init container for config validation |
| `resourceQuota.enabled` | `false` | Create ResourceQuota |
| `blueGreen.enabled` | `false` | Enable blue-green deployment |
| `blueGreen.activeSlot` | `blue` | Active slot (`blue` or `green`) |
| `backup.enabled` | `false` | Enable backup CronJob |
| `backup.schedule` | `0 2 * * *` | Backup cron schedule |

## Monitoring

Fuse exposes Prometheus metrics at `GET /metrics`. Enable the ServiceMonitor:

```yaml
metrics:
  serviceMonitor:
    enabled: true
    labels:
      release: prometheus
```

Or use annotation-based discovery:

```yaml
metrics:
  podAnnotations: true
```

## Connectors

```yaml
connectors:
  - id: my_cluster
    type: opensearch
    url: "https://opensearch:9200"
  - id: my_ddb
    type: dynamodb
    region: us-west-2
    table_names: ["users", "orders"]
```

See [fuse.toml](../../fuse.toml) for all connector options.
