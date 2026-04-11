# Deploying Fuse on Kubernetes (EKS/K8s)

Deploy Fuse as a horizontally-scalable, stateless query engine using the official Helm chart.

## Prerequisites

- Kubernetes 1.24+ (EKS, GKE, AKS, or self-managed)
- Helm 3.x
- `kubectl` configured for your cluster
- (Optional) Redis for shared state across replicas
- (Optional) OpenSearch/DynamoDB/Postgres connectors accessible from the cluster

## Quick Start

```bash
helm install fuse deploy/helm/fuse \
  --set image.tag=1.4.0 \
  --set replicaCount=2
```

Verify:

```bash
kubectl get pods -l app.kubernetes.io/name=fuse
kubectl port-forward svc/fuse 9400:9400
curl http://localhost:9400/api/fuse/health
```

## Configuration

### Custom values file

Create `my-values.yaml`:

```yaml
replicaCount: 3

image:
  repository: ghcr.io/seraphjiang/fuse-server
  tag: "1.4.0"

resources:
  requests:
    cpu: 500m
    memory: 512Mi
  limits:
    cpu: "2"
    memory: 1Gi

config:
  bind: "0.0.0.0:9400"
  max_concurrent_queries: 128
  default_timeout: "60s"
  cors_origins: ["https://dashboard.example.com"]
  max_result_bytes: 209715200  # 200MB
  rate_limit_global: 2000
  rate_limit_per_ip: 200

connectors:
  - id: my_opensearch
    type: opensearch
    url: "https://opensearch.internal:9200"
  - id: my_postgres
    type: postgres
    url: "postgresql://user:pass@postgres.internal:5432/mydb"
  - id: my_dynamodb
    type: dynamodb
    region: us-west-2
    table_names: ["users", "orders"]
```

Deploy:

```bash
helm install fuse deploy/helm/fuse -f my-values.yaml
```

### Stateless mode with Redis

For horizontal scaling, enable Redis to share state across replicas:

```yaml
redis:
  enabled: true
  url: "redis://redis.internal:6379"
```

This shares saved queries, query history, and audit logs across all Fuse pods.

### Ingress

```yaml
ingress:
  enabled: true
  className: alb  # or nginx
  annotations:
    alb.ingress.kubernetes.io/scheme: internet-facing
    alb.ingress.kubernetes.io/target-type: ip
  hosts:
    - host: fuse.example.com
      paths:
        - path: /
          pathType: Prefix
  tls:
    - secretName: fuse-tls
      hosts:
        - fuse.example.com
```

### Autoscaling

Enabled by default (2-10 replicas, 70% CPU target):

```yaml
autoscaling:
  enabled: true
  minReplicas: 2
  maxReplicas: 20
  targetCPUUtilizationPercentage: 60
```

### Telemetry (OpenTelemetry)

```yaml
telemetry:
  enabled: true
  otlp_endpoint: "http://otel-collector.monitoring:4317"
```

## EKS-Specific Setup

### IAM Roles for Service Accounts (IRSA)

For AWS connectors (DynamoDB, S3, Athena, Timestream, CloudWatch):

```yaml
serviceAccount:
  create: true
  annotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::ACCOUNT:role/fuse-query-role
```

Required IAM permissions:
- DynamoDB: `dynamodb:Scan`, `dynamodb:Query`, `dynamodb:DescribeTable`
- S3: `s3:GetObject`, `s3:ListBucket`, `s3:PutObject` (for write path)
- Athena: `athena:StartQueryExecution`, `athena:GetQueryResults`
- Timestream: `timestream:Select`, `timestream:DescribeTable`, `timestream:ListTables`
- CloudWatch: `logs:StartQuery`, `logs:GetQueryResults`

### Node placement

For consistent performance, use dedicated node groups:

```yaml
nodeSelector:
  workload: fuse

tolerations:
  - key: workload
    value: fuse
    effect: NoSchedule
```

## Health Checks

The Helm chart configures three probes:

| Probe | Path | Purpose |
|-------|------|---------|
| Startup | `/api/fuse/health` | Wait for connectors to initialize (up to 60s) |
| Liveness | `/api/fuse/health` | Restart if unresponsive |
| Readiness | `/api/fuse/health` | Remove from LB if unhealthy |

## Security

The chart enforces:
- `runAsNonRoot: true` (UID 1000)
- `readOnlyRootFilesystem: true`
- All Linux capabilities dropped
- PodDisruptionBudget (minAvailable: 1)

For network policies, restrict connector egress:

```yaml
# Example: allow only OpenSearch and Postgres egress
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: fuse-egress
spec:
  podSelector:
    matchLabels:
      app.kubernetes.io/name: fuse
  policyTypes: [Egress]
  egress:
    - to:
        - ipBlock: { cidr: 10.0.0.0/8 }
      ports:
        - port: 9200  # OpenSearch
        - port: 5432  # Postgres
        - port: 6379  # Redis
```

## Upgrading

```bash
helm upgrade fuse deploy/helm/fuse -f my-values.yaml --set image.tag=1.5.0
```

Rolling updates are zero-downtime thanks to the PDB and readiness probes.

## Troubleshooting

```bash
# Check pod status
kubectl get pods -l app.kubernetes.io/name=fuse

# View logs
kubectl logs -l app.kubernetes.io/name=fuse --tail=100

# Check config
kubectl get configmap fuse-config -o yaml

# Test connectivity from pod
kubectl exec -it deploy/fuse -- curl http://localhost:9400/api/fuse/health
```
