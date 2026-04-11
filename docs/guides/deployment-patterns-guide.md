# Deployment Patterns Guide

Four deployment patterns from development to enterprise scale.

## 1. Single Node

```
┌──────────────────────┐
│     Fuse Server      │
│  (binary or Docker)  │
└──────────┬───────────┘
           │
    [All Datasources]
```

**Use for:** Development, small teams (< 10 users), evaluation.

### Binary

```bash
cargo build --release
./target/release/fuse-server --config fuse.toml
```

### Docker

```bash
docker run -d --name fuse \
  -p 9400:9400 \
  -v ./fuse.toml:/etc/fuse/fuse.toml:ro \
  ghcr.io/seraphjiang/fuse:1.0.0
```

### Docker Compose (with OpenSearch)

```yaml
version: "3.8"
services:
  opensearch:
    image: opensearchproject/opensearch:2.17.0
    environment:
      - discovery.type=single-node
      - DISABLE_SECURITY_PLUGIN=true
    ports: ["9200:9200"]

  fuse:
    image: ghcr.io/seraphjiang/fuse:1.0.0
    ports: ["9400:9400"]
    volumes: [./fuse.toml:/etc/fuse/fuse.toml:ro]
    depends_on: [opensearch]
```

## 2. Multi-Node (Docker Compose)

```
        ┌──────────┐
        │  nginx   │ :9400
        └────┬─────┘
        ┌────┴────┐
        ▼         ▼
   ┌────────┐ ┌────────┐
   │ Fuse 1 │ │ Fuse 2 │  stateless
   └────┬───┘ └───┬────┘
        └────┬────┘
             ▼
        ┌────────┐
        │ Redis  │  cache + tenants
        └────────┘
```

**Use for:** Medium teams (10–100 users), production without K8s.

```yaml
version: "3.8"
services:
  redis:
    image: redis:7-alpine
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s

  fuse-1: &fuse
    image: ghcr.io/seraphjiang/fuse:1.0.0
    volumes: [./fuse.stateless.toml:/etc/fuse/fuse.toml:ro]
    environment:
      - REDIS_URL=redis://redis:6379
    depends_on: { redis: { condition: service_healthy } }

  fuse-2: *fuse

  nginx:
    image: nginx:alpine
    ports: ["9400:9400"]
    volumes: [./nginx.conf:/etc/nginx/nginx.conf:ro]
    depends_on: [fuse-1, fuse-2]
```

Scale up: `docker compose up -d --scale fuse=4`

See [Horizontal Scaling Guide](./horizontal-scaling-guide.md) for full nginx config and Redis setup.

## 3. Kubernetes

```
        ┌──────────────┐
        │   Ingress    │ :443
        └──────┬───────┘
               ▼
        ┌──────────────┐
        │   Service    │ ClusterIP :9400
        └──────┬───────┘
        ┌──────┴──────┐
        ▼      ▼      ▼
   ┌──────┐┌──────┐┌──────┐
   │Pod 1 ││Pod 2 ││Pod 3 │  Deployment (replicas: 3)
   └──────┘└──────┘└──────┘
               │
        ┌──────┴──────┐
        │    Redis     │  StatefulSet
        └─────────────┘
```

**Use for:** Large teams (100+ users), auto-scaling, production.

### Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: fuse
spec:
  replicas: 3
  selector:
    matchLabels: { app: fuse }
  template:
    metadata:
      labels: { app: fuse }
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "9400"
        prometheus.io/path: "/metrics"
    spec:
      containers:
        - name: fuse
          image: ghcr.io/seraphjiang/fuse:1.0.0
          ports:
            - containerPort: 9400
          env:
            - name: FUSE_CONFIG
              value: /etc/fuse/fuse.toml
            - name: REDIS_URL
              valueFrom:
                secretKeyRef: { name: fuse-secrets, key: redis-url }
            - name: FUSE_LOG_FORMAT
              value: json
          volumeMounts:
            - name: config
              mountPath: /etc/fuse
              readOnly: true
          resources:
            requests: { cpu: 500m, memory: 512Mi }
            limits: { cpu: 2000m, memory: 2Gi }
          livenessProbe:
            httpGet: { path: /api/fuse/health, port: 9400 }
            initialDelaySeconds: 5
            periodSeconds: 10
          readinessProbe:
            httpGet: { path: /api/fuse/health, port: 9400 }
            initialDelaySeconds: 3
            periodSeconds: 5
      volumes:
        - name: config
          configMap: { name: fuse-config }
```

### Service

```yaml
apiVersion: v1
kind: Service
metadata:
  name: fuse
spec:
  selector: { app: fuse }
  ports:
    - port: 9400
      targetPort: 9400
```

### ConfigMap

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: fuse-config
data:
  fuse.toml: |
    [engine]
    mode = "stateless"
    bind = "0.0.0.0:9400"
    rate_limit_global = 5000
    rate_limit_per_ip = 500

    [redis]
    url = "${REDIS_URL}"
    cache_ttl_secs = 300

    [[datasource]]
    id = "cluster_a"
    type = "opensearch"
    url = "https://opensearch.default.svc:9200"
```

### Secrets

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: fuse-secrets
type: Opaque
stringData:
  redis-url: "redis://redis.default.svc:6379"
```

### HPA (Auto-Scaling)

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: fuse
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: fuse
  minReplicas: 2
  maxReplicas: 10
  metrics:
    - type: Resource
      resource:
        name: cpu
        target: { type: Utilization, averageUtilization: 70 }
```

### Ingress (TLS)

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: fuse
  annotations:
    nginx.ingress.kubernetes.io/ssl-redirect: "true"
spec:
  tls:
    - hosts: [fuse.internal.example.com]
      secretName: fuse-tls
  rules:
    - host: fuse.internal.example.com
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service: { name: fuse, port: { number: 9400 } }
```

## 4. Serverless (AWS Lambda)

```
API Gateway → Lambda (Fuse) → [Datasources]
```

**Use for:** Bursty workloads, pay-per-query, no infrastructure management.

### Lambda Configuration

```yaml
# SAM template
AWSTemplateFormatVersion: '2010-09-09'
Transform: AWS::Serverless-2016-10-31

Resources:
  FuseFunction:
    Type: AWS::Serverless::Function
    Properties:
      Handler: bootstrap
      Runtime: provided.al2023
      CodeUri: ./target/lambda/fuse-lambda/
      MemorySize: 1024
      Timeout: 30
      Environment:
        Variables:
          FUSE_CONFIG: /var/task/fuse.toml
          RUST_LOG: info
      Events:
        Api:
          Type: HttpApi
          Properties:
            Path: /{proxy+}
            Method: ANY
```

### Considerations

| Aspect | Lambda | Containers |
|--------|--------|-----------|
| Cold start | ~500ms (Rust binary) | None |
| Max duration | 15 minutes | Unlimited |
| Concurrency | Auto-scales to 1000+ | Manual scaling |
| State | Stateless (no Redis needed for cache) | Redis for shared state |
| Cost | Per-request | Per-hour |
| Materialized views | Not supported (no background tasks) | Supported |

Lambda is best for infrequent, bursty query patterns. For sustained load, use containers.

## Comparison

| Pattern | Users | Scaling | Complexity | Cost |
|---------|-------|---------|-----------|------|
| Single node | < 10 | None | Low | $ |
| Multi-node | 10–100 | Manual | Medium | $$ |
| Kubernetes | 100+ | Auto (HPA) | High | $$$ |
| Serverless | Bursty | Auto | Medium | Pay-per-use |

## Monitoring Across Patterns

All patterns expose the same endpoints:

```bash
# Health check (use for liveness/readiness probes)
GET /api/fuse/health

# Prometheus metrics (use for HPA, alerting, dashboards)
GET /metrics
```

See [Admin Guide](./admin-guide.md) for Grafana dashboard queries and alert rules.
