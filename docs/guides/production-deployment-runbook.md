# Production Deployment Runbook

This runbook covers deploying Fuse to production, rolling back, and handling common incidents.

## Pre-Deployment Checklist

- [ ] All CI checks pass on the release commit
- [ ] `cargo test --all-targets` passes locally
- [ ] CHANGELOG updated with release notes
- [ ] Docker image built and tagged: `docker build -t ghcr.io/seraphjiang/fuse-server:<version> .`
- [ ] Image pushed to registry
- [ ] Config changes reviewed (fuse.toml / Helm values)
- [ ] Connector credentials verified in target environment

## Deployment Methods

### Docker Compose (single-node / staging)

```bash
# Pull latest image
docker compose -f docker-compose.prod.yml pull

# Rolling restart (zero-downtime with 2+ replicas)
docker compose -f docker-compose.prod.yml up -d --no-deps --build fuse

# Verify health
curl -sf http://localhost:9400/api/fuse/health | jq .
```

### Helm / Kubernetes

```bash
# Dry-run first
helm upgrade fuse deploy/helm/fuse \
  --namespace fuse --values deploy/helm/fuse/values.yaml \
  --set image.tag=<version> \
  --dry-run --diff

# Deploy
helm upgrade fuse deploy/helm/fuse \
  --namespace fuse --values deploy/helm/fuse/values.yaml \
  --set image.tag=<version> \
  --wait --timeout 5m

# Verify rollout
kubectl -n fuse rollout status deployment/fuse --timeout=3m
```

### Canary Deploy

```bash
# Deploy to canary (10% traffic) then promote
./deploy/canary-deploy.sh <version>
```

## Post-Deployment Verification

1. Health endpoint returns all connectors healthy:
   ```bash
   curl -sf http://<host>:9400/api/fuse/health | jq '.connectors[] | select(.healthy == false)'
   ```
   Expected: empty output (all healthy).

2. Smoke query succeeds:
   ```bash
   curl -sf -X POST http://<host>:9400/api/fuse/query \
     -H 'Content-Type: application/json' \
     -d '{"query": "SELECT 1 AS ok", "format": "sql"}' | jq .
   ```

3. Metrics endpoint responds:
   ```bash
   curl -sf http://<host>:9400/metrics | head -5
   ```

4. Check Grafana dashboard for anomalies: request rate, error rate, latency p99.

5. Verify no firing alerts:
   ```bash
   curl -sf http://<prometheus>:9090/api/v1/alerts | jq '.data.alerts[] | select(.labels.severity == "critical")'
   ```

## Rollback

### Helm

```bash
# List history
helm -n fuse history fuse

# Rollback to previous revision
helm -n fuse rollback fuse <revision> --wait --timeout 3m
```

### Docker Compose

```bash
# Pin previous image tag
docker compose -f docker-compose.prod.yml up -d --no-deps fuse
```

### Emergency: restart with known-good config

```bash
kubectl -n fuse rollout undo deployment/fuse
kubectl -n fuse rollout status deployment/fuse --timeout=3m
```

## Incident Response

### High Error Rate (> 5%)

1. Check which connector is failing:
   ```bash
   curl -sf http://<host>:9400/api/fuse/health | jq '.connectors[] | select(.healthy == false)'
   ```
2. Check server logs for error details:
   ```bash
   kubectl -n fuse logs -l app=fuse --tail=100 | grep -i error
   ```
3. If a single connector is down, it won't affect queries to other connectors. Consider disabling the connector in config if it causes cascading timeouts.

### High Latency (p99 > 10s)

1. Check active query count — if near `max_concurrent_queries` (64), queries are queuing.
2. Check if a specific connector is slow:
   ```bash
   curl -sf http://<host>:9400/api/fuse/health | jq '.connectors[] | .latency_ms'
   ```
3. Scale up replicas if CPU-bound:
   ```bash
   kubectl -n fuse scale deployment/fuse --replicas=<N>
   ```

### Instance Down

1. Check pod status: `kubectl -n fuse get pods`
2. Check events: `kubectl -n fuse describe pod <pod>`
3. Check OOM: `kubectl -n fuse top pods` — increase memory limits if needed.
4. Check liveness probe: the `/api/fuse/health` endpoint must return 200.

### Rate Limiting Triggered

1. Check which scope is rejecting: global, per-IP, or per-API-key.
2. Adjust limits in config if legitimate traffic spike:
   ```yaml
   config:
     rate_limit_global: 2000
     rate_limit_per_ip: 200
   ```
3. Redeploy with updated values.

## Monitoring Links

- Grafana dashboard: `http://<grafana>:3000/d/fuse-cluster`
- Prometheus alerts: `http://<prometheus>:9090/alerts`
- Server health: `http://<host>:9400/api/fuse/health`
- Metrics: `http://<host>:9400/metrics`
