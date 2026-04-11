#!/bin/bash
# Canary deployment for Fuse on ECS
# Deploys to 1 instance, health checks, then rolls to full service.
#
# Usage: ./deploy/canary-deploy.sh [IMAGE_TAG]
# Defaults to 'latest' if no tag provided.

set -euo pipefail

CLUSTER="fuse-playground"
SERVICE="fuse-server"
REGION="us-west-2"
HEALTH_URL="https://fuse.huanji.profile.aws.dev/api/fuse/health"
IMAGE_TAG="${1:-latest}"
MAX_WAIT=300
POLL_INTERVAL=15

log() { echo "[$(date -u +%H:%M:%S)] $*"; }

# Step 1: Record current state
CURRENT_DESIRED=$(aws ecs describe-services --cluster "$CLUSTER" --services "$SERVICE" \
  --region "$REGION" --query 'services[0].desiredCount' --output text)
log "Current desired count: $CURRENT_DESIRED"

# Step 2: Scale to 1 canary instance with new image
log "Deploying canary (force new deployment, desired=1)..."
aws ecs update-service --cluster "$CLUSTER" --service "$SERVICE" \
  --desired-count 1 --force-new-deployment \
  --region "$REGION" --query 'service.taskDefinition' --output text

# Step 3: Wait for new task to be running
log "Waiting for canary task to stabilize..."
aws ecs wait services-stable --cluster "$CLUSTER" --services "$SERVICE" \
  --region "$REGION" 2>/dev/null || true

# Step 4: Health check the canary
log "Health checking canary..."
ELAPSED=0
HEALTHY=false
while [ $ELAPSED -lt $MAX_WAIT ]; do
  STATUS=$(curl -sf "$HEALTH_URL" --connect-timeout 5 2>/dev/null | \
    python3 -c "import sys,json; print(json.load(sys.stdin).get('status',''))" 2>/dev/null || echo "unreachable")
  if [ "$STATUS" = "healthy" ]; then
    HEALTHY=true
    break
  fi
  log "  Status: $STATUS (${ELAPSED}s elapsed)"
  sleep $POLL_INTERVAL
  ELAPSED=$((ELAPSED + POLL_INTERVAL))
done

if [ "$HEALTHY" != "true" ]; then
  log "❌ Canary FAILED health check after ${MAX_WAIT}s. Rolling back..."
  aws ecs update-service --cluster "$CLUSTER" --service "$SERVICE" \
    --desired-count "$CURRENT_DESIRED" \
    --region "$REGION" --query 'service.desiredCount' --output text
  log "Rolled back to $CURRENT_DESIRED instances."
  exit 1
fi

VERSION=$(curl -sf "$HEALTH_URL" --connect-timeout 5 | \
  python3 -c "import sys,json; print(json.load(sys.stdin).get('version','?'))" 2>/dev/null)
CONNECTORS=$(curl -sf "$HEALTH_URL" --connect-timeout 5 | \
  python3 -c "import sys,json; print(len(json.load(sys.stdin).get('connectors',{})))" 2>/dev/null)
log "✅ Canary healthy — v${VERSION}, ${CONNECTORS} connectors"

# Step 5: Roll to full deployment
log "Scaling to $CURRENT_DESIRED instances..."
aws ecs update-service --cluster "$CLUSTER" --service "$SERVICE" \
  --desired-count "$CURRENT_DESIRED" \
  --region "$REGION" --query 'service.desiredCount' --output text

log "✅ Canary deploy complete. v${VERSION} rolling to $CURRENT_DESIRED instances."
