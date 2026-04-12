#!/usr/bin/env bash
# Watchdog — periodically pings active hive agents to pick up tasks.
# Usage: ./scripts/hive-watchdog.sh [interval_seconds]
# Default interval: 1800 (30 minutes)

INTERVAL="${1:-1800}"
SESSION="xds"
AGENTS=(security ai-lead sde fee)

echo "[watchdog] Monitoring ${#AGENTS[@]} agents every ${INTERVAL}s in session '$SESSION'"

while true; do
    for agent in "${AGENTS[@]}"; do
        kiro-hive tell "$agent" "[HEARTBEAT] Pick from backlog if idle. Report status to pm." 2>/dev/null
    done
    echo "[watchdog] $(date -Iseconds) — pinged ${AGENTS[*]}"
    sleep "$INTERVAL"
done
