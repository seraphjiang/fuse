#!/bin/bash
# Fuse Hive Watchdog — pokes idle agents every 5 minutes to pick up backlog work
# Usage: ./scripts/watchdog.sh &
# Stop: kill %1 (or kill the PID)

SESSION="${KIRO_HIVE_SESSION:-xds}"
INTERVAL=300  # 5 minutes
SELF="${KIRO_HIVE_AGENT:-sisyphus}"

echo "[watchdog] Started. Session=$SESSION, interval=${INTERVAL}s, self=$SELF"

while true; do
    sleep $INTERVAL

    # Get agent status
    STATUS=$(kiro-hive status 2>/dev/null)

    # Extract agents and their last activity
    while IFS= read -r agent; do
        # Skip self
        [ "$agent" = "$SELF" ] && continue

        # Check if agent line shows idle (no "Thinking" or "Creating" in the next line)
        AGENT_STATUS=$(echo "$STATUS" | grep -A1 "\] $agent " | tail -1)

        if echo "$AGENT_STATUS" | grep -qE "Thinking|Creating|Generating|Reading|Searching|Running"; then
            echo "[watchdog] $(date +%H:%M:%S) $agent is active, skipping"
        else
            echo "[watchdog] $(date +%H:%M:%S) $agent is idle, poking"
            kiro-hive tell "$agent" "[PROGRESS] Watchdog check-in. If you're idle, review .fuse-project/backlog/backlog.md for unassigned todo items and pick one up. Report what you're working on to sisyphus." 2>/dev/null
        fi
    done <<< "$(echo "$STATUS" | grep -oP '\] \K\S+(?= - active)' | grep -v "$SELF")"

    echo "[watchdog] $(date +%H:%M:%S) cycle complete"
done
