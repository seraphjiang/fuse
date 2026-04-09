#!/bin/bash
# Fuse Hive Watchdog — single run, meant to be called by cron every 5 min
# Pokes idle agents to pick up backlog work

export PATH="$HOME/.local/bin:$HOME/bin:/usr/local/bin:/usr/bin:/bin:$PATH"
SESSION="xds"
SELF="sisyphus"
LOG="/tmp/fuse-watchdog.log"

echo "[watchdog] $(date '+%Y-%m-%d %H:%M:%S') running" >> "$LOG"

STATUS=$(kiro-hive status 2>/dev/null)
if [ $? -ne 0 ]; then
    echo "[watchdog] hive not available, skipping" >> "$LOG"
    exit 0
fi

for agent in $(echo "$STATUS" | grep -oP '\] \K\S+(?= - active)'); do
    [ "$agent" = "$SELF" ] && continue

    NEXT_LINE=$(echo "$STATUS" | grep -A1 "\] $agent " | tail -1)

    if echo "$NEXT_LINE" | grep -qE "Thinking|Creating|Generating|Reading|Searching|Running"; then
        echo "[watchdog] $agent is active, skipping" >> "$LOG"
    else
        echo "[watchdog] $agent is idle, poking" >> "$LOG"
        kiro-hive tell "$agent" --session "$SESSION" "[PROGRESS] Watchdog check-in. If idle, review .fuse-project/backlog/backlog.md for unassigned todo items and pick one up. Report to sisyphus what you chose." >> "$LOG" 2>&1
    fi
done

echo "[watchdog] cycle complete" >> "$LOG"
