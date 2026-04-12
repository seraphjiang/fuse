#!/bin/bash
# Watchdog for xds hive session
# Pokes idle agents, restarts dead ones (max 3 restarts/hour/agent)

SESSION="${KIRO_HIVE_SESSION:-xds}"
INTERVAL=120
LEAD="pm"
MAX_RESTARTS=15
RESTART_WINDOW=3600  # 1 hour in seconds
RESTART_LOG="/tmp/watchdog-restarts.log"

touch "$RESTART_LOG"

echo "[watchdog] Monitoring '$SESSION' every ${INTERVAL}s. Lead: $LEAD. Max $MAX_RESTARTS restarts/hr/agent."

count_recent_restarts() {
  local agent="$1"
  local cutoff=$(($(date +%s) - RESTART_WINDOW))
  awk -v agent="$agent" -v cutoff="$cutoff" '$1 > cutoff && $2 == agent' "$RESTART_LOG" | wc -l
}

log_restart() {
  echo "$(date +%s) $1" >> "$RESTART_LOG"
  # Prune old entries
  local cutoff=$(($(date +%s) - RESTART_WINDOW))
  awk -v cutoff="$cutoff" '$1 > cutoff' "$RESTART_LOG" > "${RESTART_LOG}.tmp" && mv "${RESTART_LOG}.tmp" "$RESTART_LOG"
}

while true; do
  for pane in $(tmux list-panes -s -t "$SESSION" -F "#{pane_id}:#{pane_title}" 2>/dev/null); do
    id=$(echo "$pane" | cut -d: -f1)
    name=$(echo "$pane" | cut -d: -f2)

    [[ "$name" == "$LEAD" || "$name" == "admin" || "$name" == "fee" ]] && continue

    last=$(tmux capture-pane -t "$id" -p -S -3 2>/dev/null)

    # Case 1: Agent at idle prompt — poke it
    if echo "$last" | grep -qE "λ >$|λ > $"; then
      tmux send-keys -t "$id" "Check mailbox for new tasks from pm (team lead). If no tasks, review .fuse-project/backlog/roadmap-ideas.md and pick up the next unassigned item. Report to pm what you chose." Enter
      echo "[watchdog] $(date +%H:%M:%S) Poked $name"
      continue
    fi

    # Case 2: Agent at builder prompt (idle/crashed) — restart if under limit
    if echo "$last" | grep -qE '\[amzn-builder\].*% >'; then
      recent=$(count_recent_restarts "$name")
      if [ "$recent" -lt "$MAX_RESTARTS" ]; then
        echo "[watchdog] $(date +%H:%M:%S) $name appears dead (builder prompt). Restarting ($((recent+1))/$MAX_RESTARTS this hour)"
        kiro-hive kill "$name" 2>/dev/null
        kiro-hive spawn "$name" 2>/dev/null
        sleep 2
        kiro-hive tell "$name" "You were restarted by watchdog. Check mailbox, pick from .fuse-project/backlog/roadmap-ideas.md, report to pm." 2>/dev/null
        log_restart "$name"
      else
        echo "[watchdog] $(date +%H:%M:%S) $name dead but hit restart limit ($MAX_RESTARTS/hr). Skipping."
      fi
      continue
    fi

    # Case 3: Empty output (no prompt at all) — likely crashed hard
    trimmed=$(echo "$last" | tr -d '[:space:]')
    if [ -z "$trimmed" ]; then
      recent=$(count_recent_restarts "$name")
      if [ "$recent" -lt "$MAX_RESTARTS" ]; then
        echo "[watchdog] $(date +%H:%M:%S) $name appears dead (empty output). Restarting ($((recent+1))/$MAX_RESTARTS this hour)"
        kiro-hive kill "$name" 2>/dev/null
        kiro-hive spawn "$name" 2>/dev/null
        sleep 2
        kiro-hive tell "$name" "You were restarted by watchdog. Check mailbox, pick from .fuse-project/backlog/roadmap-ideas.md, report to pm." 2>/dev/null
        log_restart "$name"
      else
        echo "[watchdog] $(date +%H:%M:%S) $name dead but hit restart limit ($MAX_RESTARTS/hr). Skipping."
      fi
    fi
  done
  sleep "$INTERVAL"
done
