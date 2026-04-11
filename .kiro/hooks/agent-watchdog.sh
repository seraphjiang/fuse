#!/bin/bash
# Watchdog for xds hive session
# pm is team lead — all reports go to pm
# Pokes idle agents every 2 minutes to pick up backlog items

SESSION="${KIRO_HIVE_SESSION:-xds}"
INTERVAL=120
LEAD="pm"

echo "[watchdog] Monitoring session '$SESSION' every ${INTERVAL}s. Lead: $LEAD. Ctrl+C to stop."

while true; do
  for pane in $(tmux list-panes -s -t "$SESSION" -F "#{pane_id}:#{pane_title}" 2>/dev/null); do
    id=$(echo "$pane" | cut -d: -f1)
    name=$(echo "$pane" | cut -d: -f2)

    # Skip the lead — they coordinate, not get poked
    [[ "$name" == "$LEAD" || "$name" == "admin" ]] && continue

    last=$(tmux capture-pane -t "$id" -p -S -2 2>/dev/null)
    if echo "$last" | grep -qE "λ >$|λ > $"; then
      tmux send-keys -t "$id" "Check mailbox for new tasks from pm (team lead). If no tasks, review .fuse-project/backlog/sprint-16-backlog.md or .fuse-project/backlog/roadmap-ideas.md and pick up the next unassigned item. Report to pm what you chose." Enter
      echo "[watchdog] $(date +%H:%M:%S) Poked $name ($id)"
    fi
  done
  sleep "$INTERVAL"
done
