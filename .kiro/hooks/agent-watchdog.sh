#!/bin/bash
SESSION="${KIRO_HIVE_SESSION:-xds}"
INTERVAL=120
echo "[watchdog] Monitoring session '$SESSION' every ${INTERVAL}s. Ctrl+C to stop."
while true; do
  for pane in $(tmux list-panes -s -t "$SESSION" -F "#{pane_id}:#{pane_title}" 2>/dev/null); do
    id=$(echo "$pane" | cut -d: -f1)
    name=$(echo "$pane" | cut -d: -f2)
    [[ "$name" == "pm" || "$name" == "admin" ]] && continue
    last=$(tmux capture-pane -t "$id" -p -S -2 2>/dev/null)
    if echo "$last" | grep -qE "λ >$|λ > $"; then
      tmux send-keys -t "$id" "Check mailbox for new tasks. If no tasks from mailbox, review .fuse-project/backlog/sprint-16-backlog.md for unassigned todo items and pick one up." Enter
      echo "[watchdog] $(date +%H:%M:%S) Poked $name ($id)"
    fi
  done
  sleep "$INTERVAL"
done
