#!/usr/bin/env bash
# Load Test Scenarios — spike, soak, stress patterns
#
# Usage:
#   ./scenarios_test.sh spike  [URL]   # sudden burst: 1→50→1 concurrency
#   ./scenarios_test.sh soak   [URL]   # steady 10c for 120s, watch for degradation
#   ./scenarios_test.sh stress [URL]   # ramp 5→10→20→40→80 until errors >10%
#   ./scenarios_test.sh all    [URL]   # run all three
#
# Env: SPIKE_PEAK=50, SOAK_DURATION=120, STRESS_MAX=80
set -euo pipefail

FUSE="${2:-${FUSE_URL:-https://fuse.huanji.profile.aws.dev}}"
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

QUERIES=(
  '{"query":"SELECT service, status FROM cluster_a.application_logs LIMIT 5","format":"sql"}'
  '{"query":"source = cluster_a.application_logs | head 5","format":"ppl"}'
  '{"query":"SELECT service FROM cluster_a.application_logs UNION ALL SELECT service FROM cluster_b.application_logs LIMIT 5","format":"sql"}'
  '{"query":"SELECT a.service, b.service FROM cluster_a.application_logs a JOIN cluster_b.application_logs b ON a.trace_id = b.trace_id LIMIT 3","format":"sql"}'
)
NQ=${#QUERIES[@]}

fire_batch() {
  local n="$1" tag="$2"
  local dir="$TMPDIR/$tag"
  mkdir -p "$dir"
  for i in $(seq 1 "$n"); do
    (
      local q="${QUERIES[$((RANDOM % NQ))]}"
      local start_ns=$(date +%s%N)
      local code
      code=$(curl -sk -o /dev/null -w "%{http_code}" --max-time 30 \
        -X POST "$FUSE/api/fuse/query" \
        -H "Content-Type: application/json" -d "$q" 2>/dev/null || echo "000")
      local end_ns=$(date +%s%N)
      echo "$(( (end_ns - start_ns) / 1000000 )) $code"
    ) >> "$dir/results.txt" &
  done
  wait
}

summarize() {
  local file="$1" label="$2"
  local total ok errs p50 p95 p99
  [ ! -s "$file" ] && { echo "  (no data)"; return; }
  total=$(wc -l < "$file")
  ok=$(awk '$2==200' "$file" | wc -l)
  errs=$((total - ok))
  p50=$(awk '$2==200{print $1}' "$file" | sort -n | awk -v n="$ok" 'NR==int(n*0.5+0.5)')
  p95=$(awk '$2==200{print $1}' "$file" | sort -n | awk -v n="$ok" 'NR==int(n*0.95+0.5)')
  p99=$(awk '$2==200{print $1}' "$file" | sort -n | awk -v n="$ok" 'NR==int(n*0.99+0.5)')
  local err_pct
  err_pct=$(python3 -c "print(f'{$errs/$total*100:.1f}')" 2>/dev/null || echo "?")
  printf "  %-12s  reqs=%-4d ok=%-4d err=%-3d (%s%%)  p50=%sms p95=%sms p99=%sms\n" \
    "$label" "$total" "$ok" "$errs" "$err_pct" "${p50:-?}" "${p95:-?}" "${p99:-?}"
}

# ═══════════════════════════════════════
# SPIKE: idle → peak → idle
# ═══════════════════════════════════════
run_spike() {
  local peak="${SPIKE_PEAK:-50}"
  echo "⚡ SPIKE TEST — 1 → $peak → 1 concurrency"
  echo ""

  # Warm-up
  fire_batch 2 "spike_warm"
  summarize "$TMPDIR/spike_warm/results.txt" "warm-up"

  # Spike
  fire_batch "$peak" "spike_peak"
  summarize "$TMPDIR/spike_peak/results.txt" "peak($peak)"

  # Cool-down (verify recovery)
  sleep 2
  fire_batch 2 "spike_cool"
  summarize "$TMPDIR/spike_cool/results.txt" "cool-down"

  # Verdict: peak errors <20%, cool-down works
  local peak_total peak_errs
  peak_total=$(wc -l < "$TMPDIR/spike_peak/results.txt")
  peak_errs=$(awk '$2!=200' "$TMPDIR/spike_peak/results.txt" | wc -l)
  local cool_errs
  cool_errs=$(awk '$2!=200' "$TMPDIR/spike_cool/results.txt" | wc -l)

  echo ""
  if [ "$peak_errs" -le $((peak_total / 5)) ] && [ "$cool_errs" -eq 0 ]; then
    echo "  ✅ SPIKE PASSED — peak errors ${peak_errs}/${peak_total}, recovery clean"
    return 0
  else
    echo "  ❌ SPIKE FAILED — peak errors ${peak_errs}/${peak_total}, cool-down errors ${cool_errs}"
    return 1
  fi
}

# ═══════════════════════════════════════
# SOAK: steady load, watch for degradation
# ═══════════════════════════════════════
run_soak() {
  local duration="${SOAK_DURATION:-120}" concurrency=10 interval=5
  echo "🔄 SOAK TEST — ${concurrency}c for ${duration}s"
  echo ""

  local start=$(date +%s) round=0
  while true; do
    local now=$(date +%s)
    [ $((now - start)) -ge "$duration" ] && break
    round=$((round + 1))
    fire_batch "$concurrency" "soak"

    # Progress every 30s
    local elapsed=$((now - start))
    if [ $((elapsed % 30)) -lt "$interval" ] && [ "$elapsed" -gt 0 ]; then
      summarize "$TMPDIR/soak/results.txt" "${elapsed}s"
    fi
    sleep "$interval"
  done

  echo ""
  summarize "$TMPDIR/soak/results.txt" "final"

  # Compare first-quarter vs last-quarter latencies for degradation
  local total first_q last_q_start
  total=$(wc -l < "$TMPDIR/soak/results.txt")
  first_q=$((total / 4))
  last_q_start=$((total - first_q))

  local early_p50 late_p50
  early_p50=$(awk '$2==200{print $1}' "$TMPDIR/soak/results.txt" | head -"$first_q" | sort -n | awk -v n="$first_q" 'NR==int(n*0.5+0.5)')
  late_p50=$(awk '$2==200{print $1}' "$TMPDIR/soak/results.txt" | tail -"$first_q" | sort -n | awk -v n="$first_q" 'NR==int(n*0.5+0.5)')

  local errs err_pct
  errs=$(awk '$2!=200' "$TMPDIR/soak/results.txt" | wc -l)
  err_pct=$(python3 -c "print(f'{$errs/$total*100:.1f}')" 2>/dev/null)

  echo "  Early p50: ${early_p50:-?}ms  Late p50: ${late_p50:-?}ms"

  local degraded=false
  if [ -n "$early_p50" ] && [ -n "$late_p50" ] && [ "$early_p50" -gt 0 ]; then
    local drift
    drift=$(python3 -c "print(f'{($late_p50 - $early_p50) / $early_p50 * 100:.0f}')")
    echo "  Latency drift: ${drift}%"
    if python3 -c "exit(0 if $drift > 50 else 1)" 2>/dev/null; then
      degraded=true
    fi
  fi

  echo ""
  if [ "$degraded" = false ] && python3 -c "exit(0 if float('$err_pct') < 10 else 1)" 2>/dev/null; then
    echo "  ✅ SOAK PASSED — error rate ${err_pct}%, no significant degradation"
    return 0
  else
    echo "  ❌ SOAK FAILED — error rate ${err_pct}%, degraded=$degraded"
    return 1
  fi
}

# ═══════════════════════════════════════
# STRESS: ramp until breaking point
# ═══════════════════════════════════════
run_stress() {
  local max="${STRESS_MAX:-80}"
  echo "💪 STRESS TEST — ramp 5→${max} until >10% errors"
  echo ""

  local breaking=0
  for c in 5 10 20 40 "$max"; do
    [ "$c" -gt "$max" ] && break
    fire_batch "$c" "stress_${c}"
    local total ok errs err_pct
    total=$(wc -l < "$TMPDIR/stress_${c}/results.txt")
    ok=$(awk '$2==200' "$TMPDIR/stress_${c}/results.txt" | wc -l)
    errs=$((total - ok))
    err_pct=$(python3 -c "print(f'{$errs/$total*100:.1f}')" 2>/dev/null)
    summarize "$TMPDIR/stress_${c}/results.txt" "${c}c"

    if python3 -c "exit(0 if float('$err_pct') > 10 else 1)" 2>/dev/null; then
      breaking=$c
      break
    fi
    sleep 1
  done

  echo ""
  if [ "$breaking" -gt 0 ]; then
    echo "  ⚠️  STRESS: breaking point at ${breaking} concurrency (>10% errors)"
    echo "  ✅ STRESS PASSED — breaking point identified"
  else
    echo "  ✅ STRESS PASSED — handled up to ${max}c with <10% errors"
  fi
  return 0
}

# ═══════════════════════════════════════
echo "╔═══════════════════════════════════════════════════╗"
echo "║  Load Test Scenarios                              ║"
echo "║  Target: $FUSE"
echo "║  Time:   $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "╚═══════════════════════════════════════════════════╝"
echo ""

FAILURES=0
case "${1:-all}" in
  spike)  run_spike  || FAILURES=$((FAILURES + 1)) ;;
  soak)   run_soak   || FAILURES=$((FAILURES + 1)) ;;
  stress) run_stress || FAILURES=$((FAILURES + 1)) ;;
  all)
    run_spike  || FAILURES=$((FAILURES + 1))
    echo ""; echo "───────────────────────────────────────"; echo ""
    run_stress || FAILURES=$((FAILURES + 1))
    echo ""; echo "───────────────────────────────────────"; echo ""
    SOAK_DURATION="${SOAK_DURATION:-60}" run_soak || FAILURES=$((FAILURES + 1))
    ;;
  *) echo "Usage: $0 {spike|soak|stress|all} [URL]"; exit 2 ;;
esac

echo ""
echo "═══════════════════════════════════════"
if [ "$FAILURES" -eq 0 ]; then
  echo "  ✅ All scenarios passed"
else
  echo "  ❌ $FAILURES scenario(s) failed"
  exit 1
fi
