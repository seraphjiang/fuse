#!/usr/bin/env python3
"""Compare two benchmark JSON files. Exit 1 if regression > threshold."""
import json, sys, os

threshold = float(os.environ.get("PERF_THRESHOLD", "15"))

with open(sys.argv[1]) as f:
    baseline = json.load(f)
with open(sys.argv[2]) as f:
    current = json.load(f)

if not baseline:
    print("❌ Baseline empty"); sys.exit(2)
if not current:
    print("❌ Current empty"); sys.exit(2)

def fmt(ns):
    if ns >= 1e6: return f"{ns/1e6:.1f}ms"
    if ns >= 1e3: return f"{ns/1e3:.1f}µs"
    return f"{ns:.0f}ns"

regressed = []
all_names = sorted(set(list(baseline) + list(current)))
print(f"Comparing {len(current)} benchmarks against {len(baseline)} baseline (threshold: {threshold}%)")
print()
print(f"{'Benchmark':<40} {'Base':>10} {'Current':>10} {'Delta':>8} {'':>3}")
print("─" * 75)

for name in all_names:
    b = baseline.get(name, {}).get("mean_ns")
    c = current.get(name, {}).get("mean_ns")
    if b is None:
        print(f"{name:<40} {'—':>10} {fmt(c):>10} {'new':>8} ℹ️"); continue
    if c is None:
        print(f"{name:<40} {fmt(b):>10} {'—':>10} {'gone':>8} ⚠️"); continue
    delta = (c - b) / b * 100
    status = "❌" if delta > threshold else "✅"
    if delta > threshold:
        regressed.append((name, delta))
    print(f"{name:<40} {fmt(b):>10} {fmt(c):>10} {delta:>+7.1f}% {status}")

print()
if regressed:
    print(f"❌ {len(regressed)} REGRESSIONS (>{threshold}%):")
    for n, p in regressed:
        print(f"   {n}: +{p:.1f}%")
    sys.exit(1)
else:
    print("✅ No regressions detected")
