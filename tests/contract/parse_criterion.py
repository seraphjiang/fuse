#!/usr/bin/env python3
"""Parse criterion benchmark output into JSON."""
import re, json, sys

results = {}
last_name = None
with open(sys.argv[1]) as f:
    for line in f:
        # Named line: "bench_name  time:   [lo mid hi]"
        m = re.match(r'^(\S+)\s+time:\s+\[[\d.]+ \w+ ([\d.]+) (\w+) [\d.]+ \w+\]', line)
        if m:
            last_name = m.group(1)
            val, unit = float(m.group(2)), m.group(3)
        else:
            # Continuation: "                        time:   [lo mid hi]"
            m = re.match(r'^\s+time:\s+\[[\d.]+ \w+ ([\d.]+) (\w+) [\d.]+ \w+\]', line)
            if m and last_name:
                # Skip continuation — it's a sub-variant without a distinct name
                continue
            else:
                # Track "Benchmarking X" lines for name context
                m2 = re.match(r'^Benchmarking (\S+)$', line)
                if m2:
                    last_name = m2.group(1)
                continue
        mult = {"ns": 1, "\u00b5s": 1000, "us": 1000, "ms": 1e6, "s": 1e9}.get(unit, 1)
        results[last_name] = {"mean_ns": val * mult}

with open(sys.argv[2], "w") as f:
    json.dump(results, f, indent=2)
print(f"Parsed {len(results)} benchmarks -> {sys.argv[2]}")
