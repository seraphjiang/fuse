# Fuse v2.0 Performance Baseline

**Date:** 2026-04-12 | **Sprint:** 18 | **Profile:** bench (optimized)

## fuse-server

| Benchmark | Median | Range |
|-----------|--------|-------|
| plan_cache_get_hit | 675 ns | 629–720 ns |
| plan_cache_get_miss | 618 ns | 510–752 ns |
| auto_suggest_5_columns | 3.59 µs | 3.14–4.07 µs |
| autocomplete_20_schemas | 9.60 µs | 9.43–9.82 µs |
| anomaly_detect | 1.64 µs | 1.53–1.74 µs |
| query_advisor_complex | 380 ns | 368–393 ns |

## fuse-engine

| Benchmark | Median | Range |
|-----------|--------|-------|
| ppl_parse/simple | 452 ns | 438–469 ns |
| ppl_parse/complex | 1.47 µs | 1.41–1.53 µs |
| ppl_to_sql/simple | 480 ns | 445–512 ns |
| ppl_to_sql/complex | 2.84 µs | 2.58–3.19 µs |
| hash_join/1K | 1.17 ms | 1.04–1.32 ms |
| hash_join/10K | 31.7 ms | 27.4–36.3 ms |
| hash_join/100K | 2.95 s | 2.76–3.16 s |
| extract_join_keys/1K | 1.28 ms | 1.14–1.43 ms |
| extract_join_keys/10K | 4.95 ms | 4.57–5.36 ms |
| extract_join_keys/100K | 59.8 ms | 52.7–67.5 ms |
| union_batches_4x10K | 1.78 µs | 1.51–2.09 µs |
| sort_batches_40K | 528 µs | 425–647 µs |
| schema_alignment/matching | 478 ns | 405–555 ns |
| schema_alignment/different | 6.54 µs | 5.52–7.62 µs |

## Regression threshold: >20% from median.
