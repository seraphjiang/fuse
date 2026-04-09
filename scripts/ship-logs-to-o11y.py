#!/usr/bin/env python3
"""
Ship Fuse server logs to S3 O11y for the pilot.

Reads structured logs from fuse-server (JSON lines) and ships them to the
S3 O11y bucket under the fuse/ prefix. Runs as a one-shot or continuous tail.

Usage:
  python3 scripts/ship-logs-to-o11y.py                  # ship last 100 lines
  python3 scripts/ship-logs-to-o11y.py --tail           # continuous tail
  python3 scripts/ship-logs-to-o11y.py --generate       # generate sample logs
"""

import argparse
import boto3
import gzip
import json
import os
import sys
import time
from datetime import datetime, timezone

BUCKET = "s3-query-logs-544277935543-us-west-1"
REGION = "us-west-1"
PREFIX = "fuse/logs"


def make_key() -> str:
    now = datetime.now(timezone.utc)
    return f"{PREFIX}/{now:%Y/%m/%d/%H}/{now:%Y%m%d%H%M%S%f}.json.gz"


def ship(logs: list[dict]) -> str:
    if not logs:
        return ""
    body = gzip.compress("\n".join(json.dumps(l) for l in logs).encode())
    key = make_key()
    s3 = boto3.client("s3", region_name=REGION)
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=body,
        ContentEncoding="gzip",
        ContentType="application/json",
    )
    return key


from typing import Optional


def parse_fuse_log_line(line: str) -> Optional[dict]:
    """Parse a fuse-server tracing log line into a structured dict."""
    line = line.strip()
    if not line:
        return None
    # Try JSON first (structured logging)
    try:
        return json.loads(line)
    except json.JSONDecodeError:
        pass
    # Fall back: parse tracing text format
    # e.g. "2026-04-09T19:00:00Z  INFO fuse_server: Query executed"
    now = datetime.now(timezone.utc).isoformat()
    level = "INFO"
    for lvl in ("ERROR", "WARN", "INFO", "DEBUG", "TRACE"):
        if lvl in line:
            level = lvl
            break
    return {
        "timestamp": now,
        "level": level,
        "service": "fuse-server",
        "message": line,
    }


def generate_sample_logs() -> list[dict]:
    """Generate realistic sample logs for the pilot demo."""
    now = datetime.now(timezone.utc).isoformat()
    return [
        {"timestamp": now, "level": "INFO",  "service": "fuse-server",    "message": "Fuse server started", "version": "0.1.0", "bind": "0.0.0.0:9400"},
        {"timestamp": now, "level": "INFO",  "service": "fuse-engine",    "message": "Query executed", "query": "SELECT * FROM cluster_a.services LIMIT 10", "duration_ms": 45, "rows": 10},
        {"timestamp": now, "level": "INFO",  "service": "fuse-engine",    "message": "Query executed", "query": "SELECT service, COUNT(*) FROM cluster_a.services WHERE status >= 500 GROUP BY service", "duration_ms": 120, "rows": 3},
        {"timestamp": now, "level": "ERROR", "service": "fuse-connector", "message": "Connection timeout", "connector": "cluster_b", "error": "timeout after 30s"},
        {"timestamp": now, "level": "INFO",  "service": "fuse-engine",    "message": "Federated query completed", "datasources": ["cluster_a", "cluster_b"], "total_rows": 42, "duration_ms": 230},
        {"timestamp": now, "level": "WARN",  "service": "fuse-cache",     "message": "Cache miss", "key": "cluster_a:services:SELECT *", "ttl_remaining_ms": 0},
        {"timestamp": now, "level": "INFO",  "service": "fuse-server",    "message": "Health check", "status": "healthy", "connectors": {"cluster_a": "healthy", "cluster_b": "degraded"}},
        {"timestamp": now, "level": "ERROR", "service": "fuse-connector", "message": "Schema discovery failed", "connector": "cluster_a", "error": "expected array from _cat/indices"},
        {"timestamp": now, "level": "INFO",  "service": "fuse-engine",    "message": "PPL query executed", "query": "source = cluster_a.services | where status >= 500 | stats count() by service", "duration_ms": 88, "rows": 5},
        {"timestamp": now, "level": "INFO",  "service": "fuse-server",    "message": "Query validated", "query": "SELECT * FROM cluster_a.services", "valid": True},
    ]


def tail_log_file(path: str, batch_size: int = 50, interval: float = 10.0):
    """Tail a log file and ship batches to S3 O11y."""
    print(f"Tailing {path}, shipping every {interval}s or {batch_size} lines")
    pending = []
    with open(path) as f:
        f.seek(0, 2)  # seek to end
        while True:
            line = f.readline()
            if line:
                parsed = parse_fuse_log_line(line)
                if parsed:
                    pending.append(parsed)
                if len(pending) >= batch_size:
                    key = ship(pending)
                    print(f"Shipped {len(pending)} logs → s3://{BUCKET}/{key}")
                    pending = []
            else:
                if pending:
                    key = ship(pending)
                    print(f"Shipped {len(pending)} logs → s3://{BUCKET}/{key}")
                    pending = []
                time.sleep(interval)


def main():
    parser = argparse.ArgumentParser(description="Ship Fuse logs to S3 O11y")
    parser.add_argument("--generate", action="store_true", help="Generate and ship sample logs")
    parser.add_argument("--tail", metavar="FILE", help="Tail a log file continuously")
    parser.add_argument("--file", metavar="FILE", help="Ship a log file once")
    args = parser.parse_args()

    if args.generate:
        logs = generate_sample_logs()
        key = ship(logs)
        print(f"Shipped {len(logs)} sample logs → s3://{BUCKET}/{key}")
        print(f"\nView at: https://d235elh6ccld6x.cloudfront.net")
        print(f"Query:   where level == \"ERROR\" | summarize count() by service")
        return

    if args.tail:
        tail_log_file(args.tail)
        return

    if args.file:
        logs = []
        with open(args.file) as f:
            for line in f:
                parsed = parse_fuse_log_line(line)
                if parsed:
                    logs.append(parsed)
        key = ship(logs)
        print(f"Shipped {len(logs)} logs → s3://{BUCKET}/{key}")
        return

    # Default: generate sample logs
    logs = generate_sample_logs()
    key = ship(logs)
    print(f"Shipped {len(logs)} sample logs → s3://{BUCKET}/{key}")
    print(f"\nView at: https://d235elh6ccld6x.cloudfront.net")


if __name__ == "__main__":
    main()
