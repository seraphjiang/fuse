#!/usr/bin/env python3
"""
Fuse Sprint 3 — Correlated demo data seeder.

Creates consistent data across all datasources:
- DynamoDB: user_profiles (50 users)
- OpenSearch: application_logs with user_id (update existing clusters)
- S3: access_logs.parquet (Parquet files with matching user_ids + trace_ids)
- CloudWatch: lambda_logs (JSON structured logs)

All data shares:
- 50 user_ids (user-001 to user-050)
- 30 trace_ids (shared across OpenSearch + S3 + CloudWatch)
- Timestamps within 2026-04-09 00:00 - 23:59 UTC
"""

import boto3
import json
import hashlib
import random
import time
from datetime import datetime, timedelta
from decimal import Decimal

random.seed(42)  # Reproducible

REGION = "us-west-2"
ACCOUNT = "544277935543"
BASE_TIME = datetime(2026, 4, 9, 0, 0, 0)

# --- Shared entities ---
USERS = [f"user-{i:03d}" for i in range(1, 51)]
TRACE_IDS = [f"tr-{hashlib.md5(f'shared-{i}'.encode()).hexdigest()[:12]}" for i in range(30)]
SERVICES = ["api-gateway", "auth-service", "user-service", "order-service", "payment-service", "notification-service"]
PLANS = ["free", "pro", "enterprise"]
REGIONS_USER = ["us-west-2", "us-east-1", "eu-west-1", "ap-southeast-1"]

def rand_ts():
    return BASE_TIME + timedelta(seconds=random.randint(0, 86399))

def ts_iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

# ============================================================
# 1. DynamoDB — user_profiles
# ============================================================
def seed_dynamodb():
    ddb = boto3.resource("dynamodb", region_name=REGION)

    # Create table if not exists
    client = boto3.client("dynamodb", region_name=REGION)
    try:
        client.describe_table(TableName="fuse_user_profiles")
        print("DynamoDB table fuse_user_profiles exists")
    except client.exceptions.ResourceNotFoundException:
        print("Creating DynamoDB table fuse_user_profiles...")
        client.create_table(
            TableName="fuse_user_profiles",
            KeySchema=[{"AttributeName": "user_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "user_id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        client.get_waiter("table_exists").wait(TableName="fuse_user_profiles")
        print("Table created")

    table = ddb.Table("fuse_user_profiles")
    names = [
        "Alice Chen", "Bob Smith", "Carol Wu", "David Kim", "Eva Martinez",
        "Frank Lee", "Grace Patel", "Henry Zhang", "Iris Johnson", "Jack Brown",
        "Karen Liu", "Leo Garcia", "Mia Wang", "Noah Davis", "Olivia Taylor",
        "Paul Wilson", "Quinn Moore", "Rachel Anderson", "Sam Thomas", "Tina Jackson",
        "Uma White", "Victor Harris", "Wendy Martin", "Xavier Thompson", "Yuki Robinson",
        "Zara Clark", "Adam Lewis", "Beth Walker", "Chris Hall", "Diana Allen",
        "Eric Young", "Fiona King", "George Wright", "Hannah Scott", "Ian Green",
        "Julia Baker", "Kevin Adams", "Laura Nelson", "Mike Hill", "Nina Campbell",
        "Oscar Mitchell", "Penny Roberts", "Ryan Carter", "Sophie Phillips", "Tom Evans",
        "Ursula Turner", "Vince Parker", "Wanda Collins", "Xander Edwards", "Yvonne Stewart",
    ]

    with table.batch_writer() as batch:
        for i, uid in enumerate(USERS):
            batch.put_item(Item={
                "user_id": uid,
                "name": names[i],
                "email": f"{names[i].lower().replace(' ', '.')}@example.com",
                "plan": random.choice(PLANS),
                "region": random.choice(REGIONS_USER),
                "signup_date": ts_iso(BASE_TIME - timedelta(days=random.randint(1, 365))),
                "request_count": random.randint(10, 5000),
                "active": random.random() > 0.1,
            })
    print(f"DynamoDB: seeded {len(USERS)} user profiles")

# ============================================================
# 2. OpenSearch — add user_id to existing logs + new logs
# ============================================================
def seed_opensearch_user_ids():
    """Add user_id-enriched logs to existing AOSS clusters."""
    import requests
    from requests_aws4auth import AWS4Auth

    session = boto3.Session(region_name=REGION)
    creds = session.get_credentials().get_frozen_credentials()
    auth = AWS4Auth(creds.access_key, creds.secret_key, REGION, "aoss", session_token=creds.token)

    endpoints = {
        "cluster_a": "https://epk7ap540halh4ufyff6.us-west-2.aoss.amazonaws.com",
        "cluster_b": "https://wg2fj60hpfsc9ziwv0u0.us-west-2.aoss.amazonaws.com",
    }

    for cluster, endpoint in endpoints.items():
        docs = []
        svcs = SERVICES[:3] if cluster == "cluster_a" else SERVICES[3:]
        for i in range(100):
            uid = random.choice(USERS)
            svc = random.choice(svcs)
            status = random.choices([200, 201, 301, 400, 401, 403, 404, 500, 503], weights=[50, 10, 5, 8, 5, 3, 8, 7, 4])[0]
            dt = rand_ts()
            tid = random.choice(TRACE_IDS) if random.random() < 0.6 else f"tr-{hashlib.md5(f'{i}-{random.random()}'.encode()).hexdigest()[:12]}"
            docs.append({
                "timestamp": ts_iso(dt),
                "service": svc,
                "status": status,
                "method": random.choice(["GET", "POST", "PUT", "DELETE"]),
                "path": random.choice(["/api/users", "/api/orders", "/api/auth/login", "/api/payments", "/api/notifications"]),
                "duration_ms": random.randint(1, 200) if status < 400 else random.randint(100, 3000),
                "message": f"{'OK' if status < 400 else 'Error'} {status}",
                "trace_id": tid,
                "user_id": uid,
                "host": f"ip-10-0-{random.randint(1,3)}-{random.randint(1,254)}",
            })

        # Bulk index
        bulk_body = ""
        for doc in docs:
            bulk_body += json.dumps({"index": {"_index": "application_logs"}}) + "\n"
            bulk_body += json.dumps(doc) + "\n"

        resp = requests.post(
            f"{endpoint}/_bulk",
            auth=auth,
            headers={"Content-Type": "application/x-ndjson"},
            data=bulk_body,
        )
        errors = resp.json().get("errors", True)
        print(f"OpenSearch {cluster}: indexed {len(docs)} docs (errors={errors})")

# ============================================================
# 3. S3 — access_logs.parquet
# ============================================================
def seed_s3_parquet():
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        print("S3 Parquet: skipping (pyarrow not installed). Install with: pip install pyarrow")
        return

    s3 = boto3.client("s3", region_name="us-west-1")
    bucket = f"s3-query-logs-{ACCOUNT}-us-west-1"

    rows = []
    for i in range(200):
        uid = random.choice(USERS)
        dt = rand_ts()
        tid = random.choice(TRACE_IDS) if random.random() < 0.5 else f"tr-local-{i:04d}"
        rows.append({
            "timestamp": ts_iso(dt),
            "user_id": uid,
            "trace_id": tid,
            "action": random.choice(["page_view", "api_call", "login", "logout", "purchase", "search"]),
            "resource": random.choice(["/dashboard", "/settings", "/orders", "/products", "/checkout"]),
            "duration_ms": random.randint(5, 500),
            "status": random.choices([200, 301, 400, 404, 500], weights=[70, 5, 10, 10, 5])[0],
            "bytes": random.randint(100, 50000),
            "region": random.choice(REGIONS_USER),
        })

    table = pa.table({
        "timestamp": pa.array([r["timestamp"] for r in rows], type=pa.string()),
        "user_id": pa.array([r["user_id"] for r in rows]),
        "trace_id": pa.array([r["trace_id"] for r in rows]),
        "action": pa.array([r["action"] for r in rows]),
        "resource": pa.array([r["resource"] for r in rows]),
        "duration_ms": pa.array([r["duration_ms"] for r in rows], type=pa.int32()),
        "status": pa.array([r["status"] for r in rows], type=pa.int32()),
        "bytes": pa.array([r["bytes"] for r in rows], type=pa.int32()),
        "region": pa.array([r["region"] for r in rows]),
    })

    local_path = "/tmp/access_logs.parquet"
    pq.write_table(table, local_path)
    s3.upload_file(local_path, bucket, "fuse/demo/access_logs.parquet")
    print(f"S3: uploaded {len(rows)} rows to s3://{bucket}/fuse/demo/access_logs.parquet")

# ============================================================
# 4. CloudWatch — lambda execution logs
# ============================================================
def seed_cloudwatch():
    logs = boto3.client("logs", region_name=REGION)
    log_group = "/aws/lambda/fuse-demo-processor"

    try:
        logs.create_log_group(logGroupName=log_group)
        print(f"Created log group {log_group}")
    except logs.exceptions.ResourceAlreadyExistsException:
        print(f"Log group {log_group} exists")

    stream = "2026/04/09/demo"
    try:
        logs.create_log_stream(logGroupName=log_group, logStreamName=stream)
    except logs.exceptions.ResourceAlreadyExistsException:
        pass

    events = []
    for i in range(50):
        uid = random.choice(USERS)
        tid = random.choice(TRACE_IDS) if random.random() < 0.5 else f"tr-lambda-{i:03d}"
        dt = rand_ts()
        event = {
            "level": random.choices(["INFO", "WARN", "ERROR"], weights=[80, 15, 5])[0],
            "service": "fuse-demo-processor",
            "user_id": uid,
            "trace_id": tid,
            "action": random.choice(["process_order", "send_notification", "update_profile", "generate_report"]),
            "duration_ms": random.randint(10, 2000),
            "memory_mb": random.randint(64, 512),
            "cold_start": random.random() < 0.15,
        }
        events.append({
            "timestamp": int(dt.timestamp() * 1000),
            "message": json.dumps(event),
        })

    events.sort(key=lambda e: e["timestamp"])

    # CloudWatch has a 1MB limit per batch, send in chunks of 25
    for chunk_start in range(0, len(events), 25):
        chunk = events[chunk_start:chunk_start + 25]
        try:
            logs.put_log_events(logGroupName=log_group, logStreamName=stream, logEvents=chunk)
        except Exception as e:
            # Need sequence token for subsequent puts
            resp = logs.describe_log_streams(logGroupName=log_group, logStreamNamePrefix=stream)
            token = resp["logStreams"][0].get("uploadSequenceToken")
            if token:
                logs.put_log_events(logGroupName=log_group, logStreamName=stream, logEvents=chunk, sequenceToken=token)

    print(f"CloudWatch: seeded {len(events)} lambda log events")

# ============================================================
if __name__ == "__main__":
    print("=== Fuse Sprint 3 Demo Data Seeder ===\n")
    seed_dynamodb()
    seed_opensearch_user_ids()
    seed_s3_parquet()
    seed_cloudwatch()
    print("\n=== Done! ===")
    print(f"\nCorrelation keys:")
    print(f"  Users: {len(USERS)} (user-001 to user-050)")
    print(f"  Shared trace_ids: {len(TRACE_IDS)}")
    print(f"  Time range: {ts_iso(BASE_TIME)} to {ts_iso(BASE_TIME + timedelta(days=1))}")
    print(f"\nDemo queries:")
    print(f"  JOIN logs + profiles: SELECT l.service, l.status, u.name, u.plan FROM cluster_a.application_logs l JOIN dynamodb.fuse_user_profiles u ON l.user_id = u.user_id LIMIT 10")
    print(f"  Cross-source trace:   SELECT * FROM cluster_a.application_logs WHERE trace_id = '{TRACE_IDS[0]}' UNION ALL SELECT * FROM cluster_b.application_logs WHERE trace_id = '{TRACE_IDS[0]}'")
