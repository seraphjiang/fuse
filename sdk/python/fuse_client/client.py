# SPDX-License-Identifier: Apache-2.0

"""Fuse client — query, explain, health, trace, streaming."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, List, Optional
from urllib.request import Request, urlopen
from urllib.error import HTTPError


@dataclass
class QueryResult:
    """Result of a Fuse query."""
    columns: List[str]
    rows: List[List[Any]]
    total_rows: int
    format: str
    trace_id: str
    datasources_queried: Optional[List[str]] = None
    next_cursor: Optional[str] = None

    def to_dicts(self) -> List[Dict[str, Any]]:
        """Convert rows to list of dicts."""
        return [dict(zip(self.columns, row)) for row in self.rows]

    def to_dataframe(self):
        """Convert to pandas DataFrame. Requires pandas."""
        import pandas as pd
        return pd.DataFrame(self.rows, columns=self.columns)


@dataclass
class TraceResult:
    """Result of a trace reconstruction."""
    trace_id: str
    spans: List[Dict[str, Any]]
    datasources_searched: List[str]
    datasources_matched: List[str]
    total_spans: int
    search_ms: int


class FuseClient:
    """Client for the Fuse federated query engine REST API."""

    def __init__(self, base_url: str = "http://localhost:3000", api_key: Optional[str] = None):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key

    def _headers(self) -> Dict[str, str]:
        h = {"Content-Type": "application/json"}
        if self.api_key:
            h["x-api-key"] = self.api_key
        return h

    def _request(self, method: str, path: str, body: Optional[dict] = None) -> Any:
        url = f"{self.base_url}{path}"
        data = json.dumps(body).encode() if body else None
        req = Request(url, data=data, headers=self._headers(), method=method)
        try:
            with urlopen(req) as resp:
                return json.loads(resp.read())
        except HTTPError as e:
            error_body = e.read().decode() if e.fp else str(e)
            raise FuseError(e.code, error_body) from e

    def query(
        self,
        sql: str,
        format: str = "sql",
        params: Optional[Dict[str, Any]] = None,
        page_size: Optional[int] = None,
        cursor: Optional[str] = None,
    ) -> QueryResult:
        """Execute a SQL or PPL query."""
        body: Dict[str, Any] = {"query": sql, "format": format}
        if params:
            body["params"] = params
        if page_size:
            body["page_size"] = page_size
        if cursor:
            body["cursor"] = cursor
        resp = self._request("POST", "/api/fuse/query", body)
        meta = resp.get("metadata", {})
        return QueryResult(
            columns=resp.get("columns", []),
            rows=resp.get("rows", []),
            total_rows=meta.get("total_rows", 0),
            format=meta.get("format", format),
            trace_id=meta.get("trace_id", ""),
            datasources_queried=meta.get("datasources_queried"),
            next_cursor=resp.get("next_cursor"),
        )

    def query_all(self, sql: str, format: str = "sql", page_size: int = 1000) -> QueryResult:
        """Query with automatic cursor pagination — fetches all pages."""
        first = self.query(sql, format=format, page_size=page_size)
        all_rows = list(first.rows)
        cursor = first.next_cursor
        while cursor:
            page = self.query(sql, format=format, page_size=page_size, cursor=cursor)
            all_rows.extend(page.rows)
            cursor = page.next_cursor
        first.rows = all_rows
        first.total_rows = len(all_rows)
        first.next_cursor = None
        return first

    def explain(self, sql: str, format: str = "sql") -> Dict[str, Any]:
        """Get execution plan for a query."""
        return self._request("POST", "/api/fuse/query/explain", {"query": sql, "format": format})

    def validate(self, sql: str, format: str = "sql") -> Dict[str, Any]:
        """Validate query syntax."""
        return self._request("POST", "/api/fuse/query/validate", {"query": sql, "format": format})

    def health(self) -> Dict[str, Any]:
        """Check connector health."""
        return self._request("GET", "/api/fuse/health")

    def datasources(self) -> List[Dict[str, Any]]:
        """List connected datasources."""
        return self._request("GET", "/api/fuse/datasources")

    def trace(self, trace_id: str) -> TraceResult:
        """Reconstruct a trace across all datasources."""
        resp = self._request("GET", f"/api/fuse/trace/{trace_id}")
        return TraceResult(
            trace_id=resp["trace_id"],
            spans=resp["spans"],
            datasources_searched=resp["datasources_searched"],
            datasources_matched=resp["datasources_matched"],
            total_spans=resp["total_spans"],
            search_ms=resp["search_ms"],
        )

    def history(self) -> List[Dict[str, Any]]:
        """Get query history."""
        return self._request("GET", "/api/fuse/history")

    def saved_queries(self) -> List[Dict[str, Any]]:
        """List saved queries."""
        return self._request("GET", "/api/fuse/saved")

    def save_query(self, name: str, sql: str, description: str = "") -> Dict[str, Any]:
        """Save a query."""
        return self._request("POST", "/api/fuse/saved", {"name": name, "query": sql, "description": description})

    def get_saved_query(self, name: str) -> Dict[str, Any]:
        """Get a saved query by name."""
        return self._request("GET", f"/api/fuse/saved/{name}")

    def delete_saved_query(self, name: str) -> Dict[str, Any]:
        """Delete a saved query."""
        return self._request("DELETE", f"/api/fuse/saved/{name}")

    def query_stream(self, sql: str, format: str = "sql") -> Iterator[Dict[str, Any]]:
        """Stream query results as NDJSON lines."""
        import urllib.request
        body = json.dumps({"query": sql, "format": format, "result_format": "ndjson"}).encode()
        req = urllib.request.Request(
            f"{self.base_url}/api/fuse/query",
            data=body, headers=self._headers(), method="POST",
        )
        with urllib.request.urlopen(req) as resp:
            for line in resp:
                line = line.strip()
                if line:
                    yield json.loads(line)


    def submit_async(self, sql: str, format: str = "sql") -> str:
        """Submit an async query. Returns job_id."""
        resp = self._request("POST", "/api/fuse/query/async", {"query": sql, "format": format})
        return resp["job_id"]

    def poll_async(self, job_id: str) -> Dict[str, Any]:
        """Poll async query status. Returns job with status/result."""
        return self._request("GET", f"/api/fuse/query/async/{job_id}")

    def cancel_async(self, job_id: str) -> Dict[str, Any]:
        """Cancel an async query."""
        return self._request("DELETE", f"/api/fuse/query/async/{job_id}")

    def wait_async(self, job_id: str, poll_interval: float = 0.5, timeout: float = 30.0) -> Dict[str, Any]:
        """Poll until async query completes or times out."""
        import time
        deadline = time.time() + timeout
        while time.time() < deadline:
            result = self.poll_async(job_id)
            status = result.get("status", "")
            if status in ("completed", "failed", "cancelled"):
                return result
            time.sleep(poll_interval)
        raise TimeoutError(f"async query {job_id} did not complete within {timeout}s")

    # ── Sprint 18: Webhooks (#1811) ──

    def webhooks(self) -> List[Dict[str, Any]]:
        """List webhook subscriptions."""
        return self._request("GET", "/api/fuse/webhooks")

    def create_webhook(self, name: str, query: str, condition: Dict[str, Any],
                       callback_url: str, format: str = "sql") -> Dict[str, Any]:
        """Create a webhook subscription."""
        return self._request("POST", "/api/fuse/webhooks", {
            "name": name, "query": query, "format": format,
            "condition": condition, "callback_url": callback_url,
        })

    def delete_webhook(self, webhook_id: str) -> Dict[str, Any]:
        """Delete a webhook subscription."""
        return self._request("DELETE", f"/api/fuse/webhooks/{webhook_id}")

    def test_webhook(self, webhook_id: str) -> Dict[str, Any]:
        """Test-fire a webhook: run query, evaluate condition, deliver if met."""
        return self._request("POST", f"/api/fuse/webhooks/{webhook_id}/test")

    # ── Sprint 18: Schema Relationships (#1831) ──

    def relationships(self) -> List[Dict[str, Any]]:
        """Discover cross-datasource foreign key relationships."""
        return self._request("GET", "/api/fuse/relationships")

    # ── Sprint 18: CDC (#1852) ──

    def cdc_status(self) -> Dict[str, Any]:
        """Get CDC tracker status and pending views."""
        return self._request("GET", "/api/fuse/cdc/status")

    def cdc_event(self, datasource: str, table: str,
                  change_type: str = "insert") -> Dict[str, Any]:
        """Ingest a change event to trigger materialized view refresh."""
        return self._request("POST", "/api/fuse/cdc/events", {
            "datasource": datasource, "table": table,
            "change_type": change_type,
            "timestamp": int(__import__("time").time()),
        })

    # ── Predictive Performance ──

    def predict(self, query: str) -> Dict[str, Any]:
        """Predict query latency based on historical data."""
        return self._request("GET", f"/api/fuse/predict?query={query}")


        super().__init__(f"HTTP {status_code}: {body}")

    # ── Scheduled Queries ──

    def schedules(self) -> list: return self._request("GET", "/api/fuse/schedules")
    def create_schedule(self, id: str, query: str, cron: str, format: str = "sql") -> dict:
        return self._request("POST", "/api/fuse/schedules", {"id": id, "query": query, "cron": cron, "format": format})
    def delete_schedule(self, id: str) -> None: self._request("DELETE", f"/api/fuse/schedules/{id}")

    # ── Data Quality Rules ──

    def quality_rules(self) -> list: return self._request("GET", "/api/fuse/quality/rules")
    def create_quality_rule(self, id: str, datasource: str, table: str, rule_type: str, **kwargs) -> dict:
        return self._request("POST", "/api/fuse/quality/rules", {"id": id, "datasource": datasource, "table": table, "rule_type": rule_type, **kwargs})
    def delete_quality_rule(self, id: str) -> None: self._request("DELETE", f"/api/fuse/quality/rules/{id}")

    # ── Query Lineage ──

    def lineage(self, query: str, format: str = "sql") -> dict:
        return self._request("POST", "/api/fuse/lineage", {"query": query, "format": format})

    # ── Query Replay ──

    def recordings(self) -> list: return self._request("GET", "/api/fuse/replay/recordings")
    def record_query(self, query: str, format: str = "sql") -> dict:
        return self._request("POST", "/api/fuse/replay/record", {"query": query, "format": format})
    def clear_recordings(self) -> None: self._request("DELETE", "/api/fuse/replay/recordings")

class FuseError(Exception):
    """Error from the Fuse API."""
    def __init__(self, status_code: int, body: str):
        self.status_code = status_code
        self.body = body
