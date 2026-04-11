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


class FuseError(Exception):
    """Error from the Fuse API."""
    def __init__(self, status_code: int, body: str):
        self.status_code = status_code
        self.body = body
        super().__init__(f"HTTP {status_code}: {body}")
