# SPDX-License-Identifier: Apache-2.0

"""Async Fuse client — async/await interface using aiohttp."""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from .client import QueryResult

try:
    import aiohttp
except ImportError:
    aiohttp = None  # type: ignore[assignment]


class AsyncFuseClient:
    """Async client for the Fuse federated query engine REST API."""

    def __init__(self, base_url: str = "http://localhost:9400", api_key: Optional[str] = None):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self._session: Optional[aiohttp.ClientSession] = None

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if aiohttp is None:
            raise ImportError("aiohttp is required for AsyncFuseClient: pip install aiohttp")
        if self._session is None or self._session.closed:
            headers: Dict[str, str] = {"Content-Type": "application/json"}
            if self.api_key:
                headers["Authorization"] = f"Bearer {self.api_key}"
            self._session = aiohttp.ClientSession(headers=headers)
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def __aenter__(self) -> "AsyncFuseClient":
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

    async def _request(self, method: str, path: str, body: Any = None) -> Any:
        session = await self._ensure_session()
        url = f"{self.base_url}{path}"
        kwargs: Dict[str, Any] = {}
        if body is not None:
            kwargs["data"] = json.dumps(body)
        async with session.request(method, url, **kwargs) as resp:
            text = await resp.text()
            if resp.status >= 400:
                raise RuntimeError(f"HTTP {resp.status}: {text}")
            return json.loads(text) if text else None

    async def query(self, sql: str, format: str = "sql", page_size: Optional[int] = None,
                    cursor: Optional[str] = None) -> QueryResult:
        body: Dict[str, Any] = {"query": sql, "format": format}
        if page_size:
            body["page_size"] = page_size
        if cursor:
            body["cursor"] = cursor
        resp = await self._request("POST", "/api/fuse/query", body)
        return QueryResult(
            columns=resp.get("columns", []),
            rows=resp.get("rows", []),
            total_rows=resp.get("total_rows", len(resp.get("rows", []))),
            format=format,
            trace_id=resp.get("trace_id", ""),
            datasources_queried=resp.get("datasources_queried"),
            next_cursor=resp.get("next_cursor"),
        )

    async def health(self) -> Dict[str, Any]:
        return await self._request("GET", "/api/fuse/health")

    async def datasources(self) -> List[Dict[str, Any]]:
        return await self._request("GET", "/api/fuse/datasources")

    async def explain(self, sql: str, format: str = "sql") -> Dict[str, Any]:
        return await self._request("POST", "/api/fuse/query", {
            "query": f"EXPLAIN {sql}" if format == "sql" else sql,
            "format": format,
        })

    async def schemas(self, datasource_id: str) -> List[Dict[str, Any]]:
        return await self._request("GET", f"/api/fuse/datasources/{datasource_id}/schemas")

    async def fields(self, datasource_id: str, table: str) -> List[Dict[str, Any]]:
        return await self._request("GET", f"/api/fuse/datasources/{datasource_id}/schemas/{table}/fields")
