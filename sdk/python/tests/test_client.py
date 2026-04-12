import pytest
# SPDX-License-Identifier: Apache-2.0

"""Tests for fuse_client — unit tests (no server required)."""

import json
from unittest.mock import patch, MagicMock
from fuse_client import FuseClient
from fuse_client.client import QueryResult, TraceResult, FuseError


def test_client_init_defaults():
    c = FuseClient()
    assert c.base_url == "http://localhost:3000"
    assert c.api_key is None


def test_client_init_custom():
    c = FuseClient("http://fuse:8080/", api_key="key-123")
    assert c.base_url == "http://fuse:8080"
    assert c.api_key == "key-123"


def test_headers_no_key():
    c = FuseClient()
    h = c._headers()
    assert "x-api-key" not in h
    assert h["Content-Type"] == "application/json"


def test_headers_with_key():
    c = FuseClient(api_key="abc")
    h = c._headers()
    assert h["x-api-key"] == "abc"


def test_query_result_to_dicts():
    r = QueryResult(columns=["a", "b"], rows=[[1, 2], [3, 4]], total_rows=2, format="sql", trace_id="t1")
    dicts = r.to_dicts()
    assert dicts == [{"a": 1, "b": 2}, {"a": 3, "b": 4}]


def test_query_result_empty():
    r = QueryResult(columns=[], rows=[], total_rows=0, format="sql", trace_id="t1")
    assert r.to_dicts() == []


def test_trace_result():
    t = TraceResult(
        trace_id="abc", spans=[{"ds": "a"}],
        datasources_searched=["a", "b"], datasources_matched=["a"],
        total_spans=1, search_ms=5,
    )
    assert t.total_spans == 1
    assert len(t.datasources_matched) == 1


def test_fuse_error():
    e = FuseError(401, '{"error":"unauthorized"}')
    assert e.status_code == 401
    assert "401" in str(e)


def test_query_result_cursor():
    r = QueryResult(columns=["x"], rows=[[1]], total_rows=1, format="sql", trace_id="t", next_cursor="fuse_c_1")
    assert r.next_cursor == "fuse_c_1"


def test_query_result_to_dataframe():
    """Test to_dataframe with pandas."""
    pytest.importorskip("pandas")
    import pandas as pd
    r = QueryResult(columns=["x", "y"], rows=[[1, "a"], [2, "b"]], total_rows=2, format="sql", trace_id="t")
    df = r.to_dataframe()
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["x", "y"]
    assert len(df) == 2


def test_saved_queries_methods_exist():
    """Verify saved query methods are defined."""
    c = FuseClient()
    assert hasattr(c, "saved_queries")
    assert hasattr(c, "save_query")
    assert hasattr(c, "get_saved_query")
    assert hasattr(c, "delete_saved_query")


def test_query_stream_method_exists():
    """Verify streaming method is defined."""
    c = FuseClient()
    assert hasattr(c, "query_stream")
    assert callable(c.query_stream)


def test_async_methods_exist():
    c = FuseClient()
    assert hasattr(c, "submit_async")
    assert hasattr(c, "poll_async")
    assert hasattr(c, "cancel_async")
    assert hasattr(c, "wait_async")


# ── Sprint 18 method tests (mock-based) ──

def _mock_response(body, status=200):
    """Create a mock urllib response."""
    m = MagicMock()
    m.read.return_value = json.dumps(body).encode()
    m.__enter__ = lambda s: s
    m.__exit__ = MagicMock(return_value=False)
    return m


@patch("fuse_client.client.urlopen")
def test_webhooks(mock_urlopen):
    mock_urlopen.return_value = _mock_response([{"id": "w1", "name": "alert"}])
    c = FuseClient()
    ws = c.webhooks()
    assert len(ws) == 1
    assert ws[0]["id"] == "w1"


@patch("fuse_client.client.urlopen")
def test_create_webhook(mock_urlopen):
    mock_urlopen.return_value = _mock_response({"id": "w-new"})
    c = FuseClient()
    resp = c.create_webhook("alert", "SELECT count(*) FROM ds.logs", {"row_count_gt": 100}, "https://hook.example.com")
    assert resp["id"] == "w-new"
    sent = json.loads(mock_urlopen.call_args[0][0].data)
    assert sent["name"] == "alert"
    assert sent["callback_url"] == "https://hook.example.com"


@patch("fuse_client.client.urlopen")
def test_delete_webhook(mock_urlopen):
    mock_urlopen.return_value = _mock_response({"ok": True})
    c = FuseClient()
    c.delete_webhook("w-1")
    req = mock_urlopen.call_args[0][0]
    assert req.method == "DELETE"
    assert "/api/fuse/webhooks/w-1" in req.full_url


@patch("fuse_client.client.urlopen")
def test_test_webhook(mock_urlopen):
    mock_urlopen.return_value = _mock_response({"fired": True, "row_count": 42})
    c = FuseClient()
    resp = c.test_webhook("w-1")
    assert resp["fired"] is True
    assert resp["row_count"] == 42


@patch("fuse_client.client.urlopen")
def test_relationships(mock_urlopen):
    mock_urlopen.return_value = _mock_response([{"left_datasource": "a", "confidence": 0.8}])
    c = FuseClient()
    rels = c.relationships()
    assert len(rels) == 1
    assert rels[0]["confidence"] == 0.8


@patch("fuse_client.client.urlopen")
def test_cdc_status(mock_urlopen):
    mock_urlopen.return_value = _mock_response({"enabled": True, "tracked_views": 3})
    c = FuseClient()
    s = c.cdc_status()
    assert s["enabled"] is True


@patch("fuse_client.client.urlopen")
def test_cdc_event(mock_urlopen):
    mock_urlopen.return_value = _mock_response({"accepted": True, "affected_views": ["v1"]})
    c = FuseClient()
    resp = c.cdc_event("ds1", "users", "update")
    assert resp["accepted"] is True
    sent = json.loads(mock_urlopen.call_args[0][0].data)
    assert sent["datasource"] == "ds1"
    assert sent["change_type"] == "update"
    assert isinstance(sent["timestamp"], int)


@patch("fuse_client.client.urlopen")
def test_predict(mock_urlopen):
    mock_urlopen.return_value = _mock_response({"estimated_ms": 150, "confidence": "medium"})
    c = FuseClient()
    p = c.predict("SELECT * FROM ds.logs")
    assert p["estimated_ms"] == 150
    assert p["confidence"] == "medium"


@patch("fuse_client.client.urlopen")
def test_explain(mock_urlopen):
    mock_urlopen.return_value = _mock_response({"plan": "Scan: ds.logs"})
    c = FuseClient()
    e = c.explain("SELECT * FROM ds.logs")
    assert e["plan"] == "Scan: ds.logs"


@patch("fuse_client.client.urlopen")
def test_validate(mock_urlopen):
    mock_urlopen.return_value = _mock_response({"valid": True})
    c = FuseClient()
    v = c.validate("SELECT 1")
    assert v["valid"] is True


@patch("fuse_client.client.urlopen")
def test_datasources(mock_urlopen):
    mock_urlopen.return_value = _mock_response([{"id": "ds1", "type": "opensearch"}])
    c = FuseClient()
    ds = c.datasources()
    assert len(ds) == 1
    assert ds[0]["type"] == "opensearch"


@patch("fuse_client.client.urlopen")
def test_history(mock_urlopen):
    mock_urlopen.return_value = _mock_response([{"query": "SELECT 1", "latency_ms": 10}])
    c = FuseClient()
    h = c.history()
    assert len(h) == 1
    assert h[0]["query"] == "SELECT 1"


@patch("fuse_client.client.urlopen")
def test_query_with_ppl(mock_urlopen):
    mock_urlopen.return_value = _mock_response({
        "columns": ["x"], "rows": [[1]],
        "metadata": {"total_rows": 1, "format": "ppl", "trace_id": "t1"},
    })
    c = FuseClient()
    r = c.query("source = ds.logs | head 10", format="ppl")
    sent = json.loads(mock_urlopen.call_args[0][0].data)
    assert sent["format"] == "ppl"
    assert r.format == "ppl"
