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
