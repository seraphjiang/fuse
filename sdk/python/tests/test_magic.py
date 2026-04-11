# SPDX-License-Identifier: Apache-2.0

"""Tests for Fuse Jupyter magic command."""

import os
import json
from http.server import HTTPServer, BaseHTTPRequestHandler
from threading import Thread
from unittest.mock import MagicMock, patch

import pytest

from fuse_client.magic import FuseMagics, _get_client, _result_to_dataframe, load_ipython_extension
from fuse_client.client import QueryResult


class MockHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length)) if length else {}
        resp = {
            "columns": ["id", "name"],
            "rows": [[1, "alice"], [2, "bob"]],
            "metadata": {"total_rows": 2, "format": "sql", "trace_id": "t-1"},
        }
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(resp).encode())

    def log_message(self, *args):
        pass  # Suppress logs


@pytest.fixture(scope="module")
def mock_server():
    server = HTTPServer(("127.0.0.1", 0), MockHandler)
    port = server.server_address[1]
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{port}"
    server.shutdown()


def test_get_client_defaults():
    client = _get_client()
    assert client.base_url == "http://localhost:9400"
    assert client.api_key is None


def test_get_client_from_env():
    os.environ["FUSE_URL"] = "http://custom:1234"
    os.environ["FUSE_API_KEY"] = "secret"
    try:
        client = _get_client()
        assert client.base_url == "http://custom:1234"
        assert client.api_key == "secret"
    finally:
        del os.environ["FUSE_URL"]
        del os.environ["FUSE_API_KEY"]


def test_result_to_dataframe_without_pandas():
    result = QueryResult(
        columns=["a", "b"], rows=[[1, 2]], total_rows=1,
        format="sql", trace_id="t"
    )
    with patch.dict("sys.modules", {"pandas": None}):
        out = _result_to_dataframe(result)
    assert out == [{"a": 1, "b": 2}]


def test_result_to_dataframe_with_pandas():
    pytest.importorskip("pandas")
    import pandas as pd
    result = QueryResult(
        columns=["x", "y"], rows=[[1, 2], [3, 4]], total_rows=2,
        format="sql", trace_id="t"
    )
    df = _result_to_dataframe(result)
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["x", "y"]
    assert len(df) == 2


def test_load_extension():
    ipython = MagicMock()
    load_ipython_extension(ipython)
    ipython.register_magics.assert_called_once_with(FuseMagics)


def test_line_magic_empty():
    magics = FuseMagics(shell=None)
    result = magics.fuse("", local_ns={})
    assert result is None


def test_line_magic_with_server(mock_server):
    os.environ["FUSE_URL"] = mock_server
    try:
        magics = FuseMagics(shell=None)
        ns = {}
        with patch("fuse_client.magic.display"):
            df = magics.fuse("SELECT 1", local_ns=ns)
        assert df is not None
        assert "_fuse_result" in ns
    finally:
        del os.environ["FUSE_URL"]
