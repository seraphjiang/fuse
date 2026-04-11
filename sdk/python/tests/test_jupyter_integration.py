# SPDX-License-Identifier: Apache-2.0
"""#1630 — Jupyter magic integration test.
Run with: FUSE_URL=http://localhost:9400 pytest tests/test_jupyter_integration.py -v
Skips if FUSE_URL not set or server unreachable.
"""

import os
import json
import pytest
from unittest.mock import patch, MagicMock
from http.server import HTTPServer, BaseHTTPRequestHandler
from threading import Thread

from fuse_client.magic import FuseMagics, _get_client, _result_to_dataframe, load_ipython_extension
from fuse_client.client import FuseClient, QueryResult


class FuseMockHandler(BaseHTTPRequestHandler):
    """Mock Fuse server for integration testing."""
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length)) if length else {}
        query = body.get("query", "")

        if "JOIN" in query.upper():
            resp = {
                "columns": ["service", "name"],
                "rows": [["auth", "alice"], ["api", "bob"]],
                "metadata": {"total_rows": 2, "format": "sql", "trace_id": "t-join"},
            }
        elif "UNION" in query.upper():
            resp = {
                "columns": ["source", "value"],
                "rows": [["os", "1"], ["ddb", "2"], ["s3", "3"]],
                "metadata": {"total_rows": 3, "format": "sql", "trace_id": "t-union"},
            }
        else:
            resp = {
                "columns": ["result"],
                "rows": [["ok"]],
                "metadata": {"total_rows": 1, "format": body.get("format", "sql"), "trace_id": "t-1"},
            }

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(resp).encode())

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"status":"healthy"}')

    def log_message(self, *args):
        pass


@pytest.fixture(scope="module")
def mock_fuse():
    server = HTTPServer(("127.0.0.1", 0), FuseMockHandler)
    port = server.server_address[1]
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{port}"
    server.shutdown()


def test_magic_sql_query(mock_fuse):
    os.environ["FUSE_URL"] = mock_fuse
    try:
        magics = FuseMagics(shell=None)
        ns = {}
        with patch("fuse_client.magic.display"):
            df = magics.fuse("SELECT 1", local_ns=ns)
        assert df is not None
        assert "_fuse_result" in ns
        assert ns["_fuse_result"].trace_id == "t-1"
    finally:
        del os.environ["FUSE_URL"]


def test_magic_join_query(mock_fuse):
    os.environ["FUSE_URL"] = mock_fuse
    try:
        magics = FuseMagics(shell=None)
        ns = {}
        with patch("fuse_client.magic.display"):
            df = magics.fuse("SELECT * FROM a JOIN b ON a.id = b.id", local_ns=ns)
        result = ns["_fuse_result"]
        assert result.trace_id == "t-join"
        assert len(result.rows) == 2
    finally:
        del os.environ["FUSE_URL"]


def test_magic_cell_ppl(mock_fuse):
    os.environ["FUSE_URL"] = mock_fuse
    try:
        magics = FuseMagics(shell=None)
        ns = {}
        with patch("fuse_client.magic.display"):
            df = magics.fuse("ppl", cell="source = logs | head 5", local_ns=ns)
        assert df is not None
        assert ns["_fuse_result"].format == "ppl"
    finally:
        del os.environ["FUSE_URL"]


def test_magic_union_query(mock_fuse):
    os.environ["FUSE_URL"] = mock_fuse
    try:
        magics = FuseMagics(shell=None)
        ns = {}
        with patch("fuse_client.magic.display"):
            df = magics.fuse("SELECT * FROM a UNION ALL SELECT * FROM b", local_ns=ns)
        result = ns["_fuse_result"]
        assert result.trace_id == "t-union"
        assert len(result.rows) == 3
    finally:
        del os.environ["FUSE_URL"]


def test_client_direct_query(mock_fuse):
    client = FuseClient(base_url=mock_fuse)
    result = client.query("SELECT 1", "sql")
    assert result.total_rows == 1
    assert result.columns == ["result"]


def test_client_health(mock_fuse):
    client = FuseClient(base_url=mock_fuse)
    h = client.health()
    assert h["status"] == "healthy"
