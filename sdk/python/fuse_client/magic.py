# SPDX-License-Identifier: Apache-2.0

"""Jupyter magic command for Fuse — %fuse and %%fuse.

Usage:
    %load_ext fuse_client.magic
    %fuse SELECT * FROM cluster_a.logs LIMIT 10
    %%fuse
    SELECT l.service, count(*)
    FROM cluster_a.logs l
    JOIN dynamodb.users u ON l.user_id = u.user_id
    GROUP BY l.service
"""

from __future__ import annotations

import os
from typing import Optional

from IPython.core.magic import Magics, magics_class, line_cell_magic, needs_local_scope
from IPython.display import display

from .client import FuseClient, FuseError


def _get_client() -> FuseClient:
    url = os.environ.get("FUSE_URL", "http://localhost:9400")
    key = os.environ.get("FUSE_API_KEY")
    return FuseClient(base_url=url, api_key=key)


def _result_to_dataframe(result):
    """Convert QueryResult to pandas DataFrame if available, else dict list."""
    try:
        import pandas as pd
        return pd.DataFrame(result.rows, columns=result.columns)
    except ImportError:
        return result.to_dicts()


@magics_class
class FuseMagics(Magics):
    """Jupyter magics for querying Fuse."""

    @line_cell_magic
    @needs_local_scope
    def fuse(self, line: str, cell: str = None, local_ns=None):
        """Line: %fuse SELECT ... | Cell: %%fuse [ppl]\nSELECT ..."""
        if cell is not None:
            query = cell.strip()
            fmt = "ppl" if line.strip() == "ppl" else "sql"
        else:
            query = line.strip()
            fmt = "sql"
        if not query:
            print("Usage: %fuse <SQL query>  or  %%fuse\n<multi-line query>")
            return None
        return self._run(query, local_ns, fmt=fmt)

    def _run(self, query: str, local_ns=None, fmt: str = "sql"):
        if not query:
            print("Usage: %fuse <SQL query>  or  %%fuse\\n<multi-line query>")
            return None
        client = _get_client()
        try:
            result = client.query(query, format=fmt)
        except FuseError as e:
            print(f"Fuse error: {e}")
            return None

        df = _result_to_dataframe(result)
        if local_ns is not None:
            local_ns["_fuse_result"] = result
            local_ns["_"] = df
        display(df)
        return df


def load_ipython_extension(ipython):
    """Called by %load_ext fuse_client.magic."""
    ipython.register_magics(FuseMagics)
