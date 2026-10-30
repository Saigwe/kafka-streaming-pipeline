"""Idempotent DuckDB sink for window results."""
from __future__ import annotations

from pathlib import Path

import duckdb

from .windows import WindowResult

DDL = """
create table if not exists kpi_5min (
    window_start timestamp, depot varchar, attempts integer, delivered integer, on_time integer,
    on_time_rate double, primary key (window_start, depot)
)
"""


class DuckDBSink:
    def __init__(self, path: str | Path = ":memory:"):
        self.con = duckdb.connect(str(path))
        self.con.execute(DDL)

    def write(self, results: list[WindowResult]) -> None:
        # insert or replace makes replays and reprocessing idempotent
        self.con.executemany(
            "insert or replace into kpi_5min values (?, ?, ?, ?, ?, ?)",
            [(r.window_start.replace(tzinfo=None), r.depot, r.attempts, r.delivered, r.on_time, r.on_time_rate)
             for r in results],
        )

    def query(self, sql: str) -> list[tuple]:
        return self.con.execute(sql).fetchall()
