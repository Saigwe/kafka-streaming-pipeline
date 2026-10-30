"""Run the whole pipeline offline (no broker): python -m stream.simulate"""
from __future__ import annotations

from pathlib import Path

from .events import simulate
from .pipeline import Processor
from .sink import DuckDBSink

ROOT = Path(__file__).resolve().parent.parent


def run(db_path: str | Path = ":memory:") -> Processor:
    processor = Processor(sink=DuckDBSink(db_path))
    for message in simulate():
        processor.handle(message)
    processor.finish()
    return processor


def report(p: Processor) -> str:
    rows = p.sink.query("""
        select depot, count(*) windows, sum(attempts) attempts,
               round(100.0 * sum(on_time) / sum(attempts), 1) on_time_pct
        from kpi_5min group by depot order by depot""")
    lines = ["# Streaming run summary", "", "## Message handling", "", "| Outcome | Messages |", "|---|---|"]
    lines += [f"| {k} | {v} |" for k, v in sorted(p.stats.items())]
    lines += ["", "## On-time rate by depot", "", "| Depot | Windows | Attempts | On-time % |", "|---|---|---|---|"]
    lines += [f"| {d} | {w} | {a} | {r} |" for d, w, a, r in rows]
    lines += ["", f"## Alerts ({len(p.alerts)})", ""] + [f"- {a.message()}" for a in p.alerts]
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    out = ROOT / "output"
    out.mkdir(exist_ok=True)
    processor = run(out / "kpis.duckdb")
    text = report(processor)
    (out / "run_summary.md").write_text(text, encoding="utf-8")
    print(text)
