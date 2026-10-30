"""Broker-agnostic processing: validate -> dedupe -> window -> sink -> alert. The Kafka consumer and the offline
simulator both feed raw messages into the same Processor, so all logic is testable without Kafka."""
from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass, field

from .alerts import Alert, DropDetector
from .schemas import validate
from .sink import DuckDBSink
from .windows import WindowedAggregator


@dataclass
class Processor:
    sink: DuckDBSink = field(default_factory=DuckDBSink)
    aggregator: WindowedAggregator = field(default_factory=WindowedAggregator)
    detector: DropDetector = field(default_factory=DropDetector)
    dead_letters: list[dict] = field(default_factory=list)
    alerts: list[Alert] = field(default_factory=list)
    stats: Counter = field(default_factory=Counter)

    def handle(self, raw: bytes | str | dict) -> None:
        try:
            payload = raw if isinstance(raw, dict) else json.loads(raw)
            event = validate(payload)
        except json.JSONDecodeError:  # subclass of ValueError, so it must come first
            self.dead_letters.append({"reason": "unparseable", "payload": raw})
            self.stats["dead_letter"] += 1
            return
        except ValueError as exc:
            self.dead_letters.append({"reason": str(exc), "payload": raw})
            self.stats["dead_letter"] += 1
            return
        outcome = self.aggregator.add(event)
        self.stats[outcome] += 1
        if outcome == "late":
            self.dead_letters.append({"reason": "late", "payload": payload})
        self._emit(self.aggregator.close_ready())

    def finish(self) -> None:
        self._emit(self.aggregator.flush())

    def _emit(self, results) -> None:
        if not results:
            return
        self.sink.write(results)
        for r in results:
            if (alert := self.detector.check(r)):
                self.alerts.append(alert)
