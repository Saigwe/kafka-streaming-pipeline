"""Tumbling-window aggregation with deduplication and a watermark for late data. No broker needed."""
from __future__ import annotations

from collections import OrderedDict, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta

from .schemas import ParcelEvent

WINDOW = timedelta(minutes=5)
ALLOWED_LATENESS = timedelta(minutes=10)


def window_start(epoch_seconds: int, size_seconds: int = 300) -> int:
    """Start of the tumbling window containing the timestamp."""
    return epoch_seconds - (epoch_seconds % size_seconds)


def window_of(ts: datetime, size: timedelta = WINDOW) -> datetime:
    seconds = int(size.total_seconds())
    epoch = int(ts.timestamp())
    return datetime.fromtimestamp(window_start(epoch, seconds), tz=ts.tzinfo)


@dataclass
class WindowResult:
    window_start: datetime
    depot: str
    attempts: int
    delivered: int
    on_time: int

    @property
    def on_time_rate(self) -> float:
        return round(100 * self.on_time / self.attempts, 1) if self.attempts else 0.0


class WindowedAggregator:
    """Counts delivery outcomes per (5-minute window, depot).

    - Duplicates (same event_id, as produced by at-least-once delivery) are ignored.
    - The watermark is the latest event time seen minus the allowed lateness. Windows that end before the
      watermark are closed and emitted; events for an already-closed window are rejected as late.
    """

    def __init__(self, dedupe_capacity: int = 100_000):
        self._seen: OrderedDict[str, None] = OrderedDict()
        self._capacity = dedupe_capacity
        self._open: dict[tuple[datetime, str], list[int]] = defaultdict(lambda: [0, 0, 0])
        self._max_event_time: datetime | None = None
        self._closed_before: datetime | None = None

    def add(self, event: ParcelEvent) -> str:
        """Returns 'counted', 'duplicate', 'late' or 'ignored' (non-outcome statuses)."""
        if event.event_id in self._seen:
            return "duplicate"
        self._remember(event.event_id)
        w = window_of(event.event_time)
        if self._closed_before is not None and w < self._closed_before:
            return "late"
        if self._max_event_time is None or event.event_time > self._max_event_time:
            self._max_event_time = event.event_time
        if not event.is_delivery_outcome:
            return "ignored"
        counts = self._open[(w, event.depot)]
        counts[0] += 1
        counts[1] += event.status == "delivered"
        counts[2] += event.on_time
        return "counted"

    def close_ready(self) -> list[WindowResult]:
        """Emit and drop windows that end before the watermark."""
        if self._max_event_time is None:
            return []
        watermark = self._max_event_time - ALLOWED_LATENESS
        ready = [k for k in self._open if k[0] + WINDOW <= watermark]
        return self._emit(ready, window_of(watermark))

    def flush(self) -> list[WindowResult]:
        return self._emit(list(self._open), None)

    def _emit(self, keys, closed_before) -> list[WindowResult]:
        out = [WindowResult(w, depot, *self._open.pop((w, depot))) for w, depot in sorted(keys)]
        if closed_before is not None and (self._closed_before is None or closed_before > self._closed_before):
            self._closed_before = closed_before
        return out

    def _remember(self, event_id: str) -> None:
        self._seen[event_id] = None
        if len(self._seen) > self._capacity:
            self._seen.popitem(last=False)


def aggregate(events: list[ParcelEvent]) -> dict[tuple[datetime, str], WindowResult]:
    """Batch helper: aggregate a list of events (deduplicated) into window results."""
    agg = WindowedAggregator()
    for e in events:
        agg.add(e)
    return {(r.window_start, r.depot): r for r in agg.flush()}
