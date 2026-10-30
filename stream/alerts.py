"""On-time rate drop alerts per depot, against each depot's own rolling baseline."""
from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime

from .windows import WindowResult

MIN_ATTEMPTS = 40        # ignore thin windows: below ~40 outcomes, sampling noise alone moves the rate ~5 pts
DROP_POINTS = 12.0       # alert when on-time rate is this many points below baseline
BASELINE_WINDOWS = 12    # one hour of 5-minute windows


@dataclass(frozen=True)
class Alert:
    window_start: datetime
    depot: str
    on_time_rate: float
    baseline_rate: float

    def message(self) -> str:
        return (f"{self.depot}: on-time {self.on_time_rate}% in window {self.window_start:%H:%M}, "
                f"{self.baseline_rate - self.on_time_rate:.1f} pts below its {self.baseline_rate}% baseline")


class DropDetector:
    def __init__(self):
        self._history: dict[str, deque] = defaultdict(lambda: deque(maxlen=BASELINE_WINDOWS))

    def check(self, result: WindowResult) -> Alert | None:
        history = self._history[result.depot]
        alert = None
        if result.attempts >= MIN_ATTEMPTS and len(history) >= BASELINE_WINDOWS // 2:
            baseline = round(sum(history) / len(history), 1)
            if result.on_time_rate < baseline - DROP_POINTS:
                alert = Alert(result.window_start, result.depot, result.on_time_rate, baseline)
        if result.attempts >= MIN_ATTEMPTS and alert is None:
            history.append(result.on_time_rate)  # alerting windows don't drag the baseline down
        return alert
