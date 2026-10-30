"""Simulated parcel event stream with realistic problems: duplicates, malformed messages, late arrivals,
and an incident at one depot."""
from __future__ import annotations

import json
import random
from datetime import datetime, timedelta, timezone

DEPOTS = ["LIS", "OPO", "MAD", "BCN"]
START = datetime(2026, 10, 1, 8, 0, tzinfo=timezone.utc)
INCIDENT_DEPOT, INCIDENT_FROM, INCIDENT_TO = "OPO", timedelta(hours=3), timedelta(hours=4, minutes=30)


def simulate(hours: int = 6, per_minute: int = 80, seed: int = 9) -> list[str]:
    """Return raw JSON messages in arrival order."""
    rng = random.Random(seed)
    messages: list[tuple[datetime, str]] = []
    n = 0
    for minute in range(hours * 60):
        for _ in range(per_minute):
            n += 1
            t = START + timedelta(minutes=minute, seconds=rng.randint(0, 59))
            depot = rng.choice(DEPOTS)
            in_incident = depot == INCIDENT_DEPOT and INCIDENT_FROM <= t - START < INCIDENT_TO
            p_fail, p_late = (0.20, 0.35) if in_incident else (0.04, 0.06)
            r = rng.random()
            status = "failed_attempt" if r < p_fail else rng.choice(["delivered"] * 6 + ["out_for_delivery"])
            promised = t + timedelta(minutes=-rng.randint(1, 60) if rng.random() < p_late else rng.randint(5, 120))
            event = {"event_id": f"e{n}", "parcel_id": f"p{rng.randint(1, 50_000)}", "depot": depot,
                     "status": status, "event_time": t.isoformat(), "promised_time": promised.isoformat()}
            arrival = t
            if rng.random() < 0.01:          # arrives 30 minutes late (beyond allowed lateness)
                arrival = t + timedelta(minutes=30)
            messages.append((arrival, json.dumps(event)))
            if rng.random() < 0.03:          # at-least-once redelivery
                messages.append((arrival + timedelta(seconds=rng.randint(1, 90)), json.dumps(event)))
            if rng.random() < 0.005:         # malformed producer output
                messages.append((arrival, json.dumps({**event, "status": "teleported"})))
    messages.sort(key=lambda m: m[0])
    return [m for _, m in messages]
