"""Event schema and validation."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

VALID_STATUSES = {"collected", "in_transit", "out_for_delivery", "delivered", "failed_attempt"}
REQUIRED = ("event_id", "parcel_id", "depot", "status", "event_time", "promised_time")


@dataclass(frozen=True)
class ParcelEvent:
    event_id: str
    parcel_id: str
    depot: str
    status: str
    event_time: datetime
    promised_time: datetime

    @property
    def is_delivery_outcome(self) -> bool:
        return self.status in {"delivered", "failed_attempt"}

    @property
    def on_time(self) -> bool:
        return self.status == "delivered" and self.event_time <= self.promised_time


def validate(raw: dict) -> ParcelEvent:
    """Parse and validate a raw event dict. Raises ValueError with a reason code for the dead-letter topic."""
    missing = [k for k in REQUIRED if not raw.get(k)]
    if missing:
        raise ValueError(f"missing_fields:{','.join(missing)}")
    if raw["status"] not in VALID_STATUSES:
        raise ValueError(f"invalid_status:{raw['status']}")
    try:
        event_time = datetime.fromisoformat(raw["event_time"])
        promised_time = datetime.fromisoformat(raw["promised_time"])
    except (TypeError, ValueError):
        raise ValueError("invalid_timestamp") from None
    return ParcelEvent(raw["event_id"], raw["parcel_id"], raw["depot"].upper(), raw["status"], event_time,
                       promised_time)
