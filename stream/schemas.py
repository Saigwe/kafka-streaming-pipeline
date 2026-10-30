"""Event schema and validation."""
from dataclasses import dataclass
from datetime import datetime

VALID_STATUSES = {"collected", "in_transit", "out_for_delivery", "delivered", "failed_attempt"}


@dataclass(frozen=True)
class ParcelEvent:
    event_id: str
    parcel_id: str
    depot: str
    status: str
    event_time: datetime
    promised_date: str


def validate(raw: dict) -> ParcelEvent:
    """Parse and validate a raw event dict. Raise ValueError with a reason for the dead-letter topic.

    TODO: implement required-field, status and timestamp checks.
    """
    raise NotImplementedError
