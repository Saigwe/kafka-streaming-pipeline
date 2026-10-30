"""Pure windowing logic, testable without a broker."""


def window_start(epoch_seconds: int, size_seconds: int = 300) -> int:
    """Start of the tumbling window containing the timestamp."""
    return epoch_seconds - (epoch_seconds % size_seconds)


def aggregate(events: list) -> dict:
    """Per (window, depot): deliveries, on-time deliveries, on-time rate.

    TODO: implement, deduplicating on event_id.
    """
    raise NotImplementedError
