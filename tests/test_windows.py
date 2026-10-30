from datetime import datetime, timedelta, timezone

import pytest

from stream.alerts import DropDetector
from stream.pipeline import Processor
from stream.schemas import validate
from stream.simulate import run
from stream.windows import WindowedAggregator, WindowResult, aggregate, window_start

T0 = datetime(2026, 10, 1, 8, 0, tzinfo=timezone.utc)


def raw(i, minute, status="delivered", depot="LIS", late=False):
    t = T0 + timedelta(minutes=minute)
    promised = t - timedelta(minutes=5) if late else t + timedelta(minutes=30)
    return {"event_id": f"e{i}", "parcel_id": f"p{i}", "depot": depot, "status": status,
            "event_time": t.isoformat(), "promised_time": promised.isoformat()}


def test_window_start_is_aligned():
    assert window_start(1_000_123, 300) == 1_000_123 - (1_000_123 % 300)
    assert window_start(600, 300) == 600


def test_validate_rejects_with_reason_codes():
    with pytest.raises(ValueError, match="missing_fields:depot"):
        validate({**raw(1, 0), "depot": ""})
    with pytest.raises(ValueError, match="invalid_status"):
        validate({**raw(1, 0), "status": "teleported"})
    assert validate(raw(1, 0)).depot == "LIS"


def test_aggregate_dedupes_and_counts_on_time():
    events = [validate(raw(1, 1)), validate(raw(1, 1)), validate(raw(2, 2, late=True)),
              validate(raw(3, 3, status="failed_attempt")), validate(raw(4, 4, status="in_transit"))]
    result = list(aggregate(events).values())[0]
    assert (result.attempts, result.delivered, result.on_time) == (3, 2, 1)
    assert result.on_time_rate == 33.3


def test_watermark_closes_windows_and_rejects_late_events():
    agg = WindowedAggregator()
    assert agg.add(validate(raw(1, 0))) == "counted"
    assert agg.add(validate(raw(2, 20))) == "counted"     # watermark = 08:10 -> 08:00 window closes
    closed = agg.close_ready()
    assert [r.window_start.minute for r in closed] == [0]
    assert agg.add(validate(raw(3, 1))) == "late"          # belongs to an already-closed window


def test_drop_detector_uses_depot_baseline():
    detector = DropDetector()
    for k in range(8):
        assert detector.check(WindowResult(T0 + timedelta(minutes=5 * k), "LIS", 40, 38, 36)) is None
    alert = detector.check(WindowResult(T0 + timedelta(minutes=45), "LIS", 40, 28, 24))
    assert alert is not None and alert.baseline_rate == 90.0 and alert.on_time_rate == 60.0


def test_processor_sends_bad_messages_to_dead_letters():
    p = Processor()
    p.handle(b"not json")
    p.handle(raw(1, 0) | {"status": "teleported"})
    p.handle(raw(2, 0))
    p.finish()
    assert [d["reason"] for d in p.dead_letters] == ["unparseable", "invalid_status:teleported"]
    assert p.sink.query("select attempts from kpi_5min") == [(1,)]


def test_simulation_detects_planted_incident():
    p = run()
    assert p.stats["duplicate"] > 0 and p.stats["late"] > 0 and p.stats["dead_letter"] > 0
    depots = {a.depot for a in p.alerts}
    assert depots == {"OPO"}
    assert all(timedelta(hours=3) <= a.window_start - T0 < timedelta(hours=4, minutes=30) for a in p.alerts)
