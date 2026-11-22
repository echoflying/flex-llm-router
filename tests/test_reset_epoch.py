"""Tests for parse_reset_epoch clock-time disambiguation.

Agnes and similar upstreams emit ``Try again after HH:MM:SS`` in *UTC* without
a zone marker.  Reading that as local wall-clock time rolls the instant a full
day forward and pins the channel for a multiple of the real backoff.  The
function must prefer the UTC reading when the local one lands absurdly far
ahead (>=6h) while the same wall clock read as UTC is imminent (<=6h).
"""
from datetime import datetime
from flex_llm_router.app import parse_reset_epoch


def _local(y, m, d, hh, mm, ss):
    """Epoch for a local wall-clock instant via the platform's own conversion."""
    import time
    return time.mktime(datetime(y, m, d, hh, mm, ss).timetuple())


def _utc(y, m, d, hh, mm, ss):
    import calendar
    return calendar.timegm(datetime(y, m, d, hh, mm, ss).timetuple())


def test_utc_clock_preferred_when_local_rolls_a_day_ahead(monkeypatch):
    """Beijing 12:00:35, upstream says 'Try again after 04:03:24' in UTC.

    Local reading -> tomorrow 04:03 (≈16h ahead, absurd for a 3-minute
    throttle); UTC reading -> +169s (matches the throttle).  Prefer UTC.
    """
    local_now = _local(2026, 11, 22, 12, 0, 35)
    monkeypatch.setattr('flex_llm_router.app.time.time', lambda: local_now)
    got = parse_reset_epoch('Try again after 04:03:24. (request id: 2026112204003557)')
    expected_utc = _utc(2026, 11, 22, 4, 3, 24)
    assert got is not None
    assert abs(got - expected_utc) < 2


def test_local_clock_kept_when_local_is_imminent(monkeypatch):
    """A genuinely local clock time just ahead is not shifted to UTC."""
    local_now = _local(2026, 11, 22, 12, 0, 35)
    monkeypatch.setattr('flex_llm_router.app.time.time', lambda: local_now)
    got = parse_reset_epoch('Try again after 12:03:24.')
    expected_local = _local(2026, 11, 22, 12, 3, 24)
    assert got is not None
    assert abs(got - expected_local) < 2


def test_absent_clock_returns_none():
    assert parse_reset_epoch('some unrelated error') is None
    assert parse_reset_epoch('') is None
