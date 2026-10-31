"""Daylight-saving and time-zone rules. US clocks in 2026: jump forward 8 Mar, fall back 1 Nov."""
from datetime import datetime

from scheduler.timeutil import next_fire, next_fires, UTC


def ts(*args) -> float:
    return datetime(*args, tzinfo=UTC).timestamp()


def iso(t: float) -> str:
    return datetime.fromtimestamp(t, UTC).strftime("%Y-%m-%d %H:%M")


def test_kolkata_10pm_roster_job():
    # 22:00 IST = 16:30 UTC (India has no DST)
    assert iso(next_fire("0 22 * * *", "Asia/Kolkata", ts(2026, 10, 31, 0, 0))) == "2026-10-31 16:30"


def test_gap_0230_runs_when_clock_jumps_to_0300():
    # New York, 8 Mar 2026: 02:00 EST -> 03:00 EDT. 02:30 does not exist.
    t = next_fire("30 2 * * *", "America/New_York", ts(2026, 3, 7, 12, 0))
    assert iso(t) == "2026-03-08 07:00"   # 07:00 UTC = 03:00 EDT
    # and the day after is back to normal 02:30 EDT = 06:30 UTC
    assert iso(next_fire("30 2 * * *", "America/New_York", t)) == "2026-03-09 06:30"


def test_fold_0130_runs_once_at_first_occurrence():
    # New York, 1 Nov 2026: 02:00 EDT -> 01:00 EST. 01:30 happens twice.
    fires = next_fires("30 1 * * *", "America/New_York", ts(2026, 10, 31, 12, 0), 2)
    assert iso(fires[0]) == "2026-11-01 05:30"   # first 01:30 (EDT, UTC-4)
    assert iso(fires[1]) == "2026-11-02 06:30"   # next day, 01:30 EST (UTC-5) - not 06:30 on 1 Nov


def test_hourly_job_does_not_repeat_the_folded_hour():
    fires = next_fires("0 * * * *", "America/New_York", ts(2026, 11, 1, 4, 30), 3)
    # 01:00 EDT (05:00 UTC) runs, the second 01:00 (EST, 06:00 UTC) is skipped, then 02:00 EST
    assert [iso(f) for f in fires] == ["2026-11-01 05:00", "2026-11-01 07:00", "2026-11-01 08:00"]
