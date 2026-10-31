"""Cron + time zones + daylight-saving rules. Pure functions, easy to test.

Rules (written down because the brief asks for them):
  * GAP  - a wall time that does not exist (02:30 when clocks jump 02:00 -> 03:00)
           runs at the moment the clock jumps (03:00 local).
  * FOLD - a wall time that happens twice (01:30 when clocks go 02:00 -> 01:00)
           runs ONCE, at the first occurrence.
"""
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from croniter import croniter

UTC = timezone.utc


def local_to_utc(naive: datetime, tz: ZoneInfo) -> datetime:
    """Turn a wall-clock time in `tz` into a UTC instant, applying the GAP/FOLD rules."""
    utc = naive.replace(tzinfo=tz, fold=0).astimezone(UTC)  # fold=0 -> first occurrence
    if utc.astimezone(tz).replace(tzinfo=None) == naive:
        return utc  # the wall time exists (if it repeats, fold=0 already picked the first one)
    # GAP: fold=0 maps a missing time to after the jump (02:30 -> 03:30).
    # Walk back minute by minute to the instant the clock jumped (03:00).
    while (utc - timedelta(minutes=1)).astimezone(tz).replace(tzinfo=None) >= naive:
        utc -= timedelta(minutes=1)
    return utc


def next_fire(cron: str, tz_name: str, after: float) -> float:
    """First fire time strictly after `after` (unix seconds). Cron is read in the tenant's zone."""
    tz = ZoneInfo(tz_name)
    local_after = datetime.fromtimestamp(after, UTC).astimezone(tz).replace(tzinfo=None)
    it = croniter(cron, local_after)
    while True:
        candidate = local_to_utc(it.get_next(datetime), tz).timestamp()
        if candidate > after:   # guards the FOLD case: never fire the repeated hour twice
            return candidate


def next_fires(cron: str, tz_name: str, after: float, count: int) -> list[float]:
    out, t = [], after
    for _ in range(count):
        t = next_fire(cron, tz_name, t)
        out.append(t)
    return out


def validate_cron(cron: str) -> bool:
    return croniter.is_valid(cron)
