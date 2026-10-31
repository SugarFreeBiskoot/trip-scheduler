"""Plain data. No database or web code here."""
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class MisfirePolicy(str, Enum):
    FIRE_NOW = "FIRE_NOW"   # run every missed occurrence as soon as possible
    SKIP = "SKIP"           # drop missed occurrences, continue at the next future time
    COALESCE = "COALESCE"   # run ONCE now for all missed occurrences, then continue


@dataclass
class Job:
    job_id: str
    tenant_id: str
    job_type: str                     # which handler runs it, e.g. "send_alert"
    payload: dict = field(default_factory=dict)
    run_at: Optional[float] = None    # one-off: unix seconds (UTC)
    cron: Optional[str] = None        # recurring: cron expression in the tenant's time zone
    tz: str = "UTC"
    priority: int = 0                 # higher runs first
    max_runtime: float = 30.0         # seconds; longer counts as a failure
    misfire: MisfirePolicy = MisfirePolicy.FIRE_NOW
