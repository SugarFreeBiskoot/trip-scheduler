"""Plain data classes. No database or web code here."""
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class RunStatus(str, Enum):
    PENDING = "PENDING"    # waiting for its due time (or a retry time)
    RUNNING = "RUNNING"    # claimed by a node
    DONE = "DONE"          # effect committed
    SKIPPED = "SKIPPED"    # missed and the misfire policy said skip
    DEAD = "DEAD"          # failed max_attempts times -> dead-letter list


class MisfirePolicy(str, Enum):
    FIRE_NOW = "FIRE_NOW"   # run every missed occurrence as soon as possible
    SKIP = "SKIP"           # drop missed occurrences, continue at the next future time
    COALESCE = "COALESCE"   # run once now for all missed occurrences, then continue


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
    max_runtime: float = 30.0         # seconds; slower counts as a failure
    misfire: MisfirePolicy = MisfirePolicy.FIRE_NOW

    @property
    def recurring(self) -> bool:
        return self.cron is not None


@dataclass
class Run:
    """One scheduled occurrence of a job. Retries reuse the same Run (same idempotency key)."""
    run_id: int
    job_id: str
    tenant_id: str
    partition: int
    scheduled_at: float     # the time it was meant to fire (part of the idempotency key)
    due_at: float           # when it may run next (scheduled_at, or later for retries)
    status: RunStatus
    attempts: int
    idem_key: str
