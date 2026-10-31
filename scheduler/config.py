"""All tunable settings in one place. Every value can be overridden by an env var."""
import os
from dataclasses import dataclass


def _env(name: str, default):
    raw = os.environ.get(name)
    return type(default)(raw) if raw is not None else default


@dataclass(frozen=True)
class Settings:
    db_path: str = _env("SCHED_DB", "scheduler.db")
    partitions: int = _env("SCHED_PARTITIONS", 64)          # fixed number of buckets
    lease_seconds: float = _env("SCHED_LEASE_SECONDS", 10.0)  # how long a node owns a bucket without renewing
    poll_seconds: float = _env("SCHED_POLL_SECONDS", 0.2)     # how often a node looks for due runs
    workers: int = _env("SCHED_WORKERS", 32)                  # threads per node that execute runs
    tenant_cap: int = _env("SCHED_TENANT_CAP", 16)            # max concurrent runs per tenant per node
    batch_size: int = _env("SCHED_BATCH", 200)                # max runs fetched per poll
    max_attempts: int = _env("SCHED_MAX_ATTEMPTS", 5)         # then dead-letter
    backoff_base: float = _env("SCHED_BACKOFF_BASE", 1.0)     # seconds
    backoff_max: float = _env("SCHED_BACKOFF_MAX", 60.0)
    misfire_seconds: float = _env("SCHED_MISFIRE_SECONDS", 60.0)  # later than this = misfire


def load() -> Settings:
    return Settings()
