"""One scheduler node: keep bucket leases, claim due runs, execute them, save results (fenced)."""
import json
import logging
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from .config import Settings
from .handlers import HANDLERS
from .store import Store
from .timeutil import next_fire

log = logging.getLogger("scheduler")


def backoff(attempt: int, base: float, cap: float) -> float:
    """Exponential backoff with jitter: ~1s, 2s, 4s ... capped; each randomised to 50-100%
    so retries from many runs don't all hit the downstream at the same instant."""
    d = min(cap, base * 2 ** (attempt - 1))
    return d / 2 + random.uniform(0, d / 2)


class SchedulerNode:
    def __init__(self, node_id: str, store: Store, settings: Settings, handlers=HANDLERS):
        self.node_id, self.store, self.s, self.handlers = node_id, store, settings, handlers
        self.owned: dict[int, int] = {}
        self.pool = ThreadPoolExecutor(settings.workers)
        self.in_flight = 0
        self.lock = threading.Lock()
        self.stop_event = threading.Event()

    # ---- main loop ----
    def run_forever(self):
        next_sync = 0.0
        while not self.stop_event.is_set():
            try:
                if time.monotonic() >= next_sync:   # renew leases 3x per lease period
                    self.owned = self.store.sync_partitions(self.node_id, self.s.lease_seconds)
                    next_sync = time.monotonic() + self.s.lease_seconds / 3
                claimed = self.tick()
            except Exception as e:                   # e.g. DB busy: log, keep going
                log.error(json.dumps({"event": "loop_error", "node": self.node_id, "error": type(e).__name__}))
                claimed = 0
            if not claimed:
                self.stop_event.wait(self.s.poll_seconds)

    def tick(self, inline: bool = False) -> int:
        """Claim due runs (up to free worker slots) and run them. inline=True runs them here (for tests)."""
        with self.lock:
            free = self.s.workers - self.in_flight
        runs = self.store.claim_due(self.node_id, min(free, self.s.batch_size), self.s.tenant_cap)
        for run in runs:
            if inline:
                self.execute(run)
                continue
            with self.lock:
                self.in_flight += 1
            self.pool.submit(self._safe_execute, run)
        return len(runs)

    def _safe_execute(self, run):
        try:
            self.execute(run)
        except Exception as e:
            log.error(json.dumps({"event": "execute_error", "run_id": run["run_id"], "error": type(e).__name__}))
        finally:
            with self.lock:
                self.in_flight -= 1

    # ---- one run ----
    def execute(self, run: dict) -> str:
        now = self.store.now()
        cron, tz, policy = run["cron"], run["tz"], run["misfire"]
        next_after = lambda t: next_fire(cron, tz, t) if cron else None
        late = run["attempts"] == 1 and now - run["scheduled_at"] > self.s.misfire_seconds

        if late and policy == "SKIP":
            return self._save(run, "SKIPPED", next_at=next_after(now))
        # COALESCE: one run now stands in for all missed ones, so jump to the next future time.
        # FIRE_NOW: continue from the scheduled time, so every missed occurrence also runs.
        nxt = next_after(now if (late and policy == "COALESCE") else run["scheduled_at"])

        try:
            started = time.monotonic()
            effect = self.handlers[run["job_type"]].execute(run["payload"])
            if time.monotonic() - started > run["max_runtime"]:
                raise TimeoutError("exceeded max_runtime")
        except Exception as e:
            err = f"{type(e).__name__}: {e}"
            if run["attempts"] >= self.s.max_attempts:
                return self._save(run, "DEAD", next_at=nxt, error=err)
            retry_at = self.store.now() + backoff(run["attempts"], self.s.backoff_base, self.s.backoff_max)
            return self._save(run, "PENDING", retry_at=retry_at, error=err)
        return self._save(run, "DONE", effect=effect, next_at=nxt)

    def _save(self, run: dict, status: str, **kw) -> str:
        ok = self.store.finish(run, self.node_id, status, **kw)
        log.info(json.dumps({"event": "run_saved" if ok else "stale_write_rejected", "node": self.node_id,
                             "run_id": run["run_id"], "status": status, "epoch": run["claimed_epoch"]}))
        return status if ok else "REJECTED"
