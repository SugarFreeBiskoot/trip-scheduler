"""All database access (repository layer). SQLite file shared by every node.

  partitions : 64 buckets. A node owns a bucket through a timed lease.
  epoch      : +1 every time a bucket changes owner = the FENCING TOKEN.
  idem_key   : job_id + scheduled time. effects.idem_key is unique -> effect happens once.
  clock      : the database's clock decides due times and lease expiry (not each node's clock).
"""
import json
import math
import sqlite3
import threading
import uuid
import zlib
from contextlib import contextmanager
from typing import Optional

from .models import Job
from .timeutil import next_fire

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (job_id TEXT PRIMARY KEY, tenant_id TEXT, job_type TEXT, payload TEXT,
    run_at REAL, cron TEXT, tz TEXT, priority INTEGER, max_runtime REAL, misfire TEXT);
CREATE TABLE IF NOT EXISTS runs (run_id INTEGER PRIMARY KEY AUTOINCREMENT, job_id TEXT, tenant_id TEXT,
    partition INTEGER, scheduled_at REAL, due_at REAL, status TEXT, attempts INTEGER DEFAULT 0,
    idem_key TEXT UNIQUE, claimed_by TEXT, claimed_epoch INTEGER, first_started_at REAL, last_error TEXT);
CREATE INDEX IF NOT EXISTS runs_status_due ON runs(status, due_at);
CREATE TABLE IF NOT EXISTS partitions (pid INTEGER PRIMARY KEY, owner TEXT, epoch INTEGER DEFAULT 0,
    lease_until REAL DEFAULT 0);
CREATE TABLE IF NOT EXISTS nodes (node_id TEXT PRIMARY KEY, heartbeat REAL);
CREATE TABLE IF NOT EXISTS tenants (tenant_id TEXT PRIMARY KEY, paused INTEGER DEFAULT 0);
CREATE TABLE IF NOT EXISTS effects (idem_key TEXT PRIMARY KEY, run_id INTEGER, tenant_id TEXT,
    kind TEXT, target TEXT, node_id TEXT);
"""
NOW = "(julianday('now') - 2440587.5) * 86400.0"   # unix seconds from the DB clock


class Store:
    def __init__(self, path: str, partitions: int = 64):
        self.path, self.partitions = path, partitions
        self._local = threading.local()   # sqlite connections are per thread

    @property
    def conn(self) -> sqlite3.Connection:
        if not hasattr(self._local, "c"):
            c = sqlite3.connect(self.path, timeout=30, isolation_level=None)
            c.row_factory = sqlite3.Row
            c.execute("PRAGMA journal_mode=WAL")
            self._local.c = c
        return self._local.c

    @contextmanager
    def tx(self):
        """BEGIN IMMEDIATE takes the write lock first, so read-check-write is atomic."""
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            yield self.conn
            self.conn.execute("COMMIT")
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise

    def init_schema(self):
        self.conn.executescript(SCHEMA)
        with self.tx() as c:
            c.executemany("INSERT OR IGNORE INTO partitions(pid) VALUES (?)", [(p,) for p in range(self.partitions)])

    def now(self) -> float:
        return self.conn.execute(f"SELECT {NOW}").fetchone()[0]

    def partition_of(self, job_id: str) -> int:
        return zlib.crc32(job_id.encode()) % self.partitions   # stable across processes (hash() is not)

    def _add_run(self, c, job_id, tenant_id, at, key):
        c.execute("INSERT OR IGNORE INTO runs(job_id, tenant_id, partition, scheduled_at, due_at, status, idem_key)"
                  " VALUES (?,?,?,?,?,'PENDING',?)", (job_id, tenant_id, self.partition_of(job_id), at, at, key))

    # ---------------- jobs ----------------
    def create_jobs(self, jobs: list[Job]):
        with self.tx() as c:
            now = c.execute(f"SELECT {NOW}").fetchone()[0]
            for j in jobs:
                c.execute("INSERT INTO jobs VALUES (?,?,?,?,?,?,?,?,?,?)", (j.job_id, j.tenant_id, j.job_type,
                          json.dumps(j.payload), j.run_at, j.cron, j.tz, j.priority, j.max_runtime, j.misfire.value))
                c.execute("INSERT OR IGNORE INTO tenants(tenant_id) VALUES (?)", (j.tenant_id,))
                first = next_fire(j.cron, j.tz, now) if j.cron else j.run_at
                self._add_run(c, j.job_id, j.tenant_id, first, f"{j.job_id}:{first:.3f}")

    def get_job(self, job_id: str):
        return self.conn.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()

    # ---------------- leases + rebalancing ----------------
    def sync_partitions(self, node_id: str, lease: float) -> dict[int, int]:
        """Heartbeat, renew my leases, hand back extras, claim my fair share. Returns {pid: epoch}."""
        with self.tx() as c:
            now = c.execute(f"SELECT {NOW}").fetchone()[0]
            c.execute("INSERT OR REPLACE INTO nodes VALUES (?,?)", (node_id, now))
            live = c.execute("SELECT COUNT(*) FROM nodes WHERE heartbeat >= ?", (now - lease,)).fetchone()[0]
            share = math.ceil(self.partitions / max(live, 1))
            c.execute("UPDATE partitions SET lease_until=? WHERE owner=?", (now + lease, node_id))  # renew (only if still mine)
            mine = [r[0] for r in c.execute("SELECT pid FROM partitions WHERE owner=? ORDER BY pid", (node_id,))]
            if len(mine) > share:   # a node joined -> give back extras
                c.executemany("UPDATE partitions SET owner=NULL, lease_until=0 WHERE pid=?", [(p,) for p in mine[share:]])
            elif len(mine) < share:  # a node died (lease expired) or I just started -> take free buckets
                free = [r[0] for r in c.execute("SELECT pid FROM partitions WHERE owner IS NULL OR lease_until < ?"
                                                " ORDER BY pid LIMIT ?", (now, share - len(mine)))]
                c.executemany("UPDATE partitions SET owner=?, epoch=epoch+1, lease_until=? WHERE pid=?",
                              [(node_id, now + lease, p) for p in free])
            return {r[0]: r[1] for r in c.execute("SELECT pid, epoch FROM partitions WHERE owner=?", (node_id,))}

    # ---------------- claim due runs ----------------
    def claim_due(self, node_id: str, limit: int, tenant_cap: int) -> list[dict]:
        """Claim due runs in my buckets and stamp each with the bucket's epoch (its fencing token).

        Due = PENDING and due_at <= now, OR RUNNING but claimed under an older epoch (its node died).
        Fairness: rank runs per tenant and take at most (cap - already running) per tenant, round-robin.
        """
        if limit <= 0:
            return []
        with self.tx() as c:
            now = c.execute(f"SELECT {NOW}").fetchone()[0]
            ids = [r[0] for r in c.execute("""
                WITH due AS (
                  SELECT r.run_id, r.tenant_id, j.priority, r.due_at FROM runs r
                    JOIN partitions p ON p.pid = r.partition JOIN jobs j ON j.job_id = r.job_id
                   WHERE p.owner = :me AND ((r.status = 'PENDING' AND r.due_at <= :now)
                                            OR (r.status = 'RUNNING' AND r.claimed_epoch < p.epoch))),
                ranked AS (
                  SELECT d.*, ROW_NUMBER() OVER (PARTITION BY d.tenant_id ORDER BY d.priority DESC, d.due_at) rn,
                         (SELECT COUNT(*) FROM runs x WHERE x.status = 'RUNNING' AND x.claimed_by = :me
                             AND x.tenant_id = d.tenant_id) busy
                    FROM due d JOIN tenants t ON t.tenant_id = d.tenant_id WHERE t.paused = 0)
                SELECT run_id FROM ranked WHERE rn + busy <= :cap ORDER BY rn, priority DESC, due_at LIMIT :limit""",
                {"me": node_id, "now": now, "cap": tenant_cap, "limit": limit})]
            if not ids:
                return []
            marks = ",".join("?" * len(ids))
            c.execute(f"""UPDATE runs SET status='RUNNING', claimed_by=?, attempts=attempts+1,
                              first_started_at=COALESCE(first_started_at, ?),
                              claimed_epoch=(SELECT epoch FROM partitions WHERE pid=runs.partition)
                          WHERE run_id IN ({marks})""", [node_id, now, *ids])
            rows = c.execute(f"""SELECT r.*, j.job_type, j.payload, j.max_runtime, j.misfire, j.cron, j.tz
                                   FROM runs r JOIN jobs j ON j.job_id = r.job_id WHERE r.run_id IN ({marks})""", ids)
            return [dict(r) | {"payload": json.loads(r["payload"])} for r in rows]

    # ---------------- the fenced write ----------------
    def finish(self, run: dict, node_id: str, status: str, effect=None, next_at=None, retry_at=None, error=None) -> bool:
        """Save effect + new status + next occurrence in ONE transaction, only if my fencing token is current.

        status: DONE | SKIPPED | DEAD | PENDING (retry at retry_at). Returns False if rejected as stale.
        """
        with self.tx() as c:
            epoch = c.execute("SELECT epoch FROM partitions WHERE pid=? AND owner=?",
                              (run["partition"], node_id)).fetchone()
            row = c.execute("SELECT status, claimed_by, claimed_epoch FROM runs WHERE run_id=?", (run["run_id"],)).fetchone()
            if (epoch is None or epoch[0] != run["claimed_epoch"] or row["status"] != "RUNNING"
                    or row["claimed_by"] != node_id or row["claimed_epoch"] != run["claimed_epoch"]):
                return False   # stale: my lease expired and another node took this bucket
            if effect:
                c.execute("INSERT OR IGNORE INTO effects VALUES (?,?,?,?,?,?)",
                          (run["idem_key"], run["run_id"], run["tenant_id"], effect[0], effect[1], node_id))
            c.execute("UPDATE runs SET status=?, due_at=COALESCE(?, due_at), last_error=? WHERE run_id=?",
                      (status, retry_at, (error or "")[:200] or None, run["run_id"]))
            if next_at is not None:
                self._add_run(c, run["job_id"], run["tenant_id"], next_at, f"{run['job_id']}:{next_at:.3f}")
            return True

    # ---------------- operator controls ----------------
    def set_paused(self, tenant_id: str, paused: bool):
        with self.tx() as c:
            c.execute("INSERT OR REPLACE INTO tenants VALUES (?,?)", (tenant_id, int(paused)))

    def trigger_now(self, job_id: str, tenant_id: str):
        with self.tx() as c:
            self._add_run(c, job_id, tenant_id, c.execute(f"SELECT {NOW}").fetchone()[0], f"{job_id}:manual:{uuid.uuid4().hex}")

    def dead_letters(self, tenant_id: Optional[str]) -> list[dict]:
        rows = self.conn.execute("SELECT run_id, job_id, tenant_id, attempts, last_error FROM runs WHERE status='DEAD'"
                                 " AND (? IS NULL OR tenant_id=?) ORDER BY run_id", (tenant_id, tenant_id))
        return [dict(r) for r in rows]

    def replay(self, run_id: int, tenant_id: Optional[str]) -> bool:
        with self.tx() as c:
            return c.execute(f"UPDATE runs SET status='PENDING', attempts=0, due_at={NOW}, last_error=NULL"
                             " WHERE run_id=? AND status='DEAD' AND (? IS NULL OR tenant_id=?)",
                             (run_id, tenant_id, tenant_id)).rowcount == 1

    def metrics(self, tenant_id: Optional[str] = None) -> dict:
        """Fire lag histogram + running/retrying/dead counts, for the whole cluster."""
        f = "(? IS NULL OR tenant_id=?)"
        a = (tenant_id, tenant_id)
        q = lambda sql: self.conn.execute(sql, a).fetchone()[0]
        lags = sorted(r[0] for r in self.conn.execute(
            f"SELECT first_started_at - scheduled_at FROM runs WHERE first_started_at IS NOT NULL AND {f}", a))
        buckets = [0.1, 0.25, 0.5, 1, 2, 5, 10, 60]
        pct = lambda p: round(lags[min(len(lags) - 1, int(p * len(lags)))], 3) if lags else None
        return {
            "running": q(f"SELECT COUNT(*) FROM runs WHERE status='RUNNING' AND {f}"),
            "retrying": q(f"SELECT COUNT(*) FROM runs WHERE status='PENDING' AND attempts > 0 AND {f}"),
            "dead_lettered": q(f"SELECT COUNT(*) FROM runs WHERE status='DEAD' AND {f}"),
            "done": q(f"SELECT COUNT(*) FROM runs WHERE status='DONE' AND {f}"),
            "fire_lag_histogram": {f"le_{b}s": sum(1 for x in lags if x <= b) for b in buckets} | {"total": len(lags)},
            "fire_lag_p50_s": pct(0.50), "fire_lag_p99_s": pct(0.99),
        }
