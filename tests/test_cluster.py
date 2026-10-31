"""The two tests the brief asks for, with REAL processes and REAL signals.

  1. kill -9 a node while 1,200 pickup alerts are being sent -> every employee gets exactly one.
  2. SIGSTOP (freeze) a node past its lease -> another node finishes the job, and when the
     frozen node wakes up its stale write is rejected by the fencing token.
"""
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from scheduler.models import Job
from scheduler.store import Store

ROOT = Path(__file__).resolve().parent.parent
pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="needs POSIX signals")


def start(node, db, logdir, **env):
    e = os.environ | {"SCHED_DB": db, "SCHED_LEASE_SECONDS": "2", "SCHED_POLL_SECONDS": "0.1",
                      "SCHED_WORKERS": "8"} | {k: str(v) for k, v in env.items()}
    log = open(Path(logdir) / f"{node}.log", "w")
    return subprocess.Popen([sys.executable, "-m", "scheduler.worker", "--node", node],
                            cwd=ROOT, env=e, stdout=log, stderr=subprocess.STDOUT)


def wait_for(cond, timeout, step=0.1):
    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return True
        time.sleep(step)
    return False


def q(store, sql, *args):
    return store.conn.execute(sql, args).fetchone()[0]


@pytest.fixture
def cluster(tmp_path):
    db = str(tmp_path / "c.db")
    store = Store(db)
    store.init_schema()
    procs = []
    yield store, db, tmp_path, procs
    for p in procs:
        if p.poll() is None:
            p.send_signal(signal.SIGCONT)
            p.kill()
            p.wait()


def test_kill_9_mid_batch_every_employee_gets_exactly_one_alert(cluster):
    store, db, logs, procs = cluster
    procs += [start(n, db, logs) for n in ("n1", "n2", "n3")]
    assert wait_for(lambda: q(store, "SELECT COUNT(DISTINCT owner) FROM partitions") == 3, 15)

    now = store.now()
    store.create_jobs([Job(f"pickup-{i}", f"tenant{i % 4}", "send_alert",
                           {"employee_id": f"E{i}", "work_ms": 200}, run_at=now + 0.5) for i in range(1200)])
    assert wait_for(lambda: q(store, "SELECT COUNT(*) FROM effects") >= 200, 30)   # mid-batch...
    procs[0].send_signal(signal.SIGKILL)                                             # ... kill -9 n1

    assert wait_for(lambda: q(store, "SELECT COUNT(*) FROM runs WHERE status='DONE'") == 1200, 90)
    assert q(store, "SELECT COUNT(*) FROM effects") == 1200                  # exactly one alert each
    assert q(store, "SELECT COUNT(DISTINCT target) FROM effects") == 1200
    assert q(store, "SELECT COUNT(DISTINCT owner) FROM partitions") == 2     # n1's buckets moved
    assert q(store, "SELECT COUNT(*) FROM runs WHERE attempts >= 2") > 0     # some runs executed twice...
    # ...but their effect still happened once: at-least-once execution + idempotency = exactly-once effect.


def test_frozen_node_stale_write_is_rejected(cluster):
    store, db, logs, procs = cluster
    procs.append(start("n1", db, logs))
    assert wait_for(lambda: q(store, "SELECT COUNT(*) FROM partitions WHERE owner='n1'") == 64, 15)

    store.create_jobs([Job("close-billing", "acme", "send_alert",
                           {"employee_id": "E1", "work_ms": 3000}, run_at=store.now())])
    assert wait_for(lambda: q(store, "SELECT status FROM runs") == "RUNNING", 10)
    procs[0].send_signal(signal.SIGSTOP)                       # freeze n1 mid-job (like a long GC pause)

    procs.append(start("n2", db, logs))                        # n2 takes the expired buckets and re-runs it
    assert wait_for(lambda: q(store, "SELECT COUNT(*) FROM effects") == 1, 20)
    assert q(store, "SELECT node_id FROM effects") == "n2"

    procs[0].send_signal(signal.SIGCONT)                       # n1 wakes up and tries to save its result
    n1_log = logs / "n1.log"
    assert wait_for(lambda: "stale_write_rejected" in n1_log.read_text(), 15)
    assert q(store, "SELECT COUNT(*) FROM effects") == 1       # still exactly one effect, written by n2
