"""Fire-lag benchmark on a real 3-process cluster.

  python bench/bench.py --jobs 100000 --window 600   # the brief: 100k jobs due within 10 minutes
  python bench/bench.py --jobs 10000 --window 60     # same rate (~167 jobs/s), 1 minute

Fire lag = time a run actually started - time it was scheduled. Target: p99 < 2 s.
"""
import argparse
import json
import os
import platform
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from scheduler.models import Job   # noqa: E402
from scheduler.store import Store  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--jobs", type=int, default=10000)
    ap.add_argument("--window", type=float, default=60)
    ap.add_argument("--nodes", type=int, default=3)
    ap.add_argument("--tenants", type=int, default=20)
    ap.add_argument("--work-ms", type=int, default=5)
    ap.add_argument("--db", default=str(ROOT / "bench" / "bench.db"))
    a = ap.parse_args()

    for suffix in ("", "-wal", "-shm"):
        Path(a.db + suffix).unlink(missing_ok=True)
    store = Store(a.db)
    store.init_schema()
    env = os.environ | {"SCHED_DB": a.db}
    procs = [subprocess.Popen([sys.executable, "-m", "scheduler.worker", "--node", f"n{i}"], cwd=ROOT, env=env,
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) for i in range(1, a.nodes + 1)]
    try:
        q = lambda sql: store.conn.execute(sql).fetchone()[0]
        while q("SELECT COUNT(DISTINCT owner) FROM partitions") < a.nodes:   # wait for buckets to spread
            time.sleep(0.2)

        start = store.now() + 10
        store.create_jobs([Job(f"job-{i}", f"tenant{i % a.tenants}", "send_alert",
                               {"employee_id": f"E{i}", "work_ms": a.work_ms},
                               run_at=start + a.window * i / a.jobs) for i in range(a.jobs)])
        print(f"inserted {a.jobs} jobs due over {a.window:.0f}s on {a.nodes} nodes", flush=True)

        deadline = time.time() + a.window + 180
        while time.time() < deadline:
            done = q("SELECT COUNT(*) FROM runs WHERE status='DONE'")
            if done >= a.jobs:
                break
            print(f"  done {done}/{a.jobs}", flush=True)
            time.sleep(10)

        m = store.metrics()
        result = {
            "jobs": a.jobs, "window_s": a.window, "nodes": a.nodes, "tenants": a.tenants, "work_ms": a.work_ms,
            "done": m["done"], "effects": q("SELECT COUNT(*) FROM effects"),
            "fire_lag_p50_s": m["fire_lag_p50_s"], "fire_lag_p99_s": m["fire_lag_p99_s"],
            "fire_lag_max_s": round(q("SELECT MAX(first_started_at - scheduled_at) FROM runs"), 3),
            "throughput_jobs_per_s": round(a.jobs / a.window, 1),
            "machine": f"{platform.platform()} | {os.cpu_count()} CPUs | Python {platform.python_version()}",
        }
        print(json.dumps(result, indent=2))
        out = ROOT / "bench" / f"results_{a.jobs}.json"
        out.write_text(json.dumps(result, indent=2))
    finally:
        for p in procs:
            p.terminate()
            p.wait()


if __name__ == "__main__":
    main()
