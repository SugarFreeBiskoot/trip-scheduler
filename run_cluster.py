"""One command to run everything:   python run_cluster.py

Starts 3 scheduler nodes + the API at http://127.0.0.1:8000 (interactive docs at /docs).
All processes share one SQLite file (SCHED_DB, default scheduler.db). Ctrl+C stops everything.
"""
import os
import subprocess
import sys
import time

from scheduler.config import load
from scheduler.store import Store

NODES = int(os.environ.get("NODES", 3))

if __name__ == "__main__":
    s = load()
    Store(s.db_path, s.partitions).init_schema()   # create tables once, before anyone starts
    procs = [subprocess.Popen([sys.executable, "-m", "scheduler.worker", "--node", f"n{i}"]) for i in range(1, NODES + 1)]
    procs.append(subprocess.Popen([sys.executable, "-m", "uvicorn", "scheduler.api:create_app", "--factory",
                                   "--port", os.environ.get("PORT", "8000"), "--log-level", "warning"]))
    print(f"{NODES} nodes + API running on http://127.0.0.1:8000  (docs: /docs, token: acme-operator). Ctrl+C to stop.")
    try:
        while all(p.poll() is None for p in procs):
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass
    finally:
        for p in procs:
            p.terminate()
        for p in procs:
            p.wait()
