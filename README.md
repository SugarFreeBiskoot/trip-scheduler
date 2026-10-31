# Distributed Trip Job Scheduler (MoveInSync Case Study 3)

A scheduler for timed jobs (roster generation, "your cab is assigned" alerts, no-show cancels,
billing close) that runs on a cluster of nodes. It never loses a job and never lets a job's effect
happen twice, even when a node is killed (`kill -9`) or frozen (`SIGSTOP`).

**Stack:** Python 3.10+ (tested on 3.13), FastAPI, SQLite (one shared file), croniter. No other services.
The kill/freeze tests use POSIX signals, so run them on Linux or macOS (they are skipped on Windows).

## Run it

```bash
pip install -r requirements.txt
python run_cluster.py            # one command: 3 scheduler nodes + API on http://127.0.0.1:8000
python -m pytest -q              # all tests (unit + real-process kill/freeze tests)
python bench/bench.py --jobs 100000 --window 600   # the brief's benchmark
```

Interactive API docs: http://127.0.0.1:8000/docs. Demo tokens: `admin-token`, `acme-operator`,
`acme-employee` (read only), `globex-operator`. Override them with `SCHED_TOKENS='{"tok": ["operator", "acme"]}'`.

```bash
H='Authorization: Bearer acme-operator'
curl -X POST localhost:8000/jobs -H "$H" -H 'Content-Type: application/json' \
  -d '{"job_id":"roster-10pm","job_type":"send_alert","payload":{"employee_id":"E1"},"cron":"0 22 * * *","tz":"Asia/Kolkata"}'
curl "localhost:8000/jobs/roster-10pm/next?count=3" -H "$H"      # next fire times
curl -X POST localhost:8000/jobs/roster-10pm/trigger -H "$H"     # manual trigger
curl -X POST localhost:8000/tenants/acme/pause -H "$H"           # pause / resume a tenant
curl localhost:8000/dead-letters -H "$H"                          # inspect, then POST .../{run_id}/replay
curl localhost:8000/metrics -H "$H"                               # fire-lag histogram + counts
```

## How it works (one paragraph)

Jobs are split into 64 **partitions** by `crc32(job_id) % 64`. Each node takes a fair share of
partitions with a **timed lease** and renews it 3 times per lease period. Every change of owner bumps
the partition's **epoch**, which is the **fencing token**. When a node claims a run it stamps the run with
the current epoch, and when it saves the result it must present that epoch. The effect, the new status and
the next occurrence are written in **one transaction** that first checks the epoch, so a node that froze
past its lease cannot write anything. Each run has an **idempotency key** (`job_id:scheduled_time`) and
effects are unique per key. Execution is at least once, and the effect happens exactly once.

## Settings (env vars)

| Variable | Default | Meaning |
|---|---|---|
| `SCHED_DB` | `scheduler.db` | shared SQLite file |
| `SCHED_LEASE_SECONDS` | 10 | partition lease; also how fast a dead node's work moves |
| `SCHED_POLL_SECONDS` | 0.2 | how often a node looks for due runs |
| `SCHED_WORKERS` | 32 | threads per node |
| `SCHED_TENANT_CAP` | 16 | max concurrent runs per tenant per node |
| `SCHED_MAX_ATTEMPTS` | 5 | then dead-letter |
| `SCHED_MISFIRE_SECONDS` | 60 | later than this counts as a misfire |

## Assumptions

* All nodes share one SQLite file on one machine, so there is one clock. With Postgres it would be the server's `now()`.
* `send_alert` records the alert in the `effects` table (an outbox). A real SMS sender would read it and pass the idempotency key to the provider.
* Payloads carry employee IDs only, never phone numbers, emails or home locations.

Design note, failure-mode table, trade-offs and benchmark results are in `docs/report.pdf`.
