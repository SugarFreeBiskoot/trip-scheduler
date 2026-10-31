"""Core guarantees, tested in one process with no threads (tick(inline=True))."""
import pytest

from scheduler.config import Settings
from scheduler.models import Job, MisfirePolicy
from scheduler.node import SchedulerNode
from scheduler.store import Store


@pytest.fixture
def store(tmp_path):
    s = Store(str(tmp_path / "t.db"), partitions=8)
    s.init_schema()
    return s


def make_node(store, name, **kw):
    settings = Settings(**({"partitions": 8, "lease_seconds": 5, "backoff_base": 0.01, "backoff_max": 0.02} | kw))
    node = SchedulerNode(name, store, settings)
    node.owned = store.sync_partitions(name, settings.lease_seconds)
    return node


def alert(job_id, tenant="acme", at=None, **kw):
    return Job(job_id, tenant, "send_alert", {"employee_id": job_id}, run_at=at, **kw)


def q(store, sql, *args):
    return store.conn.execute(sql, args).fetchone()[0]


def expire(store, node_id):
    """Pretend node_id froze: its heartbeat and leases are in the past."""
    store.conn.execute("UPDATE nodes SET heartbeat = 0 WHERE node_id = ?", (node_id,))
    store.conn.execute("UPDATE partitions SET lease_until = 0 WHERE owner = ?", (node_id,))


def test_rebalance_on_join_and_leave(store):
    a = make_node(store, "a")
    assert len(a.owned) == 8                        # alone -> owns every bucket
    b = make_node(store, "b")
    a.owned = store.sync_partitions("a", 5)         # b joined -> a hands back half
    b.owned = store.sync_partitions("b", 5)
    assert len(a.owned) == 4 and len(b.owned) == 4
    expire(store, "a")                              # a dies
    b.owned = store.sync_partitions("b", 5)
    assert len(b.owned) == 8                        # b takes over everything


def test_frozen_node_cannot_commit_after_lease_expired(store):
    store.create_jobs([alert("j1", at=store.now() - 1)])
    a = make_node(store, "a")
    run = store.claim_due("a", 10, 10)[0]           # a claims the run ... then freezes
    expire(store, "a")
    b = make_node(store, "b")                        # b takes the bucket: epoch goes up
    assert b.tick(inline=True) == 1                  # b re-runs the abandoned run and commits
    assert a.execute(run) == "REJECTED"              # a wakes up: stale fencing token -> rejected
    assert q(store, "SELECT COUNT(*) FROM effects") == 1
    assert q(store, "SELECT node_id FROM effects") == "b"


def test_retries_with_backoff_then_dead_letter_then_replay(store):
    store.create_jobs([Job("bad", "acme", "fail", run_at=store.now() - 1)])
    node = make_node(store, "a", max_attempts=3)
    import time
    for _ in range(100):
        node.tick(inline=True)
        if q(store, "SELECT status FROM runs") == "DEAD":
            break
        time.sleep(0.02)
    assert q(store, "SELECT attempts FROM runs") == 3
    dead = store.dead_letters("acme")
    assert len(dead) == 1 and "downstream unavailable" in dead[0]["last_error"]
    assert store.replay(dead[0]["run_id"], "acme")
    assert q(store, "SELECT status FROM runs") == "PENDING"


def test_same_feed_twice_creates_one_run(store):
    j = alert("j1", at=store.now() + 60)
    store.create_jobs([j])
    store._add_run(store.conn, "j1", "acme", j.run_at, f"j1:{j.run_at:.3f}")   # same idempotency key again
    assert q(store, "SELECT COUNT(*) FROM runs") == 1


def test_big_tenant_does_not_starve_small_tenant(store):
    now = store.now() - 1
    store.create_jobs([alert(f"big{i}", "big", now) for i in range(500)] +
                      [alert(f"small{i}", "small", now) for i in range(5)])
    make_node(store, "a")
    claimed = store.claim_due("a", limit=20, tenant_cap=10)
    tenants = [r["tenant_id"] for r in claimed]
    assert tenants.count("small") == 5               # all 20 small-tenant slots go first
    assert tenants.count("big") == 10                # big tenant is capped at 10


def past_recurring(store, policy):
    store.create_jobs([Job("cron1", "acme", "send_alert", {"employee_id": "e1"}, cron="* * * * *", misfire=policy)])
    old = store.now() - 600                          # pretend the cluster was down for 10 minutes
    store.conn.execute("UPDATE runs SET scheduled_at=?, due_at=?, idem_key=?", (old, old, f"cron1:{old:.3f}"))
    return old


@pytest.mark.parametrize("policy", list(MisfirePolicy))
def test_misfire_policies(store, policy):
    old = past_recurring(store, policy)
    node = make_node(store, "a")
    node.tick(inline=True)
    nxt = store.conn.execute("SELECT scheduled_at FROM runs WHERE status='PENDING'").fetchone()[0]
    effects = q(store, "SELECT COUNT(*) FROM effects")
    if policy is MisfirePolicy.SKIP:
        assert effects == 0 and nxt > store.now()                  # skipped, resumes in the future
    elif policy is MisfirePolicy.COALESCE:
        assert effects == 1 and nxt > store.now()                  # ran once, jumped to the future
    else:
        assert effects == 1 and abs(nxt - (old + 60)) < 61         # FIRE_NOW: next missed one is due too


def test_pause_and_resume_tenant(store):
    store.create_jobs([alert("j1", at=store.now() - 1)])
    node = make_node(store, "a")
    store.set_paused("acme", True)
    assert node.tick(inline=True) == 0
    store.set_paused("acme", False)
    assert node.tick(inline=True) == 1
