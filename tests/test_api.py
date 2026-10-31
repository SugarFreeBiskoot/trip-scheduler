"""API: login, roles, tenant isolation, validation, operator controls."""
import pytest
from fastapi.testclient import TestClient

from scheduler.api import create_app
from scheduler.store import Store

ACME = {"Authorization": "Bearer acme-operator"}
ACME_EMPLOYEE = {"Authorization": "Bearer acme-employee"}
GLOBEX = {"Authorization": "Bearer globex-operator"}


@pytest.fixture
def client(tmp_path):
    return TestClient(create_app(Store(str(tmp_path / "api.db"))))


def roster_job(job_id="roster"):
    return {"job_id": job_id, "job_type": "send_alert", "payload": {"employee_id": "E1"},
            "cron": "0 22 * * *", "tz": "Asia/Kolkata"}


def test_needs_a_valid_token(client):
    assert client.get("/metrics").status_code == 401
    assert client.get("/metrics", headers={"Authorization": "Bearer nope"}).status_code == 401


def test_create_job_and_list_next_fire_times(client):
    assert client.post("/jobs", json=roster_job(), headers=ACME).status_code == 201
    r = client.get("/jobs/roster/next?count=3", headers=ACME).json()
    assert len(r["next"]) == 3 and all(t["local"].endswith("22:00:00+05:30") for t in r["next"])


def test_tenant_isolation_other_tenant_gets_404(client):
    client.post("/jobs", json=roster_job(), headers=ACME)
    assert client.get("/jobs/roster/next", headers=GLOBEX).status_code == 404
    assert client.post("/jobs/roster/trigger", headers=GLOBEX).status_code == 404
    assert client.post("/tenants/acme/pause", headers=GLOBEX).status_code == 404


def test_employee_is_read_only(client):
    assert client.post("/jobs", json=roster_job(), headers=ACME_EMPLOYEE).status_code == 403


def test_validation_errors_do_not_echo_input(client):
    bad = roster_job() | {"cron": "not a cron"}
    r = client.post("/jobs", json=bad, headers=ACME)
    assert r.status_code == 422 and "not a cron" not in r.text
    r = client.post("/jobs", json={"job_id": "x", "job_type": "send_alert", "payload": {"phone": "+919999999999"},
                                   "max_runtime": -1}, headers=ACME)
    assert r.status_code == 422 and "9999" not in r.text
    no_employee = roster_job() | {"payload": {}}
    assert client.post("/jobs", json=no_employee, headers=ACME).status_code == 422


def test_duplicate_job_is_409(client):
    client.post("/jobs", json=roster_job(), headers=ACME)
    assert client.post("/jobs", json=roster_job(), headers=ACME).status_code == 409


def test_trigger_pause_resume_dead_letters_metrics(client):
    client.post("/jobs", json=roster_job(), headers=ACME)
    assert client.post("/jobs/roster/trigger", headers=ACME).status_code == 202
    assert client.post("/tenants/acme/pause", headers=ACME).json()["paused"] is True
    assert client.post("/tenants/acme/resume", headers=ACME).json()["paused"] is False
    assert client.get("/dead-letters", headers=ACME).json() == []
    m = client.get("/metrics", headers=ACME).json()
    assert {"running", "retrying", "dead_lettered", "fire_lag_histogram"} <= m.keys()
