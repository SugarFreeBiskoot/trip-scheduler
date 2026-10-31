"""Control API (FastAPI). Run:  uvicorn scheduler.api:create_app --factory

Login: 'Authorization: Bearer <token>'. Each token maps to a role and a tenant.
  admin    - every tenant
  operator - own tenant: create jobs, pause/resume, trigger, replay dead letters
  employee - own tenant: read only
Another tenant's job answers 404 (not 403), so you can't even learn that it exists.
"""
import json
import os
import sqlite3
from datetime import datetime, timezone
from typing import Optional
from zoneinfo import ZoneInfo

from fastapi import Depends, FastAPI, Header, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .config import load
from .handlers import HANDLERS
from .models import Job, MisfirePolicy
from .store import Store
from .timeutil import next_fires, validate_cron

DEMO_TOKENS = {"admin-token": ["admin", None], "acme-operator": ["operator", "acme"],
               "acme-employee": ["employee", "acme"], "globex-operator": ["operator", "globex"]}


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str):
        self.status, self.code, self.message = status, code, message


class User(BaseModel):
    role: str
    tenant: Optional[str]


class JobIn(BaseModel):
    job_id: str = Field(min_length=1, max_length=100)
    tenant_id: Optional[str] = None          # only admins may set it; others use their own tenant
    job_type: str
    payload: dict = {}
    run_at: Optional[float] = None           # one-off: unix seconds
    cron: Optional[str] = None               # recurring
    tz: str = "UTC"
    priority: int = 0
    max_runtime: float = Field(30, gt=0)
    misfire: MisfirePolicy = MisfirePolicy.FIRE_NOW


def create_app(store: Optional[Store] = None) -> FastAPI:
    settings = load()
    store = store or Store(settings.db_path, settings.partitions)
    store.init_schema()
    tokens = json.loads(os.environ.get("SCHED_TOKENS", "null")) or DEMO_TOKENS
    app = FastAPI(title="Trip Job Scheduler")

    # ---------- errors: clear message, no internals, no personal data ----------
    @app.exception_handler(ApiError)
    async def api_error(_: Request, e: ApiError):
        return JSONResponse({"error": e.code, "message": e.message}, status_code=e.status)

    @app.exception_handler(RequestValidationError)
    async def bad_request(_: Request, e: RequestValidationError):   # field names only, never the input values
        fields = [".".join(str(p) for p in err["loc"][1:]) for err in e.errors()]
        return JSONResponse({"error": "invalid_request", "message": f"Invalid fields: {fields}"}, status_code=422)

    @app.exception_handler(Exception)
    async def unexpected(_: Request, e: Exception):
        return JSONResponse({"error": "internal", "message": "Something went wrong"}, status_code=500)

    # ---------- auth ----------
    def user(authorization: str = Header(default="")) -> User:
        role_tenant = tokens.get(authorization.removeprefix("Bearer ").strip())
        if not role_tenant:
            raise ApiError(401, "unauthorized", "Missing or invalid token")
        return User(role=role_tenant[0], tenant=role_tenant[1])

    def writer(u: User = Depends(user)) -> User:
        if u.role == "employee":
            raise ApiError(403, "forbidden", "This role is read-only")
        return u

    def scope(u: User, tenant: Optional[str]) -> Optional[str]:
        """Which tenant may this user touch? Admin: whatever they ask (None = all)."""
        if u.role == "admin":
            return tenant
        if tenant and tenant != u.tenant:
            raise ApiError(404, "not_found", "Not found")
        return u.tenant

    def own_job(u: User, job_id: str):
        job = store.get_job(job_id)
        if not job or (u.role != "admin" and job["tenant_id"] != u.tenant):
            raise ApiError(404, "not_found", "Job not found")
        return job

    # ---------- endpoints ----------
    @app.get("/health")
    def health():
        return {"status": "ok"}

    @app.post("/jobs", status_code=201)
    def create_job(body: JobIn, u: User = Depends(writer)):
        tenant = scope(u, body.tenant_id)
        if not tenant:
            raise ApiError(422, "invalid_request", "tenant_id is required for admins")
        if (body.run_at is None) == (body.cron is None):
            raise ApiError(422, "invalid_request", "Give exactly one of run_at (one-off) or cron (recurring)")
        if body.cron and not validate_cron(body.cron):
            raise ApiError(422, "invalid_request", "Invalid cron expression")
        if body.job_type not in HANDLERS:
            raise ApiError(422, "invalid_request", f"Unknown job_type; use one of {sorted(HANDLERS)}")
        if body.job_type == "send_alert" and "employee_id" not in body.payload:
            raise ApiError(422, "invalid_request", "send_alert needs payload.employee_id")
        try:
            ZoneInfo(body.tz)
        except Exception:
            raise ApiError(422, "invalid_request", "Unknown time zone")
        try:
            store.create_jobs([Job(**(body.model_dump() | {"tenant_id": tenant}))])
        except sqlite3.IntegrityError:
            raise ApiError(409, "conflict", "job_id already exists")
        return {"job_id": body.job_id, "tenant_id": tenant}

    @app.get("/jobs/{job_id}/next")
    def next_fire_times(job_id: str, count: int = 5, u: User = Depends(user)):
        job = own_job(u, job_id)
        if not job["cron"]:
            times = [job["run_at"]]
        else:
            times = next_fires(job["cron"], job["tz"], store.now(), min(count, 50))
        tz = ZoneInfo(job["tz"])
        return {"job_id": job_id, "tz": job["tz"], "next": [
            {"utc": datetime.fromtimestamp(t, timezone.utc).isoformat(),
             "local": datetime.fromtimestamp(t, tz).isoformat()} for t in times]}

    @app.post("/jobs/{job_id}/trigger", status_code=202)
    def trigger(job_id: str, u: User = Depends(writer)):
        job = own_job(u, job_id)
        store.trigger_now(job_id, job["tenant_id"])
        return {"job_id": job_id, "triggered": True}

    @app.post("/tenants/{tenant_id}/pause")
    def pause(tenant_id: str, u: User = Depends(writer)):
        store.set_paused(scope(u, tenant_id), True)
        return {"tenant_id": tenant_id, "paused": True}

    @app.post("/tenants/{tenant_id}/resume")
    def resume(tenant_id: str, u: User = Depends(writer)):
        store.set_paused(scope(u, tenant_id), False)
        return {"tenant_id": tenant_id, "paused": False}

    @app.get("/dead-letters")
    def dead_letters(u: User = Depends(user)):
        return store.dead_letters(scope(u, None))

    @app.post("/dead-letters/{run_id}/replay")
    def replay(run_id: int, u: User = Depends(writer)):
        if not store.replay(run_id, scope(u, None)):
            raise ApiError(404, "not_found", "Dead-lettered run not found")
        return {"run_id": run_id, "replayed": True}

    @app.get("/metrics")
    def metrics(u: User = Depends(user)):
        return store.metrics(scope(u, None))

    return app
