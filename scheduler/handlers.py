"""Job types. A new type = a new subclass of JobHandler (polymorphism)."""
import time
from abc import ABC, abstractmethod
from typing import Optional


class JobHandler(ABC):
    @abstractmethod
    def execute(self, payload: dict) -> Optional[tuple[str, str]]:
        """Do the work. Return the effect to record as (kind, target), or None."""


class SendAlert(JobHandler):
    """'Your cab is assigned' alert. The effect row is the outbox a real SMS sender would read,
    so it is written in the same transaction as the run's status (see Store.finish)."""

    def execute(self, payload):
        time.sleep(payload.get("work_ms", 0) / 1000)   # simulated work
        return ("alert", str(payload["employee_id"]))   # employee ID only, never phone/email


class AlwaysFail(JobHandler):
    """Fails every time. Used to show retries and the dead-letter list."""

    def execute(self, payload):
        raise RuntimeError("downstream unavailable")


HANDLERS: dict[str, JobHandler] = {"send_alert": SendAlert(), "fail": AlwaysFail()}
