from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Any


class ServiceState(StrEnum):
    NONE = "none"
    REGISTERED = "registered"
    STARTING = "starting"
    READY = "ready"
    RUNNING = "running"
    FAILED = "failed"
    STOPPING = "stopping"
    STOPPED = "stopped"


class ProcessOutcome(StrEnum):
    STOPPED = "stopped"
    CRASHED = "crashed"
    CANCELLED = "cancelled"


@dataclass(slots=True)
class ServiceStatus:
    ready: bool
    ready_reason: str | None
    healthy: bool
    healthy_reason: str | None

    def to_bytes(self) -> bytes:
        return json.dumps(
            asdict(self),
            separators=(",", ":"),
        ).encode("utf-8")

    @classmethod
    def from_bytes(cls, data: bytes) -> "ServiceStatus":
        values: dict[str, Any] = json.loads(data.decode("utf-8"))
        return cls(**values)


@dataclass(slots=True)
class ProcessResult:
    outcome: ProcessOutcome
    error: str | None = None
    traceback: str | None = None

    def to_bytes(self) -> bytes:
        data = asdict(self)
        data["outcome"] = self.outcome.value

        return json.dumps(
            data,
            separators=(",", ":"),
        ).encode("utf-8")

    @classmethod
    def from_bytes(cls, data: bytes) -> "ProcessResult":
        values: dict[str, Any] = json.loads(data.decode("utf-8"))

        values["outcome"] = ProcessOutcome(values["outcome"])

        return cls(**values)
