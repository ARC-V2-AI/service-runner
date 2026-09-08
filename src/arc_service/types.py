from __future__ import annotations

import json
from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class ServiceState(StrEnum):
    NONE = "none"
    REGISTERED = "registered"
    CREATED = "created"
    STARTING = "starting"
    READY = "ready"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FAILED = "failed"


class ProcessOutcome(StrEnum):
    STOPPED = "stopped"
    CRASHED = "crashed"
    CANCELLED = "cancelled"


@dataclass(frozen=True, slots=True)
class ServiceStatus:
    ready: bool
    ready_reason: str | None
    healthy: bool
    healthy_reason: str | None

    def to_bytes(self) -> bytes:
        return json.dumps(
            {
                "ready": self.ready,
                "ready_reason": self.ready_reason,
                "healthy": self.healthy,
                "healthy_reason": self.healthy_reason,
            },
            separators=(",", ":"),
        ).encode("utf-8")

    @classmethod
    def from_bytes(cls, data: bytes) -> ServiceStatus:
        value: Any = json.loads(data)

        if not isinstance(value, dict):
            raise ValueError("Invalid ServiceStatus payload")

        return cls(
            ready=bool(value["ready"]),
            ready_reason=value.get("ready_reason"),
            healthy=bool(value["healthy"]),
            healthy_reason=value.get("healthy_reason"),
        )


@dataclass(frozen=True, slots=True)
class ProcessResult:
    outcome: ProcessOutcome
    error: str | None = None
    traceback: str | None = None

    def to_bytes(self) -> bytes:
        return json.dumps(
            {
                "outcome": self.outcome.value,
                "error": self.error,
                "traceback": self.traceback,
            },
            separators=(",", ":"),
        ).encode("utf-8")

    @classmethod
    def from_bytes(cls, data: bytes) -> ProcessResult:
        value: Any = json.loads(data)

        if not isinstance(value, dict):
            raise ValueError("Invalid ProcessResult payload")

        return cls(
            outcome=ProcessOutcome(value["outcome"]),
            error=value.get("error"),
            traceback=value.get("traceback"),
        )
