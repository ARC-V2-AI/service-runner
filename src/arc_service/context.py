from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from logging import Logger

from arc_service.communication import (
    CommunicationClient,
    CommunicationUnavailableError,
    EventBus,
    ServiceDirectory,
)


@dataclass(slots=True)
class BaseContext:
    logger: Logger
    env: Mapping[str, str] = field(default_factory=dict)
    service_name: str = ""
    process_name: str = ""
    communication: CommunicationClient | None = field(default=None, repr=False)

    @property
    def services(self) -> ServiceDirectory:
        """Request/response access to other services."""
        if self.communication is None:
            raise CommunicationUnavailableError(
                "This context has no active ARC communication client"
            )
        return self.communication.services

    @property
    def events(self) -> EventBus:
        """Publish typed events to subscribed ARC services."""
        if self.communication is None:
            raise CommunicationUnavailableError(
                "This context has no active ARC communication client"
            )
        return self.communication.events
