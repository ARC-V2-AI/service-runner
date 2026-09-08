from __future__ import annotations

import os
from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass
from logging import Logger

from arc_service.context import BaseContext


@dataclass(frozen=True, slots=True)
class ServiceInfo:
    name: str
    version: str
    description: str = ""


class Service(ABC):
    def __init__(
        self,
        name: str,
        version: str,
        description: str = "",
    ) -> None:
        self.ctx: BaseContext
        self._info = ServiceInfo(
            name=name,
            version=version,
            description=description,
        )

    @property
    def info(self) -> ServiceInfo:
        return self._info

    def _load_context_env(self) -> None:
        for key, value in self.ctx.env.items():
            os.environ.setdefault(key, value)

    async def start(self, ctx: BaseContext) -> None:
        self.ctx = ctx
        self._load_context_env()
        await self.run()

    @abstractmethod
    async def run(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def stop(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def healthy(self) -> tuple[bool, str | None]:
        raise NotImplementedError

    @abstractmethod
    async def ready(self) -> tuple[bool, str | None]:
        raise NotImplementedError
