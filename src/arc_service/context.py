from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from logging import Logger


@dataclass(slots=True)
class BaseContext:
    logger: Logger
    env: Mapping[str, str] = field(default_factory=dict)
    service_name: str = ""
    process_name: str = ""
