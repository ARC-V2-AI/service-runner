from __future__ import annotations

import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path

RESET = "\033[0m"

COLORS = {
    logging.DEBUG: "\033[36m",
    logging.INFO: "\033[32m",
    logging.WARNING: "\033[33m",
    logging.ERROR: "\033[31m",
    logging.CRITICAL: "\033[1;31m",
}


def env_bool(key: str, default: str) -> bool:
    value = os.getenv(key, default)
    return value.strip().lower() in {"1", "true", "yes", "on"}


class BootColorFormatter(logging.Formatter):
    def __init__(
        self,
        fmt: str,
        datefmt: str | None = None,
        use_color: bool = True,
    ) -> None:
        super().__init__(fmt, datefmt)
        self.use_color = use_color

    def format(self, record: logging.LogRecord) -> str:
        message = super().format(record)

        if env_bool("TERMINAL_NO_COLOR", "0") or not self.use_color:
            return message

        color = COLORS.get(record.levelno)

        if not color:
            return message

        return f"{color}{message}{RESET}"


def configure_logging() -> None:
    level = os.getenv("LOG_LEVEL", "INFO")
    log_file = os.path.expanduser(os.getenv("LOG_FILE", "~/arc/workspace/arc.log"))

    console = env_bool("LOG_CONSOLE", "1")
    json_logging = env_bool("LOG_JSON", "0")
    rotate = env_bool("LOG_ROTATE", "1")
    max_bytes = int(os.getenv("LOG_MAX_BYTES", "10485760"))
    backup_count = int(os.getenv("LOG_BACKUP_COUNT", "2"))

    logger = logging.getLogger()
    logger.setLevel(level)

    if logger.handlers:
        return

    fmt = "[%(asctime)s] %(levelname)-8s %(name)-25s %(message)s"

    Path(log_file).parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    if rotate:
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
        )
    else:
        file_handler = logging.FileHandler(
            log_file,
            encoding="utf-8",
        )

    if json_logging:
        # Add your JSON formatter here when you implement it.
        file_handler.setFormatter(
            logging.Formatter(
                fmt,
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
    else:
        file_handler.setFormatter(
            logging.Formatter(
                fmt,
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )

    logger.addHandler(file_handler)

    if console:
        console_handler = logging.StreamHandler()

        console_handler.setFormatter(
            BootColorFormatter(
                fmt,
                datefmt="%H:%M:%S",
                use_color=True,
            )
        )

        logger.addHandler(console_handler)
