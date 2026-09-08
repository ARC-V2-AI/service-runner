from __future__ import annotations

import asyncio
import importlib
import inspect
import logging
import os
import traceback
from multiprocessing.connection import Connection
from typing import TypeAlias

from .context import BaseContext
from .service import Service
from .types import ProcessOutcome, ProcessResult, ServiceStatus

_ControlConn: TypeAlias = Connection
_ResultConn: TypeAlias = Connection


def resolve_service_class(
    module_path: str,
    class_name: str | None = None,
) -> type[Service]:
    module = importlib.import_module(module_path)

    if class_name is not None:
        target = getattr(module, class_name, None)

        if (
            not inspect.isclass(target)
            or not issubclass(target, Service)
            or target is Service
        ):
            raise TypeError(f"'{module_path}:{class_name}' is not a Service subclass")

        return target

    for target in vars(module).values():
        if (
            inspect.isclass(target)
            and issubclass(target, Service)
            and target is not Service
        ):
            return target

    raise TypeError(f"No Service subclass found in '{module_path}'")


def make_context(service_name: str) -> BaseContext:
    return BaseContext(
        logger=logging.getLogger(service_name),
        env=dict(os.environ),
        service_name=service_name,
        process_name=__import__("multiprocessing").current_process().name,
    )


async def run_service(
    service_class: type[Service],
    service_name: str,
    control_conn: _ControlConn,
    result_conn: _ResultConn,
) -> None:
    ctx = make_context(service_name)
    service = service_class()

    run_task = asyncio.create_task(service.start(ctx))

    try:
        while True:
            if control_conn.poll():
                command = control_conn.recv()

                if command == "status":
                    try:
                        ready, ready_reason = await service.ready()
                    except BaseException as exc:
                        control_conn.send(
                            ServiceStatus(
                                ready=False,
                                ready_reason=f"ready() failed: {exc}",
                                healthy=False,
                                healthy_reason=None,
                            )
                        )
                        continue

                    try:
                        healthy, healthy_reason = await service.healthy()
                    except BaseException as exc:
                        control_conn.send(
                            ServiceStatus(
                                ready=ready,
                                ready_reason=ready_reason,
                                healthy=False,
                                healthy_reason=f"healthy() failed: {exc}",
                            )
                        )
                        continue

                    control_conn.send(
                        ServiceStatus(
                            ready=ready,
                            ready_reason=ready_reason,
                            healthy=healthy,
                            healthy_reason=healthy_reason,
                        )
                    )

                elif command == "stop":
                    await service.stop()

                    run_task.cancel()

                    try:
                        await run_task
                    except asyncio.CancelledError:
                        pass

                    break

            if run_task.done():
                await run_task
                break

            await asyncio.sleep(0.1)

    except asyncio.CancelledError:
        result_conn.send(
            ProcessResult(
                outcome=ProcessOutcome.CANCELLED,
            )
        )
        raise

    except BaseException as exc:
        result_conn.send(
            ProcessResult(
                outcome=ProcessOutcome.CRASHED,
                error=str(exc),
                traceback=traceback.format_exc(),
            )
        )
        raise

    else:
        result_conn.send(
            ProcessResult(
                outcome=ProcessOutcome.STOPPED,
            )
        )

    finally:
        control_conn.close()
        result_conn.close()
