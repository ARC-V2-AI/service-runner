from __future__ import annotations

import argparse
import asyncio
import importlib
import inspect
import logging
import multiprocessing
import os
import traceback
from multiprocessing.connection import Connection

from .context import BaseContext
from .service import Service
from .types import ProcessOutcome, ProcessResult, ServiceStatus

logger = logging.getLogger(__name__)


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


def make_context(
    service_name: str,
) -> BaseContext:
    return BaseContext(
        logger=logging.getLogger(service_name),
        env=dict(os.environ),
        service_name=service_name,
        process_name=(multiprocessing.current_process().name),
    )


def send_result(
    result_conn: Connection,
    result: ProcessResult,
) -> None:
    try:
        result_conn.send_bytes(result.to_bytes())
    except (
        BrokenPipeError,
        ConnectionResetError,
        OSError,
    ):
        pass


async def run_service(
    module_path: str,
    service_name: str,
    control_conn: Connection,
    result_conn: Connection,
) -> None:
    run_task: asyncio.Task[None] | None = None

    try:
        service_class = resolve_service_class(module_path)

        service = service_class()

        ctx = make_context(service_name)

        run_task = asyncio.create_task(service.start(ctx))

        while True:
            if control_conn.poll():
                command = control_conn.recv_bytes()

                if command == b"status":
                    try:
                        ready, ready_reason = await service.ready()
                    except BaseException as exc:
                        control_conn.send_bytes(
                            ServiceStatus(
                                ready=False,
                                ready_reason=(f"ready() failed: {exc}"),
                                healthy=False,
                                healthy_reason=None,
                            ).to_bytes()
                        )
                        continue

                    try:
                        healthy, healthy_reason = await service.healthy()
                    except BaseException as exc:
                        control_conn.send_bytes(
                            ServiceStatus(
                                ready=ready,
                                ready_reason=ready_reason,
                                healthy=False,
                                healthy_reason=(f"healthy() failed: {exc}"),
                            ).to_bytes()
                        )
                        continue

                    control_conn.send_bytes(
                        ServiceStatus(
                            ready=ready,
                            ready_reason=ready_reason,
                            healthy=healthy,
                            healthy_reason=healthy_reason,
                        ).to_bytes()
                    )

                elif command == b"stop":
                    await service.stop()

                    if run_task is not None:
                        run_task.cancel()

                        try:
                            await run_task
                        except asyncio.CancelledError:
                            pass

                    break

            if run_task is not None and run_task.done():
                await run_task
                break

            await asyncio.sleep(0.1)

        send_result(
            result_conn,
            ProcessResult(
                outcome=ProcessOutcome.STOPPED,
            ),
        )

    except asyncio.CancelledError:
        send_result(
            result_conn,
            ProcessResult(
                outcome=ProcessOutcome.CANCELLED,
            ),
        )
        raise

    except BaseException as exc:
        send_result(
            result_conn,
            ProcessResult(
                outcome=ProcessOutcome.CRASHED,
                error=str(exc),
                traceback=traceback.format_exc(),
            ),
        )

        logger.exception(
            "Service '%s' crashed",
            service_name,
        )

        # Do not re-raise here. The result has been reported
        # and the process should exit normally after reporting it.

    finally:
        control_conn.close()
        result_conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--module",
        required=True,
    )

    parser.add_argument(
        "--service-name",
        required=True,
    )

    parser.add_argument(
        "--control-fd",
        required=True,
        type=int,
    )

    parser.add_argument(
        "--result-fd",
        required=True,
        type=int,
    )

    return parser.parse_args()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format=("[%(asctime)s] %(levelname)s %(name)s: %(message)s"),
    )

    args = parse_args()

    control_conn = Connection(
        args.control_fd,
        readable=True,
        writable=True,
    )

    result_conn = Connection(
        args.result_fd,
        readable=False,
        writable=True,
    )

    asyncio.run(
        run_service(
            module_path=args.module,
            service_name=args.service_name,
            control_conn=control_conn,
            result_conn=result_conn,
        )
    )


if __name__ == "__main__":
    main()
