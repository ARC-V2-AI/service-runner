"""Typed client API and wire protocol for ARC service communication.

The Core-owned broker is responsible for routing and durable event storage. This
module provides the service-side API and shared, versioned JSON-line protocol.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import os
import re
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Generic, Literal, TypeVar

from pydantic import BaseModel, ConfigDict, Field

PROTOCOL_VERSION = 1
MAX_FRAME_BYTES = 8 * 1024 * 1024
DEFAULT_CALL_TIMEOUT = 10.0
EVENT_LEASE_SECONDS = 60.0
_SERVICE_ID_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,127}$")
_EVENT_TYPE_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$")

PayloadT = TypeVar("PayloadT", bound=BaseModel)


class CommunicationError(RuntimeError):
    """Base class for communication failures."""


class CommunicationUnavailableError(CommunicationError):
    """The ARC communication broker is not connected or unavailable."""


class ServiceUnavailableError(CommunicationError):
    """The requested service is not currently connected to the broker."""


class RemoteCallError(CommunicationError):
    """The target service rejected or failed a remote call."""


class EventQueueFullError(CommunicationError):
    """The durable event queue is full for at least one subscriber."""


class RpcTimeoutError(TimeoutError, CommunicationError):
    """A remote call did not return before its timeout."""


class ProtocolError(CommunicationError):
    """An invalid or incompatible wire-protocol message was received."""


class WireModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Hello(WireModel):
    type: Literal["hello"] = "hello"
    protocol_version: int = PROTOCOL_VERSION
    service_id: str = Field(pattern=_SERVICE_ID_RE.pattern)
    rpc_methods: list[str] = Field(default_factory=list)
    subscriptions: list[str] = Field(default_factory=list)


class HelloAck(WireModel):
    type: Literal["hello_ack"] = "hello_ack"
    ok: bool
    protocol_version: int = PROTOCOL_VERSION
    error: str | None = None


class Activate(WireModel):
    type: Literal["activate"] = "activate"


class RpcCall(WireModel):
    type: Literal["call"] = "call"
    id: str
    target: str = Field(pattern=_SERVICE_ID_RE.pattern)
    method: str = Field(pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    payload: Any


class RpcRequest(WireModel):
    type: Literal["rpc_request"] = "rpc_request"
    id: str
    caller: str
    method: str
    payload: Any


class RpcResult(WireModel):
    type: Literal["rpc_result"] = "rpc_result"
    id: str
    ok: bool
    payload: Any = None
    error: str | None = None
    error_type: str | None = None


class Publish(WireModel):
    type: Literal["publish"] = "publish"
    id: str
    event_type: str = Field(pattern=_EVENT_TYPE_RE.pattern)
    payload: Any


class Published(WireModel):
    type: Literal["published"] = "published"
    id: str
    event_id: str = ""
    queued_for: int = Field(default=0, ge=0)
    ok: bool = True
    error: str | None = None


class EventDelivery(WireModel):
    type: Literal["event"] = "event"
    delivery_id: str
    event_id: str
    event_type: str = Field(pattern=_EVENT_TYPE_RE.pattern)
    source: str
    occurred_at: datetime
    payload: Any
    attempt: int = Field(ge=1)


class EventAck(WireModel):
    type: Literal["ack"] = "ack"
    delivery_id: str


class EventNack(WireModel):
    type: Literal["nack"] = "nack"
    delivery_id: str
    error: str = "handler failed"


class EventExtend(WireModel):
    type: Literal["extend"] = "extend"
    delivery_id: str


class ListServices(WireModel):
    type: Literal["list_services"] = "list_services"
    id: str


class ServicesList(WireModel):
    type: Literal["services"] = "services"
    id: str
    services: list[dict[str, Any]]


class ProtocolErrorMessage(WireModel):
    type: Literal["protocol_error"] = "protocol_error"
    error: str


class EventEnvelope(BaseModel, Generic[PayloadT]):
    """Typed metadata and payload passed to an event handler."""

    model_config = ConfigDict(extra="forbid")
    id: str
    type: str
    source: str
    occurred_at: datetime
    payload: PayloadT
    attempt: int = Field(ge=1)


class PublishReceipt(BaseModel):
    event_id: str
    queued_for: int = Field(ge=0)


class ServiceSummary(BaseModel):
    service_id: str
    active: bool
    methods: list[str] = Field(default_factory=list)
    subscriptions: list[str] = Field(default_factory=list)


@dataclass(frozen=True, slots=True)
class RpcSpec:
    request_model: type[BaseModel]
    response_model: type[BaseModel]
    name: str | None = None


@dataclass(frozen=True, slots=True)
class EventSpec:
    event_type: str
    payload_model: type[BaseModel]


@dataclass(frozen=True, slots=True)
class RpcBinding:
    name: str
    request_model: type[BaseModel]
    response_model: type[BaseModel]
    handler: Callable[[BaseModel], Any]


@dataclass(frozen=True, slots=True)
class EventBinding:
    event_type: str
    payload_model: type[BaseModel]
    handler: Callable[[EventEnvelope[Any]], Any]


def rpc_method(
    *,
    request: type[BaseModel],
    response: type[BaseModel],
    name: str | None = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Expose a service method to other ARC services using typed Pydantic models.

    Example::

        @rpc_method(request=SearchRequest, response=SearchResponse)
        async def search(self, request: SearchRequest) -> SearchResponse: ...
    """
    if not issubclass(request, BaseModel) or not issubclass(response, BaseModel):
        raise TypeError(
            "RPC request and response types must be Pydantic BaseModel classes"
        )
    if name is not None and not name.isidentifier():
        raise ValueError("RPC method name must be a Python identifier")

    def decorate(function: Callable[..., Any]) -> Callable[..., Any]:
        setattr(function, "__arc_rpc_spec__", RpcSpec(request, response, name))
        return function

    return decorate


def event_handler(
    event_type: str,
    *,
    payload: type[BaseModel],
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Subscribe a service method to a durable, typed event.

    A successful handler return acknowledges the event. An exception leaves it
    unacknowledged so Core retries it with at-least-once delivery semantics.
    """
    if not _EVENT_TYPE_RE.fullmatch(event_type):
        raise ValueError(f"Invalid event type: {event_type!r}")
    if not issubclass(payload, BaseModel):
        raise TypeError("Event payload type must be a Pydantic BaseModel class")

    def decorate(function: Callable[..., Any]) -> Callable[..., Any]:
        setattr(function, "__arc_event_spec__", EventSpec(event_type, payload))
        return function

    return decorate


def discover_bindings(
    service: object,
) -> tuple[dict[str, RpcBinding], dict[str, EventBinding]]:
    """Find RPC and event methods declared by decorators on a Service instance."""
    rpc_bindings: dict[str, RpcBinding] = {}
    event_bindings: dict[str, EventBinding] = {}

    for attribute_name in dir(type(service)):
        function = getattr(type(service), attribute_name, None)
        if not callable(function):
            continue
        bound = getattr(service, attribute_name, None)
        if not callable(bound):
            continue

        rpc_spec: RpcSpec | None = getattr(function, "__arc_rpc_spec__", None)
        if rpc_spec is not None:
            method_name = rpc_spec.name or attribute_name
            if method_name in rpc_bindings:
                raise ValueError(f"Duplicate RPC method name: {method_name}")
            rpc_bindings[method_name] = RpcBinding(
                name=method_name,
                request_model=rpc_spec.request_model,
                response_model=rpc_spec.response_model,
                handler=bound,
            )

        event_spec: EventSpec | None = getattr(function, "__arc_event_spec__", None)
        if event_spec is not None:
            if event_spec.event_type in event_bindings:
                raise ValueError(
                    "Only one handler per event type per service is supported: "
                    f"{event_spec.event_type}"
                )
            event_bindings[event_spec.event_type] = EventBinding(
                event_type=event_spec.event_type,
                payload_model=event_spec.payload_model,
                handler=bound,
            )

    return rpc_bindings, event_bindings


class ServiceDirectory:
    """Request/response access to other running ARC services."""

    def __init__(self, client: CommunicationClient) -> None:
        self._client = client

    async def call(
        self,
        service_id: str,
        method: str,
        request: BaseModel,
        *,
        response_model: type[PayloadT],
        timeout: float = DEFAULT_CALL_TIMEOUT,
    ) -> PayloadT:
        if not isinstance(request, BaseModel):
            raise TypeError("RPC requests must be Pydantic BaseModel instances")
        if not issubclass(response_model, BaseModel):
            raise TypeError("RPC response must be a Pydantic BaseModel class")
        if not _SERVICE_ID_RE.fullmatch(service_id):
            raise ValueError(f"Invalid service ID: {service_id!r}")
        if not method.isidentifier():
            raise ValueError(f"Invalid RPC method name: {method!r}")

        result = await self._client._request(
            RpcCall(
                id=str(uuid.uuid4()),
                target=service_id,
                method=method,
                payload=request.model_dump(mode="json"),
            ),
            response_type="rpc_result",
            timeout=timeout,
        )
        if not result.get("ok"):
            error = result.get("error") or "Remote call failed"
            if result.get("error_type") == "SERVICE_UNAVAILABLE":
                raise ServiceUnavailableError(error)
            raise RemoteCallError(error)
        payload = result.get("payload")
        try:
            return response_model.model_validate(payload)
        except Exception as exc:
            raise ProtocolError(
                f"Invalid response from service {service_id!r}, method {method!r}: {exc}"
            ) from exc

    async def list(
        self, *, timeout: float = DEFAULT_CALL_TIMEOUT
    ) -> list[ServiceSummary]:
        result = await self._client._request(
            ListServices(id=str(uuid.uuid4())),
            response_type="services",
            timeout=timeout,
        )
        return [
            ServiceSummary.model_validate(item) for item in result.get("services", [])
        ]

    async def is_available(self, service_id: str) -> bool:
        return any(
            service.service_id == service_id and service.active
            for service in await self.list()
        )


class EventBus:
    """Publish typed events; delivery and acknowledgement are handled by ARC Core."""

    def __init__(self, client: CommunicationClient) -> None:
        self._client = client

    async def publish(self, event_type: str, payload: BaseModel) -> PublishReceipt:
        if not _EVENT_TYPE_RE.fullmatch(event_type):
            raise ValueError(f"Invalid event type: {event_type!r}")
        if not isinstance(payload, BaseModel):
            raise TypeError("Event payloads must be Pydantic BaseModel instances")
        result = await self._client._request(
            Publish(
                id=str(uuid.uuid4()),
                event_type=event_type,
                payload=payload.model_dump(mode="json"),
            ),
            response_type="published",
            timeout=DEFAULT_CALL_TIMEOUT,
        )
        if not result.get("ok", True):
            raise EventQueueFullError(
                result.get("error") or "Durable event queue is full"
            )
        return PublishReceipt(
            event_id=result["event_id"],
            queued_for=result["queued_for"],
        )


class CommunicationClient:
    """One persistent local-socket connection for an ARC service process."""

    def __init__(
        self,
        service_id: str,
        socket_path: str | Path | None = None,
        *,
        logger: logging.Logger | None = None,
        rpc_bindings: Mapping[str, RpcBinding] | None = None,
        event_bindings: Mapping[str, EventBinding] | None = None,
    ) -> None:
        if not _SERVICE_ID_RE.fullmatch(service_id):
            raise ValueError(f"Invalid service ID: {service_id!r}")
        path = socket_path or os.environ.get("ARC_COMMUNICATION_SOCKET")
        if not path:
            raise CommunicationUnavailableError(
                "ARC_COMMUNICATION_SOCKET is not set; start this service through ARC Core"
            )
        self.service_id = service_id
        self.socket_path = str(path)
        self.logger = logger or logging.getLogger(
            f"arc_service.communication.{service_id}"
        )
        self.rpc_bindings = dict(rpc_bindings or {})
        self.event_bindings = dict(event_bindings or {})

        self.services = ServiceDirectory(self)
        self.events = EventBus(self)

        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._send_lock = asyncio.Lock()
        self._pending: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._event_tasks: dict[str, asyncio.Task[None]] = {}
        self._reader_task: asyncio.Task[None] | None = None
        self._hello_future: asyncio.Future[dict[str, Any]] | None = None
        self._closed = False
        self._activated = False

    async def connect(self, *, timeout: float = DEFAULT_CALL_TIMEOUT) -> None:
        if self._writer is not None:
            return
        self._closed = False
        loop = asyncio.get_running_loop()
        self._hello_future = loop.create_future()
        try:
            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_unix_connection(
                    self.socket_path,
                    limit=MAX_FRAME_BYTES,
                ),
                timeout=timeout,
            )
        except (OSError, TimeoutError) as exc:
            raise CommunicationUnavailableError(
                f"Could not connect to ARC communication socket {self.socket_path!r}: {exc}"
            ) from exc

        self._reader_task = asyncio.create_task(
            self._read_loop(),
            name=f"arc-communication-reader:{self.service_id}",
        )
        try:
            await self._send(
                Hello(
                    service_id=self.service_id,
                    rpc_methods=sorted(self.rpc_bindings),
                    subscriptions=sorted(self.event_bindings),
                )
            )
            ack = await asyncio.wait_for(self._hello_future, timeout=timeout)
            if not ack.get("ok"):
                raise CommunicationUnavailableError(
                    ack.get("error") or "ARC communication handshake failed"
                )
        except BaseException:
            await self.close()
            raise

    async def activate(self) -> None:
        """Enable event delivery after the service's run task has started."""
        if self._writer is None:
            raise CommunicationUnavailableError("Communication client is not connected")
        if self._activated:
            return
        await self._send(Activate())
        self._activated = True

    async def close(self) -> None:
        self._closed = True
        for task in list(self._event_tasks.values()):
            task.cancel()
        if self._event_tasks:
            await asyncio.gather(*self._event_tasks.values(), return_exceptions=True)
        self._event_tasks.clear()

        if (
            self._reader_task is not None
            and self._reader_task is not asyncio.current_task()
        ):
            self._reader_task.cancel()
            await asyncio.gather(self._reader_task, return_exceptions=True)
        self._reader_task = None

        writer, self._writer = self._writer, None
        self._reader = None
        if writer is not None:
            writer.close()
            try:
                await writer.wait_closed()
            except (BrokenPipeError, ConnectionResetError, OSError):
                pass

        error = CommunicationUnavailableError("ARC communication connection closed")
        for future in self._pending.values():
            if not future.done():
                future.set_exception(error)
        self._pending.clear()

    async def _send(self, message: BaseModel) -> None:
        writer = self._writer
        if writer is None or writer.is_closing():
            raise CommunicationUnavailableError(
                "ARC communication broker is disconnected"
            )
        data = message.model_dump_json().encode("utf-8") + b"\n"
        if len(data) > MAX_FRAME_BYTES:
            raise ProtocolError(f"Message exceeds {MAX_FRAME_BYTES} bytes")
        async with self._send_lock:
            writer.write(data)
            try:
                await writer.drain()
            except (BrokenPipeError, ConnectionResetError, OSError) as exc:
                raise CommunicationUnavailableError(
                    f"Could not write to ARC communication broker: {exc}"
                ) from exc

    async def _request(
        self,
        message: BaseModel,
        *,
        response_type: str,
        timeout: float,
    ) -> dict[str, Any]:
        if timeout <= 0:
            raise ValueError("timeout must be greater than zero")
        request_id = getattr(message, "id", None)
        if not request_id:
            raise ProtocolError("Request message is missing an ID")
        future: asyncio.Future[dict[str, Any]] = (
            asyncio.get_running_loop().create_future()
        )
        self._pending[request_id] = future
        try:
            await self._send(message)
            try:
                async with asyncio.timeout(timeout):
                    result = await future
            except TimeoutError as exc:
                raise RpcTimeoutError(
                    f"ARC communication request timed out after {timeout:.2f}s"
                ) from exc
            if result.get("type") != response_type:
                raise ProtocolError(
                    f"Expected {response_type!r}, received {result.get('type')!r}"
                )
            return result
        finally:
            self._pending.pop(request_id, None)

    async def _read_loop(self) -> None:
        assert self._reader is not None
        try:
            while not self._closed:
                raw = await self._reader.readline()
                if not raw:
                    raise CommunicationUnavailableError(
                        "ARC communication socket closed"
                    )
                if len(raw) > MAX_FRAME_BYTES:
                    raise ProtocolError("Incoming message exceeds maximum frame size")
                try:
                    message = json.loads(raw)
                    if not isinstance(message, dict) or not isinstance(
                        message.get("type"), str
                    ):
                        raise ValueError(
                            "message must be a JSON object with a type field"
                        )
                except (json.JSONDecodeError, ValueError) as exc:
                    raise ProtocolError(
                        f"Invalid JSON from ARC communication broker: {exc}"
                    ) from exc

                message_type = message["type"]
                if message_type == "hello_ack":
                    message = HelloAck.model_validate(message).model_dump(mode="json")
                    if self._hello_future is not None and not self._hello_future.done():
                        self._hello_future.set_result(message)
                elif message_type in {"rpc_result", "published", "services"}:
                    if message_type == "rpc_result":
                        message = RpcResult.model_validate(message).model_dump(
                            mode="json"
                        )
                    elif message_type == "published":
                        message = Published.model_validate(message).model_dump(
                            mode="json"
                        )
                    else:
                        message = ServicesList.model_validate(message).model_dump(
                            mode="json"
                        )
                    request_id = message.get("id")
                    future = self._pending.get(request_id)
                    if future is not None and not future.done():
                        future.set_result(message)
                elif message_type == "rpc_request":
                    message = RpcRequest.model_validate(message).model_dump(
                        mode="python"
                    )
                    asyncio.create_task(self._execute_rpc(message))
                elif message_type == "event":
                    message = EventDelivery.model_validate(message).model_dump(
                        mode="python"
                    )
                    delivery_id = message.get("delivery_id")
                    if not isinstance(delivery_id, str):
                        raise ProtocolError("Event delivery is missing delivery_id")
                    if delivery_id not in self._event_tasks:
                        task = asyncio.create_task(self._execute_event(message))
                        self._event_tasks[delivery_id] = task
                        task.add_done_callback(
                            lambda done, key=delivery_id: self._event_tasks.pop(
                                key, None
                            )
                        )
                elif message_type == "protocol_error":
                    self.logger.error(
                        "ARC communication protocol error: %s", message.get("error")
                    )
                else:
                    raise ProtocolError(
                        f"Unknown broker message type: {message_type!r}"
                    )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            if not self._closed:
                self.logger.error("ARC communication reader stopped: %s", exc)
                for future in self._pending.values():
                    if not future.done():
                        future.set_exception(
                            CommunicationUnavailableError(
                                f"Communication connection lost: {exc}"
                            )
                        )
                if self._hello_future is not None and not self._hello_future.done():
                    self._hello_future.set_exception(
                        CommunicationUnavailableError(f"Handshake failed: {exc}")
                    )

    async def _execute_rpc(self, message: dict[str, Any]) -> None:
        call_id = message.get("id")
        method_name = message.get("method")
        binding = self.rpc_bindings.get(method_name)
        if binding is None:
            await self._send(
                RpcResult(
                    id=str(call_id),
                    ok=False,
                    error=f"RPC method {method_name!r} is not exposed by service {self.service_id!r}",
                    error_type="RPC_METHOD_NOT_FOUND",
                )
            )
            return
        try:
            request = binding.request_model.model_validate(message.get("payload"))
            result = binding.handler(request)
            if inspect.isawaitable(result):
                result = await result
            response = binding.response_model.model_validate(result)
            await self._send(
                RpcResult(
                    id=str(call_id),
                    ok=True,
                    payload=response.model_dump(mode="json"),
                )
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.logger.exception("RPC method %s failed", method_name)
            try:
                await self._send(
                    RpcResult(
                        id=str(call_id),
                        ok=False,
                        error=f"{type(exc).__name__}: {exc}",
                        error_type=type(exc).__name__,
                    )
                )
            except CommunicationError:
                pass

    async def _execute_event(self, message: dict[str, Any]) -> None:
        delivery_id = str(message["delivery_id"])
        event_type = str(message["event_type"])
        binding = self.event_bindings.get(event_type)
        if binding is None:
            # This should only happen if the service's subscription metadata and
            # Core's persistent subscription table disagree. Nack, do not lose it.
            try:
                await self._send(
                    EventNack(
                        delivery_id=delivery_id, error="No local handler for event type"
                    )
                )
            except CommunicationError:
                pass
            return

        stop_heartbeat = asyncio.Event()

        async def heartbeat() -> None:
            while not stop_heartbeat.is_set():
                try:
                    await asyncio.wait_for(
                        stop_heartbeat.wait(), timeout=EVENT_LEASE_SECONDS / 3
                    )
                    return
                except TimeoutError:
                    try:
                        await self._send(EventExtend(delivery_id=delivery_id))
                    except CommunicationError:
                        return

        heartbeat_task = asyncio.create_task(heartbeat())
        try:
            event_model = EventEnvelope[binding.payload_model].model_validate(
                {
                    "id": message["event_id"],
                    "type": event_type,
                    "source": message["source"],
                    "occurred_at": message["occurred_at"],
                    "payload": message["payload"],
                    "attempt": message["attempt"],
                }
            )
            result = binding.handler(event_model)
            if inspect.isawaitable(result):
                await result
            await self._send(EventAck(delivery_id=delivery_id))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.logger.exception("Event handler for %s failed", event_type)
            try:
                await self._send(
                    EventNack(
                        delivery_id=delivery_id,
                        error=f"{type(exc).__name__}: {exc}"[:2048],
                    )
                )
            except CommunicationError:
                pass
        finally:
            stop_heartbeat.set()
            heartbeat_task.cancel()
            await asyncio.gather(heartbeat_task, return_exceptions=True)
