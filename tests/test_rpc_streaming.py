from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from typing import Literal

from pydantic import BaseModel

from arc_service.communication import (
    CommunicationClient,
    RemoteCallError,
    discover_bindings,
    rpc_stream,
)


class AgentRequest(BaseModel):
    prompt: str


class TextEvent(BaseModel):
    kind: Literal["text"]
    text: str


class ToolEvent(BaseModel):
    kind: Literal["tool"]
    name: str


AgentEvent = TextEvent | ToolEvent


def frame(message: dict) -> bytes:
    return (json.dumps(message) + "\n").encode("utf-8")


class StreamingService:
    @rpc_stream(request=AgentRequest, event=AgentEvent)
    async def generate(self, request: AgentRequest):
        yield TextEvent(kind="text", text=request.prompt)
        yield ToolEvent(kind="tool", name="search")


class RpcStreamingTests(unittest.IsolatedAsyncioTestCase):
    async def test_discovery_registers_stream_binding(self) -> None:
        rpc, streams, events = discover_bindings(StreamingService())
        self.assertEqual(rpc, {})
        self.assertIn("generate", streams)
        self.assertEqual(events, {})

    async def test_client_yields_typed_union_items(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            socket_path = str(Path(tmp) / "broker.sock")
            received: list[dict] = []

            async def broker(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
                try:
                    hello = json.loads(await reader.readline())
                    self.assertEqual(hello["type"], "hello")
                    writer.write(frame({"type": "hello_ack", "ok": True}))
                    await writer.drain()

                    request = json.loads(await reader.readline())
                    received.append(request)
                    writer.write(frame({
                        "type": "rpc_stream_item",
                        "id": request["id"],
                        "payload": {"kind": "text", "text": "hello"},
                    }))
                    writer.write(frame({
                        "type": "rpc_stream_item",
                        "id": request["id"],
                        "payload": {"kind": "tool", "name": "search"},
                    }))
                    writer.write(frame({
                        "type": "rpc_stream_end", "id": request["id"], "ok": True
                    }))
                    await writer.drain()
                    await asyncio.sleep(0.05)
                finally:
                    writer.close()
                    await writer.wait_closed()

            server = await asyncio.start_unix_server(broker, path=socket_path)
            client = CommunicationClient("caller", socket_path=socket_path)
            try:
                await client.connect()
                items = [
                    item async for item in client.services.stream(
                        "agent",
                        "generate",
                        AgentRequest(prompt="hello"),
                        event_model=AgentEvent,
                    )
                ]
                self.assertIsInstance(items[0], TextEvent)
                self.assertIsInstance(items[1], ToolEvent)
                self.assertEqual(items[0].text, "hello")
                self.assertEqual(items[1].name, "search")
                self.assertEqual(received[0]["type"], "rpc_stream_call")
            finally:
                await client.close()
                server.close()
                await server.wait_closed()

    async def test_remote_stream_error_is_raised(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            socket_path = str(Path(tmp) / "broker.sock")

            async def broker(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
                try:
                    await reader.readline()
                    writer.write(frame({"type": "hello_ack", "ok": True}))
                    await writer.drain()
                    request = json.loads(await reader.readline())
                    writer.write(frame({
                        "type": "rpc_stream_end",
                        "id": request["id"],
                        "ok": False,
                        "error": "agent failed",
                        "error_type": "RuntimeError",
                    }))
                    await writer.drain()
                    await asyncio.sleep(0.05)
                finally:
                    writer.close()
                    await writer.wait_closed()

            server = await asyncio.start_unix_server(broker, path=socket_path)
            client = CommunicationClient("caller", socket_path=socket_path)
            try:
                await client.connect()
                with self.assertRaisesRegex(RemoteCallError, "agent failed"):
                    async for _ in client.services.stream(
                        "agent",
                        "generate",
                        AgentRequest(prompt="hello"),
                        event_model=AgentEvent,
                    ):
                        pass
            finally:
                await client.close()
                server.close()
                await server.wait_closed()

    async def test_provider_dispatches_typed_items_and_end(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            socket_path = str(Path(tmp) / "broker.sock")
            service = StreamingService()
            rpc, streams, events = discover_bindings(service)
            received: list[dict] = []

            async def broker(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
                try:
                    hello = json.loads(await reader.readline())
                    self.assertIn("generate", hello["rpc_methods"])
                    writer.write(frame({"type": "hello_ack", "ok": True}))
                    await writer.drain()
                    writer.write(frame({
                        "type": "rpc_stream_request",
                        "id": "stream-1",
                        "caller": "caller",
                        "method": "generate",
                        "payload": {"prompt": "from provider"},
                    }))
                    await writer.drain()
                    for _ in range(3):
                        received.append(json.loads(await reader.readline()))
                finally:
                    writer.close()
                    await writer.wait_closed()

            server = await asyncio.start_unix_server(broker, path=socket_path)
            client = CommunicationClient(
                "agent",
                socket_path=socket_path,
                rpc_bindings=rpc,
                stream_bindings=streams,
                event_bindings=events,
            )
            try:
                await client.connect()
                for _ in range(100):
                    if len(received) == 3:
                        break
                    await asyncio.sleep(0.01)
                self.assertEqual([item["type"] for item in received], [
                    "rpc_stream_item", "rpc_stream_item", "rpc_stream_end"
                ])
                self.assertEqual(received[0]["payload"], {
                    "kind": "text", "text": "from provider"
                })
                self.assertEqual(received[1]["payload"], {
                    "kind": "tool", "name": "search"
                })
                self.assertTrue(received[2]["ok"])
            finally:
                await client.close()
                server.close()
                await server.wait_closed()


if __name__ == "__main__":
    unittest.main()
