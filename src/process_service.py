"""Six source replicas hosted in six independent OS processes.

This transport exercises the same JSON-line RPC contract as ``src.service``.
It is a validation harness, not a production deployment: all children remain
on one host and use loopback TCP, but an OS-process termination no longer shares
Python heap state with the surviving replicas or coordinator.
"""
from __future__ import annotations

import asyncio
import json
import multiprocessing as mp
import os
import resource
import time
from dataclasses import dataclass
from typing import Any

from .service import Replica, wire

_MESSAGE_LIMIT = 8 * 1024 * 1024


def _child_main(
    ready,
    node: int,
    shard: int,
    initial: dict[str, str],
    payloads: dict,
) -> None:
    replica = Replica(node, shard, initial, payloads)

    async def run() -> None:
        stop = asyncio.Event()

        async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            try:
                while True:
                    raw = await reader.readline()
                    if not raw:
                        break
                    try:
                        if len(raw) > _MESSAGE_LIMIT:
                            reply = {"error": "message-bound"}
                        else:
                            request = json.loads(raw)
                            operation = request.get("op")
                            if operation == "process-shutdown":
                                reply = {"ok": True, "pid": os.getpid()}
                                stop.set()
                            elif operation == "process-stat":
                                reply = {
                                    **replica.handle({"op": "stat"}),
                                    "pid": os.getpid(),
                                    "rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                                }
                            else:
                                reply = replica.handle(request)
                    except (ValueError, KeyError, TypeError, ConnectionError) as exc:
                        reply = {"error": "malformed-request", "type": type(exc).__name__}
                    writer.write(wire(reply))
                    await writer.drain()
                    if stop.is_set():
                        break
            finally:
                writer.close()
                try:
                    await asyncio.wait_for(writer.wait_closed(), 1)
                except (TimeoutError, ConnectionError, BrokenPipeError):
                    pass

        server = await asyncio.start_server(
            handler, "127.0.0.1", 0, limit=_MESSAGE_LIMIT + 1
        )
        port = server.sockets[0].getsockname()[1]
        ready.send({"port": port, "pid": os.getpid()})
        ready.close()
        async with server:
            await stop.wait()
        server.close()
        await server.wait_closed()

    asyncio.run(run())


@dataclass
class _Endpoint:
    process: mp.Process
    port: int
    pid: int
    reader: asyncio.StreamReader | None = None
    writer: asyncio.StreamWriter | None = None


class ProcessNetwork:
    """Coordinator-side RPC client for six isolated replica processes."""

    def __init__(self, initial: list[dict[str, str]], payloads: dict, start_method: str = "spawn"):
        if len(initial) != 3:
            raise ValueError("three logical shard states required")
        self.initial = [dict(state) for state in initial]
        self.payloads = payloads
        self.context = mp.get_context(start_method)
        self.endpoints: list[_Endpoint | None] = [None] * 6
        self.blocked: set[int] = set()
        self.delay_ms: dict[int, int] = {}
        self.next_id = 0
        self.evidence: dict[int, dict] = {}
        self.attempts: list[dict] = []
        self.sent_bytes = self.reply_bytes = self.messages = self.drops = 0
        self.virtual_ms = 0
        self.server_cpu_ns = 0  # Child CPU is reported only at suite granularity.
        self.start_method = start_method
        self.started_pids: list[int] = []
        self.killed_pids: list[int] = []
        self.restarted_pids: list[int] = []

    def _spawn(self, node: int) -> _Endpoint:
        parent, child = self.context.Pipe(duplex=False)
        process = self.context.Process(
            target=_child_main,
            args=(child, node, node // 2, self.initial[node // 2], self.payloads),
            name=f"federated-source-{node}",
        )
        process.start()
        child.close()
        if not parent.poll(15):
            process.terminate()
            process.join(5)
            raise TimeoutError(f"replica process {node} did not publish a port")
        message = parent.recv()
        parent.close()
        endpoint = _Endpoint(process=process, port=message["port"], pid=message["pid"])
        if endpoint.pid != process.pid:
            raise RuntimeError("child PID handshake mismatch")
        return endpoint

    async def _connect(self, endpoint: _Endpoint) -> None:
        reader, writer = await asyncio.open_connection(
            "127.0.0.1", endpoint.port, limit=_MESSAGE_LIMIT + 1
        )
        endpoint.reader, endpoint.writer = reader, writer

    async def __aenter__(self) -> "ProcessNetwork":
        for node in range(6):
            endpoint = self._spawn(node)
            await self._connect(endpoint)
            self.endpoints[node] = endpoint
            self.started_pids.append(endpoint.pid)
        if len(set(self.started_pids)) != 6:
            raise RuntimeError("source replicas did not receive distinct PIDs")
        return self

    async def _close_endpoint(self, endpoint: _Endpoint | None, graceful: bool) -> None:
        if endpoint is None:
            return
        if graceful and endpoint.process.is_alive() and endpoint.writer is not None:
            try:
                endpoint.writer.write(wire({"op": "process-shutdown"}))
                await asyncio.wait_for(endpoint.writer.drain(), 1)
                if endpoint.reader is not None:
                    await asyncio.wait_for(endpoint.reader.readline(), 1)
            except (TimeoutError, ConnectionError, BrokenPipeError, OSError):
                pass
        if endpoint.writer is not None:
            endpoint.writer.close()
            try:
                await asyncio.wait_for(endpoint.writer.wait_closed(), 1)
            except (TimeoutError, ConnectionError, BrokenPipeError, OSError):
                pass
        endpoint.process.join(2)
        if endpoint.process.is_alive():
            endpoint.process.terminate()
            endpoint.process.join(3)
        if endpoint.process.is_alive():
            endpoint.process.kill()
            endpoint.process.join(3)

    async def __aexit__(self, *exc: Any) -> None:
        for endpoint in self.endpoints:
            await self._close_endpoint(endpoint, graceful=True)
        self.endpoints = [None] * 6

    async def kill(self, node: int) -> int:
        endpoint = self.endpoints[node]
        if endpoint is None:
            raise ValueError("replica already absent")
        old_pid = endpoint.pid
        if endpoint.writer is not None:
            endpoint.writer.close()
        endpoint.process.terminate()
        endpoint.process.join(5)
        if endpoint.process.is_alive():
            endpoint.process.kill()
            endpoint.process.join(3)
        self.killed_pids.append(old_pid)
        self.endpoints[node] = None
        return old_pid

    async def restart(self, node: int) -> int:
        if self.endpoints[node] is not None:
            await self._close_endpoint(self.endpoints[node], graceful=True)
        endpoint = self._spawn(node)
        await self._connect(endpoint)
        self.endpoints[node] = endpoint
        self.restarted_pids.append(endpoint.pid)
        return endpoint.pid

    def alive_pids(self) -> list[int]:
        return sorted(
            endpoint.pid
            for endpoint in self.endpoints
            if endpoint is not None and endpoint.process.is_alive()
        )

    async def rpc(
        self,
        node: int,
        req: dict,
        *,
        lost_reply: bool = False,
        administrative: bool = False,
    ) -> tuple[int | None, dict | None]:
        raw = wire(req)
        sent_req = json.loads(raw)
        attempt = {
            "node": node,
            "request": sent_req,
            "lost_reply": lost_reply,
            "administrative": administrative,
        }
        self.attempts.append(attempt)
        if node in self.blocked and not administrative:
            attempt["outcome"] = "blocked"
            self.drops += 1
            self.virtual_ms += 25
            return None, None
        endpoint = self.endpoints[node]
        if endpoint is None or not endpoint.process.is_alive():
            attempt["outcome"] = "process-unavailable"
            self.drops += 1
            self.virtual_ms += 25
            return None, None

        self.next_id += 1
        ident = self.next_id
        attempt["receipt"] = ident
        self.sent_bytes += len(raw)
        self.messages += 1
        self.virtual_ms += 1 + self.delay_ms.get(node, 0)
        try:
            if endpoint.writer is None or endpoint.reader is None:
                raise ConnectionError("replica connection absent")
            endpoint.writer.write(raw)
            await asyncio.wait_for(endpoint.writer.drain(), 2)
            received = await asyncio.wait_for(endpoint.reader.readline(), 2)
            if not received:
                raise ConnectionError("replica endpoint closed")
        except (TimeoutError, ConnectionError, BrokenPipeError, OSError):
            attempt["outcome"] = "process-unavailable"
            self.drops += 1
            return None, None

        self.reply_bytes += len(received)
        self.messages += 1
        reply = json.loads(received)
        if lost_reply:
            attempt["outcome"] = "reply-lost"
            self.drops += 1
            return None, None
        attempt["outcome"] = "delivered"
        self.evidence[ident] = {"node": node, "request": sent_req, "reply": reply}
        return ident, reply

    def counters(self) -> dict:
        return {
            "bytes": self.sent_bytes + self.reply_bytes,
            "messages": self.messages,
            "virtual_ms": self.virtual_ms,
            "server_cpu_ns": self.server_cpu_ns,
            "dropped_requests_or_replies": self.drops,
        }
