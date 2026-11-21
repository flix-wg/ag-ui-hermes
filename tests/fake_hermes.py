"""In-process stand-in for the Hermes /v1/runs API (wire format of api_server_runs.py)."""

from __future__ import annotations

import asyncio
import json
import re
import time
from typing import TYPE_CHECKING, Any

import httpx

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

Step = dict[str, Any] | asyncio.Event


class FakeRun:
    def __init__(self, run_id: str, session_id: str) -> None:
        self.run_id = run_id
        self.status: dict[str, Any] = {
            "object": "hermes.run",
            "run_id": run_id,
            "status": "running",
            "session_id": session_id,
        }
        self.backlog: list[dict[str, Any]] = []
        self.evicted = 0
        """Seqs below this fell out of the replay backlog."""
        self.closed = False
        self.changed = asyncio.Condition()
        self.answered = asyncio.Event()
        self.task: asyncio.Task[None] | None = None

    async def emit(self, event: dict[str, Any]) -> None:
        async with self.changed:
            if not self.closed:
                self.backlog.append({**event, "run_id": self.run_id, "seq": len(self.backlog)})
                self.changed.notify_all()

    async def finish(self, event: dict[str, Any]) -> None:
        self.status.pop("approval", None)
        self.status.update({k: v for k, v in event.items() if k != "event"}, status=event["event"][4:])
        await self.emit(event)
        async with self.changed:
            self.closed = True
            self.changed.notify_all()

    async def drive(self, script: list[Step]) -> None:
        try:
            for step in script:
                if isinstance(step, asyncio.Event):
                    await step.wait()
                elif step["event"].startswith("run."):
                    await self.finish(step)
                    return
                elif step["event"] == "approval.request":
                    self.status.update(status="waiting_for_approval", approval=dict(step))
                    self.answered.clear()
                    await self.emit(step)
                    await self.answered.wait()
                else:
                    await self.emit(step)
        except asyncio.CancelledError:
            await self.finish({"event": "run.cancelled", "completed": False, "interrupted": True})

    async def sse(self, last_seq: int, drop_after: int | None) -> AsyncIterator[bytes]:
        yield b": open\n\n"
        next_seq = max(last_seq + 1, self.evicted)
        if len(self.backlog) > next_seq > last_seq + 1:  # unsequenced, before the retained events
            marker = {"event": "replay.truncated", "run_id": self.run_id, "timestamp": time.time(), "oldest_retained_seq": next_seq}
            yield f"data: {json.dumps({**marker, 'requested_seq': last_seq} if last_seq >= 0 else marker)}\n\n".encode()
        while True:
            async with self.changed:

                def ready(n: int = next_seq) -> bool:
                    return len(self.backlog) > n or self.closed

                await self.changed.wait_for(ready)
                batch = self.backlog[next_seq:]
            for event in batch:
                if drop_after is not None and next_seq > drop_after:
                    return  # connection lost without the closing comment
                yield f"id: {event['seq']}\ndata: {json.dumps(event)}\n\n".encode()
                next_seq += 1
            if self.closed and next_seq >= len(self.backlog):
                yield b": stream closed\n\n"
                return


class _Stream(httpx.AsyncByteStream):
    def __init__(self, chunks: AsyncIterator[bytes]) -> None:
        self._chunks = chunks

    async def __aiter__(self) -> AsyncIterator[bytes]:
        async for chunk in self._chunks:
            yield chunk


class FakeHermes:
    def __init__(self, *scripts: list[Step], key: str = "secret", drop_once_after: int | None = None) -> None:
        self.scripts = list(scripts)
        self.drop_once_after = drop_once_after
        self.event_cursors: list[int] = []
        self.key = key
        self.runs: dict[str, FakeRun] = {}
        self.calls: list[tuple[str, str, Any]] = []
        self.sessions: dict[str, list[dict[str, Any]]] = {}

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self.handle)

    def _error(self, status: int, code: str) -> httpx.Response:
        return httpx.Response(status, json={"error": {"message": code, "code": code}})

    async def handle(self, request: httpx.Request) -> httpx.Response:
        body: Any = json.loads(request.content) if request.content else {}
        path = request.url.path
        self.calls.append((request.method, path, body))
        if request.headers.get("authorization") != f"Bearer {self.key}":
            return self._error(401, "unauthorized")
        if request.method == "POST" and path == "/v1/runs":
            run_id = f"run_{len(self.runs) + 1}"
            new = self.runs[run_id] = FakeRun(run_id, body.get("session_id") or run_id)
            new.task = asyncio.create_task(new.drive(self.scripts.pop(0)))
            return httpx.Response(202, json={"run_id": run_id, "status": "started", "replayed": False})
        if path == "/v1/capabilities":
            features = {"run_events_sse": True, "approval_events": True, "reasoning_streaming": True}
            return httpx.Response(200, json={"object": "hermes.api_server.capabilities", "model": "hermes-agent", "features": features})
        session = re.fullmatch(r"/api/sessions/([^/]+)/messages", path)
        if session:
            rows = self.sessions.get(session.group(1))
            if rows is None:
                return self._error(404, "session_not_found")
            return httpx.Response(200, json={"object": "list", "session_id": session.group(1), "data": rows})
        match = re.fullmatch(r"/v1/runs/([^/]+)(/events|/stop|/approval)?", path)
        run = self.runs.get(match.group(1)) if match else None
        if run is None or match is None:
            return self._error(404, "run_not_found")
        action = match.group(2)
        if action is None:
            return httpx.Response(200, json=run.status)
        if action == "/events":
            last_seq = int(request.headers.get("last-event-id", "-1"))
            self.event_cursors.append(last_seq)
            drop, self.drop_once_after = self.drop_once_after, None
            return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=_Stream(run.sse(last_seq, drop)))
        if action == "/stop":
            assert run.task is not None
            run.task.cancel()
            return httpx.Response(200, json={"run_id": run.run_id, "status": "stopping"})
        pending = run.status.get("approval", {}).get("request_id")
        if run.status["status"] != "waiting_for_approval" or body.get("request_id") not in (None, pending):
            return self._error(409, "approval_not_pending")
        run.status["status"] = "running"
        run.status.pop("approval", None)
        await run.emit({"event": "approval.responded", **body, "resolved": 1})
        run.answered.set()
        return httpx.Response(200, json={"run_id": run.run_id, "choice": body["choice"], "resolved": 1})
