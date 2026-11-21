"""Runs AG-UI requests against Hermes ``/v1/runs``."""

from __future__ import annotations

import asyncio
import hashlib
from contextlib import aclosing
from collections.abc import AsyncGenerator, Awaitable, Callable, Collection, Mapping, Sequence
from typing import Any

from ag_ui.core import (
    BaseEvent,
    Message,
    ResumeEntry,
    RunAgentInput,
    RunErrorEvent,
    RunFinishedEvent,
    RunFinishedSuccessOutcome,
    RunStartedEvent,
    StateSnapshotEvent,
)

from .client import HermesAPIError, HermesError, HermesRunsClient, RunStreamGone
from .store import InMemoryThreadStore, ThreadRecord, ThreadStore
from .translator import TERMINAL_STATUSES, CustomEventMapper, HermesTranslator, ToolFilter

StateProvider = Callable[[str], Awaitable[Any]]
"""``thread_id -> state``; ``None`` emits nothing."""


class HermesAgent:
    """AG-UI agent backed by a Hermes API server.

    A client disconnect only closes the event stream; the Hermes run keeps going and
    :meth:`connect` can rejoin it. Only :meth:`stop` (or a new run on the same thread)
    stops it.
    """

    def __init__(
        self,
        client: HermesRunsClient,
        *,
        store: ThreadStore | None = None,
        tool_filter: ToolFilter | None = None,
        custom_event_mapper: CustomEventMapper | None = None,
        passthrough_unknown: bool = False,
        state_provider: StateProvider | None = None,
        state_refresh_tools: Collection[str] = (),
        run_body: Callable[[RunAgentInput], Mapping[str, Any]] | None = None,
        poll_interval: float = 1.0,
    ) -> None:
        self.client = client
        self.store: ThreadStore = store or InMemoryThreadStore()
        self.tool_filter = tool_filter
        self.custom_event_mapper = custom_event_mapper
        self.passthrough_unknown = passthrough_unknown
        self.state_provider = state_provider
        self.state_refresh_tools = frozenset(state_refresh_tools)
        self.run_body = run_body
        self.poll_interval = poll_interval

    def translator(self, hermes_run_id: str) -> HermesTranslator:
        return HermesTranslator(
            hermes_run_id, tool_filter=self.tool_filter,
            custom_event_mapper=self.custom_event_mapper,
            passthrough_unknown=self.passthrough_unknown,
        )

    async def run(self, input: RunAgentInput) -> AsyncGenerator[BaseEvent, None]:
        """Start a Hermes run for the last user message, or answer approvals on ``resume``."""
        source = self._resume(input) if input.resume else self._start(input)
        async with aclosing(_guard(input, source)) as events:
            async for event in events:
                yield event

    async def connect(self, input: RunAgentInput) -> AsyncGenerator[BaseEvent, None]:
        """Rejoin the thread's in-flight run by replaying its Hermes backlog."""
        async with aclosing(_guard(input, self._connect(input))) as events:
            async for event in events:
                yield event

    async def stop(self, thread_id: str) -> bool:
        record = await self.store.get(thread_id)
        if record is None or record.run_id is None:
            return False
        try:
            await self.client.stop_run(record.run_id)
        except HermesAPIError as exc:
            if exc.status not in (404, 409):
                raise
        return True

    async def _start(self, input: RunAgentInput) -> AsyncGenerator[BaseEvent, None]:
        record = await self.store.get(input.thread_id) or ThreadRecord()
        if record.run_id:
            await self._stop_quietly(record.run_id)
        text, history = _split_messages(input.messages)
        if not text:
            raise HermesError("No user message to send")
        started = await self.client.start_run(
            text,
            session_id=record.session_id,
            conversation_history=history if record.session_id is None else None,
            idempotency_key=_idempotency_key(input.thread_id, input.run_id),
            extra=self.run_body(input) if self.run_body else None,
        )
        if record.session_id is None:
            status = await self.client.get_run(started.run_id)
            record.session_id = status.get("session_id") or None
        record.run_id, record.cursor = started.run_id, -1
        await self.store.put(input.thread_id, record)
        async for event in self._stream(input, record, self.translator(started.run_id)):
            yield event

    async def _resume(self, input: RunAgentInput) -> AsyncGenerator[BaseEvent, None]:
        record = await self.store.get(input.thread_id)
        if record is None or record.run_id is None:
            raise HermesError("No interrupted Hermes run for this thread")
        for entry in input.resume or ():
            try:
                await self.client.respond_approval(
                    record.run_id, request_id=entry.interrupt_id, choice=approval_choice(entry)
                )
            except HermesAPIError as exc:
                if exc.status != 409:  # already answered or timed out: keep following the run
                    raise
        translator = self.translator(record.run_id)
        async for event in self._stream(input, record, translator, silent_until=record.cursor):
            yield event

    async def _connect(self, input: RunAgentInput) -> AsyncGenerator[BaseEvent, None]:
        record = await self.store.get(input.thread_id)
        if record is None or record.run_id is None:
            yield RunStartedEvent(thread_id=input.thread_id, run_id=input.run_id)
            yield RunFinishedEvent(
                thread_id=input.thread_id, run_id=input.run_id, outcome=RunFinishedSuccessOutcome()
            )
            return
        async for event in self._stream(input, record, self.translator(record.run_id)):
            yield event

    async def _stream(
        self,
        input: RunAgentInput,
        record: ThreadRecord,
        t: HermesTranslator,
        *,
        silent_until: int = -1,
    ) -> AsyncGenerator[BaseEvent, None]:
        """Feed the run's events from seq 0; those up to ``silent_until`` only rebuild state."""
        run_id = t.hermes_run_id
        opened = False
        try:
            async with aclosing(self.client.events(run_id)) as stream:
                async for event in stream:
                    seq = event.get("seq")
                    silent = isinstance(seq, int) and seq <= silent_until
                    if event.get("event") == "approval.request" and (
                        silent or not await self._approval_pending(run_id, event)
                    ):
                        continue
                    if silent:
                        t.feed(event)
                        continue
                    if not opened:
                        opened = True
                        for opening in await self._open(t, input):
                            yield opening
                    for item in t.feed(event):
                        yield item
                    if t.completed_tool in self.state_refresh_tools:
                        for state in await self._state(input.thread_id):
                            yield state
                    if t.suspended:
                        record.cursor = seq if isinstance(seq, int) else record.cursor
                        await self.store.put(input.thread_id, record)
                        return
                    if t.done:
                        break
        except RunStreamGone:
            pass
        if not opened:
            for opening in await self._open(t, input):
                yield opening
        while not t.done:
            try:
                status = await self.client.get_run(run_id)
            except HermesAPIError as exc:
                if exc.status == 404:  # Hermes forgot the run; release the thread
                    record.run_id, record.cursor = None, -1
                    await self.store.put(input.thread_id, record)
                raise
            approval = status.get("approval")
            if status.get("status") == "waiting_for_approval" and isinstance(approval, dict):
                for item in t.feed(approval):
                    yield item
                if t.suspended:
                    return
            for item in t.finish(status):
                yield item
            if not t.done:
                await asyncio.sleep(self.poll_interval)
        record.run_id, record.cursor = None, -1
        await self.store.put(input.thread_id, record)

    async def _open(self, t: HermesTranslator, input: RunAgentInput) -> list[BaseEvent]:
        return t.start(input.thread_id, input.run_id, input.messages) + await self._state(
            input.thread_id
        )

    async def _state(self, thread_id: str) -> list[BaseEvent]:
        if self.state_provider is None:
            return []
        state = await self.state_provider(thread_id)
        return [] if state is None else [StateSnapshotEvent(snapshot=state)]

    async def _approval_pending(self, run_id: str, event: Mapping[str, Any]) -> bool:
        """Replayed approvals may already be answered; only the run status knows."""
        status = await self.client.get_run(run_id)
        approval = status.get("approval")
        request_id = event.get("request_id")
        return status.get("status") == "waiting_for_approval" and (
            request_id is None or (isinstance(approval, dict) and approval.get("request_id") == request_id)
        )

    async def _stop_quietly(self, run_id: str) -> None:
        try:
            status = await self.client.get_run(run_id)
            if status.get("status") not in TERMINAL_STATUSES:
                await self.client.stop_run(run_id)
        except HermesError:
            pass


def approval_choice(entry: ResumeEntry) -> str:
    """Map a resume entry to a Hermes approval choice (``once``/``session``/``always``/``deny``).

    Accepts ``{"approved": bool}`` (assistant-ui tool approvals), ``{"choice": str}``
    or a bare choice string; a cancelled entry denies.
    """
    payload = entry.payload
    if entry.status == "cancelled":
        return "deny"
    if isinstance(payload, str):
        return payload
    if isinstance(payload, Mapping):
        if isinstance(payload.get("choice"), str):
            return str(payload["choice"])
        if isinstance(payload.get("approved"), bool):
            return "once" if payload["approved"] else "deny"
    return "deny"


def _text_of(message: Message) -> str:
    content = getattr(message, "content", None)
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "".join(str(getattr(part, "text", "")) for part in content if getattr(part, "type", "") == "text")
    return ""


def _split_messages(messages: Sequence[Message]) -> tuple[str, list[dict[str, str]]]:
    """The last user message's text, and the user/assistant text history before it."""
    last = max((i for i, m in enumerate(messages) if m.role == "user"), default=-1)
    if last < 0:
        return "", []
    history = [
        {"role": m.role, "content": _text_of(m)}
        for m in messages[:last]
        if m.role in ("user", "assistant") and _text_of(m)
    ]
    return _text_of(messages[last]), history


def _idempotency_key(thread_id: str, run_id: str) -> str:
    return "ag-ui-" + hashlib.sha256(f"{thread_id}\0{run_id}".encode()).hexdigest()[:48]


async def _guard(input: RunAgentInput, events: AsyncGenerator[BaseEvent, None]) -> AsyncGenerator[BaseEvent, None]:
    """Report Hermes failures as ``RUN_ERROR`` inside a started run."""
    started = False
    try:
        async with aclosing(events):
            async for event in events:
                started = started or isinstance(event, RunStartedEvent)
                yield event
    except HermesError as exc:
        if not started:
            yield RunStartedEvent(thread_id=input.thread_id, run_id=input.run_id)
        code = exc.code if isinstance(exc, HermesAPIError) else None
        yield RunErrorEvent(message=str(exc), code=code or "hermes_error")
