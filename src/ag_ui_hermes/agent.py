"""Runs AG-UI requests against Hermes ``/v1/runs``."""

from __future__ import annotations

import asyncio
import hashlib
import logging
from collections.abc import AsyncGenerator, Awaitable, Callable, Collection, Mapping, Sequence
from contextlib import aclosing
from dataclasses import dataclass
from functools import partial
from typing import Any, Literal

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
    SubagentFinishedEvent,
)

from .client import HermesAPIError, HermesError, RunsClient, RunStreamGoneError
from .store import InMemoryThreadStore, ThreadRecord, ThreadStore
from .translator import TERMINAL_STATUSES, HermesTranslator, run_started

logger = logging.getLogger("ag_ui_hermes")

_STARTED_HISTORY = 32
"""Started AG-UI runs per thread whose late retries still find their Hermes run."""
_CAS_ATTEMPTS = 50
"""Compare-and-set retries on one thread record before giving up."""


@dataclass(frozen=True)
class HookContext:
    """The request a hook runs for; every hook gets it as its first argument."""

    action: Literal["run", "connect", "stop"]
    thread_id: str
    run_id: str
    input: RunAgentInput | None = None
    """The AG-UI input; ``None`` for ``stop``."""
    request: Any = None
    """The incoming HTTP request (a Starlette ``Request`` from the FastAPI endpoint), if any."""


Authorize = Callable[[HookContext], Awaitable[bool]]
"""``False`` refuses the request (``AgentForbiddenError``, HTTP 403 from the endpoint)."""
StateProvider = Callable[[HookContext], Awaitable[Any]]
"""State for a ``STATE_SNAPSHOT``; ``None`` emits nothing."""
ToolFilter = Callable[[HookContext, str, str, Mapping[str, Any]], "tuple[str, str] | None"]
"""``(context, tool, preview, raw_event) -> (tool, preview)`` to rewrite, or ``None`` to drop."""
CustomEventMapper = Callable[[HookContext, Mapping[str, Any]], "Sequence[BaseEvent] | None"]
"""Maps a Hermes event the translator does not know; ``None`` falls through to the default."""
RunBody = Callable[[HookContext], Mapping[str, Any]]
"""Extra ``POST /v1/runs`` body fields."""
ErrorHook = Callable[[HookContext, Exception], None]
"""Sees every error the agent reports; its own exceptions are logged and ignored."""


class AgentInputError(HermesError):
    """The request cannot be run as sent; nothing was started or stopped."""

    code = "invalid_input"
    public_message = "The request is not valid for this thread."


class AgentForbiddenError(HermesError):
    """The ``authorize`` hook refused the request."""

    code = "forbidden"
    public_message = "The request is not allowed."


class StoreConflictError(HermesError):
    """The thread store kept refusing a compare-and-set write."""

    code = "store_conflict"
    public_message = "The thread is busy; try again."


class RunTimeoutError(HermesError):
    """The run did not finish before ``poll_timeout`` while its status was polled."""

    code = "hermes_timeout"
    public_message = "Timed out waiting for the Hermes run."


class HermesAgent:
    """AG-UI agent backed by a Hermes API server.

    A client disconnect only closes the event stream; the Hermes run keeps going and
    :meth:`connect` can rejoin it. Only :meth:`stop` (or a new run on the same thread)
    stops it.
    """

    def __init__(
        self,
        client: RunsClient,
        *,
        store: ThreadStore | None = None,
        authorize: Authorize | None = None,
        tool_filter: ToolFilter | None = None,
        custom_event_mapper: CustomEventMapper | None = None,
        passthrough_unknown: bool = False,
        raw_unknown: bool = False,
        state_provider: StateProvider | None = None,
        state_refresh_tools: Collection[str] = (),
        run_body: RunBody | None = None,
        on_error: ErrorHook | None = None,
        poll_interval: float = 1.0,
        poll_timeout: float | None = 1800.0,
        allowed_choices: Collection[str] = ("once", "deny"),
        approval_timeout: float | None = None,
    ) -> None:
        self.client: RunsClient = client
        self.store: ThreadStore = store or InMemoryThreadStore()
        self.authorize = authorize
        self.tool_filter = tool_filter
        self.custom_event_mapper = custom_event_mapper
        self.passthrough_unknown = passthrough_unknown
        self.raw_unknown = raw_unknown
        self.state_provider = state_provider
        self.state_refresh_tools = frozenset(state_refresh_tools)
        self.run_body = run_body
        self.on_error = on_error
        self.poll_interval = poll_interval
        self.poll_timeout = poll_timeout
        """Seconds to poll a run whose event stream is gone before failing with ``hermes_timeout``."""
        self.allowed_choices = frozenset(allowed_choices) | {"deny"}
        """Approval choices a client may send; add ``session``/``always`` to offer them."""
        self.approval_timeout = approval_timeout
        """Seconds Hermes waits on an approval, announced as the interrupt's ``expiresAt``."""

    async def run(self, input: RunAgentInput, *, request: Any = None) -> AsyncGenerator[BaseEvent, None]:
        """Start a Hermes run for the last user message, or answer approvals on ``resume``.

        Every failure, including a refused or invalid request, ends the stream with ``RUN_ERROR``.
        """
        ctx = HookContext("run", input.thread_id, input.run_id, input, request)
        async with aclosing(self._guard(ctx, self._deferred(ctx, self._prepare_run))) as events:
            async for event in events:
                yield event

    async def connect(self, input: RunAgentInput, *, request: Any = None) -> AsyncGenerator[BaseEvent, None]:
        """Rejoin the thread's in-flight run by replaying its Hermes backlog."""
        ctx = HookContext("connect", input.thread_id, input.run_id, input, request)
        async with aclosing(self._guard(ctx, self._deferred(ctx, self._prepare_connect))) as events:
            async for event in events:
                yield event

    async def open_run(self, input: RunAgentInput, *, request: Any = None) -> AsyncGenerator[BaseEvent, None]:
        """Authorize and validate a run request, then return its event stream.

        Raises :class:`AgentForbiddenError` or :class:`AgentInputError` before anything is
        started or stopped, so an endpoint can answer with HTTP 403/400; any later failure
        ends the stream with ``RUN_ERROR``.
        """
        ctx = HookContext("run", input.thread_id, input.run_id, input, request)
        return await self._open(ctx, self._prepare_run)

    async def open_connect(self, input: RunAgentInput, *, request: Any = None) -> AsyncGenerator[BaseEvent, None]:
        """Like :meth:`open_run` for :meth:`connect`."""
        ctx = HookContext("connect", input.thread_id, input.run_id, input, request)
        return await self._open(ctx, self._prepare_connect)

    async def stop(self, thread_id: str, run_id: str, *, request: Any = None) -> bool:
        """Stop the thread's Hermes run if AG-UI run ``run_id`` started, resumed or rejoined it."""
        ctx = HookContext("stop", thread_id, run_id, None, request)
        try:
            await self._authorize(ctx)
            record = await self.store.get(thread_id)
            if record is None or record.run_id is None or run_id not in record.agui_run_ids:
                return False
            try:
                await self.client.stop_run(record.run_id)
            except HermesAPIError as exc:
                if exc.status not in (404, 409):
                    raise
                logger.debug("Hermes run %s was already over when stopped: %s", record.run_id, exc)
            return True
        except Exception as exc:
            self._report(ctx, exc)
            raise

    async def _open(
        self, ctx: HookContext, prepare: Callable[[HookContext], Awaitable[AsyncGenerator[BaseEvent, None]]]
    ) -> AsyncGenerator[BaseEvent, None]:
        try:
            await self._authorize(ctx)
            source = await prepare(ctx)
        except (AgentForbiddenError, AgentInputError) as exc:
            self._report(ctx, exc)
            raise
        except Exception as exc:
            source = _failing(exc)
        return self._guard(ctx, source)

    async def _deferred(
        self, ctx: HookContext, prepare: Callable[[HookContext], Awaitable[AsyncGenerator[BaseEvent, None]]]
    ) -> AsyncGenerator[BaseEvent, None]:
        await self._authorize(ctx)
        async with aclosing(await prepare(ctx)) as events:
            async for event in events:
                yield event

    async def _authorize(self, ctx: HookContext) -> None:
        if self.authorize is not None and not await self.authorize(ctx):
            raise AgentForbiddenError(f"authorize refused {ctx.action} on thread {ctx.thread_id}")

    async def _prepare_run(self, ctx: HookContext) -> AsyncGenerator[BaseEvent, None]:
        """Everything that can refuse the request, checked before any Hermes run is touched."""
        input = _input(ctx)
        if input.resume:
            answers = self._answers(input.resume)
            record = await self.store.get(input.thread_id)
            if record is None or record.run_id is None:
                raise AgentInputError("No interrupted Hermes run for this thread")
            return self._resume(ctx, answers)
        record = await self.store.get(input.thread_id) or ThreadRecord()
        earlier = record.started.get(input.run_id)
        if earlier is None and record.run_id and input.run_id in record.agui_run_ids:
            earlier = record.run_id
        if earlier is not None:
            # A transport retry rejoins its own Hermes run, even one a newer run replaced.
            return self._stream(ctx, self._translator(ctx, earlier))
        text, history = _split_messages(input.messages)
        if not text:
            raise AgentInputError("No user message to send")
        return self._start(ctx, record, text, history)

    async def _prepare_connect(self, ctx: HookContext) -> AsyncGenerator[BaseEvent, None]:
        return self._connect(ctx)

    async def _start(
        self, ctx: HookContext, record: ThreadRecord, text: str, history: list[dict[str, str]]
    ) -> AsyncGenerator[BaseEvent, None]:
        input = _input(ctx)
        extra = self.run_body(ctx) if self.run_body else None
        if record.run_id:
            await self._stop_quietly(record.run_id)
        new_session = record.session_id is None
        started = await self.client.start_run(
            text,
            session_id=record.session_id,
            conversation_history=history if new_session else None,
            idempotency_key=_idempotency_key(input.thread_id, input.run_id),
            extra=extra,
        )
        run_id = started.run_id
        if await self._claim(input, run_id, stopped=record.run_id, new_session=new_session) and new_session:
            session = (await self.client.get_run(run_id)).get("session_id")
            session_id = session if isinstance(session, str) and session else None
            await self._update(input.thread_id, run_id, lambda r: setattr(r, "session_id", session_id))
        async for event in self._stream(ctx, self._translator(ctx, run_id)):
            yield event

    async def _claim(self, input: RunAgentInput, run_id: str, *, stopped: str | None, new_session: bool) -> bool:
        """Make ``run_id`` the thread's run, keeping retry mappings a concurrent start wrote.

        ``False`` if this AG-UI run already claimed ``run_id`` (an overlapping retry): the
        retry then only follows its run and never displaces a newer one.
        """
        for _ in range(_CAS_ATTEMPTS):
            record = await self.store.get(input.thread_id) or ThreadRecord()
            if record.started.get(input.run_id) == run_id:
                return False
            previous = record.run_id
            latest = list({**record.started, input.run_id: run_id}.items())[-_STARTED_HISTORY:]
            record.run_id, record.cursor, record.started = run_id, -1, dict(latest)
            record.agui_run_ids, record.pending_interrupts, record.answered_interrupts = [input.run_id], [], []
            if new_session:  # the session follows the run that holds the thread
                record.session_id = None
            if await self.store.put_if_run(input.thread_id, record, previous):
                break
        else:
            raise StoreConflictError(f"claiming thread {input.thread_id} for {run_id}")
        if previous and previous not in (run_id, stopped):  # a concurrent start claimed the thread first
            await self._stop_quietly(previous)
        return True

    async def _resume(self, ctx: HookContext, answers: dict[str, str]) -> AsyncGenerator[BaseEvent, None]:
        input = _input(ctx)
        record = await self.store.get(input.thread_id)
        if record is None or record.run_id is None:
            raise AgentInputError("No interrupted Hermes run for this thread")
        answers = {i: c for i, c in answers.items() if i in record.pending_interrupts}
        for request_id, choice in answers.items():
            try:
                await self.client.respond_approval(record.run_id, request_id=request_id, choice=choice)
            except HermesAPIError as exc:
                if exc.status != 409:
                    raise
                logger.info("Approval %s of Hermes run %s was already answered or expired", request_id, record.run_id)

        def answered(r: ThreadRecord) -> None:
            r.pending_interrupts = [i for i in r.pending_interrupts if i not in answers]
            r.answered_interrupts += [i for i in answers if i not in r.answered_interrupts]

        await self._update(input.thread_id, record.run_id, answered)
        # The live stream never repeats an approval, so one left open must interrupt again.
        unanswered = [i for i in record.pending_interrupts if i not in answers]
        if unanswered and (await self.client.get_run(record.run_id)).get("status") != "waiting_for_approval":
            unanswered = []
        await self._attach(input, record.run_id)
        translator = self._translator(ctx, record.run_id)
        async for event in self._stream(ctx, translator, silent_until=record.cursor, reask=unanswered):
            yield event

    def _answers(self, entries: Sequence[ResumeEntry]) -> dict[str, str]:
        """One Hermes choice per interrupt; rejects choices the host does not allow."""
        answers: dict[str, str] = {}
        for entry in entries:
            choice = _approval_choice(entry)
            if choice not in self.allowed_choices:
                raise AgentInputError(f"Approval choice {choice!r} is not allowed")
            if answers.setdefault(entry.interrupt_id, choice) != choice:
                raise AgentInputError(f"Conflicting answers for interrupt {entry.interrupt_id!r}")
        return answers

    async def _connect(self, ctx: HookContext) -> AsyncGenerator[BaseEvent, None]:
        input = _input(ctx)
        record = await self.store.get(input.thread_id)
        if record is None or record.run_id is None:
            yield run_started(input.thread_id, input.run_id)
            yield RunFinishedEvent(
                thread_id=input.thread_id, run_id=input.run_id, outcome=RunFinishedSuccessOutcome()
            )
            return
        await self._attach(input, record.run_id)
        async for event in self._stream(ctx, self._translator(ctx, record.run_id)):
            yield event

    def _translator(self, ctx: HookContext, hermes_run_id: str) -> HermesTranslator:
        return HermesTranslator(
            hermes_run_id,
            tool_filter=partial(self.tool_filter, ctx) if self.tool_filter else None,
            custom_event_mapper=partial(self.custom_event_mapper, ctx) if self.custom_event_mapper else None,
            passthrough_unknown=self.passthrough_unknown,
            raw_unknown=self.raw_unknown,
            approval_choices=self.allowed_choices,
            approval_timeout=self.approval_timeout,
        )

    async def _stream(
        self,
        ctx: HookContext,
        t: HermesTranslator,
        *,
        silent_until: int = -1,
        reask: Collection[str] = (),
    ) -> AsyncGenerator[BaseEvent, None]:
        """Feed the run's events from seq 0; those up to ``silent_until`` only rebuild state.

        Replayed approvals in ``reask`` end the run again once the replay reaches ``silent_until``.
        """
        input = _input(ctx)
        run_id = t.hermes_run_id
        opened = False
        held: list[Mapping[str, Any]] = []
        try:
            async with aclosing(self.client.events(run_id)) as stream:
                async for event in stream:
                    seq = event.get("seq")
                    silent = isinstance(seq, int) and seq <= silent_until
                    if held and not silent:
                        break
                    if event.get("event") == "approval.request" and (
                        silent or not await self._approval_pending(run_id, event)
                    ):
                        t.skip(event)
                        if silent and event.get("request_id") in reask:
                            held.append(event)
                            if seq == silent_until:
                                break
                        continue
                    if silent:
                        t.feed(event)
                        continue
                    if not opened:
                        opened = True
                        for opening in await self._open_events(ctx, t, replay=silent_until < 0):
                            yield opening
                    items = await self._fed(input.thread_id, t, event)
                    for item in items:
                        yield item
                    if t.suspended:
                        return
                    if t.completed_tool in self.state_refresh_tools:
                        for state in await self._state(ctx):
                            yield state
                    if t.done:
                        break
        except RunStreamGoneError as exc:
            logger.info("Event stream of Hermes run %s is gone (%s); polling its status", run_id, exc)
        if not opened:
            for opening in await self._open_events(ctx, t, replay=silent_until < 0):
                yield opening
        if held:
            for item in await self._interrupted(input.thread_id, t, t.reinterrupt(held), silent_until):
                yield item
            if t.suspended:
                return
        deadline = None if self.poll_timeout is None else asyncio.get_running_loop().time() + self.poll_timeout
        while not t.done:
            try:
                status = await self.client.get_run(run_id)
            except HermesAPIError as exc:
                if exc.status == 404:  # Hermes forgot the run; release the thread
                    await self._release(input.thread_id, run_id)
                raise
            approval = status.get("approval")
            if status.get("status") == "waiting_for_approval" and isinstance(approval, dict):
                for item in await self._fed(input.thread_id, t, approval):
                    yield item
                if t.suspended:
                    return
            for item in t.finish(status):
                yield item
            if not t.done:
                if deadline is not None and asyncio.get_running_loop().time() >= deadline:
                    raise RunTimeoutError(f"Hermes run {run_id} still {status.get('status')!r} after {self.poll_timeout}s")
                await asyncio.sleep(self.poll_interval)
        await self._release(input.thread_id, run_id)

    async def _fed(self, thread_id: str, t: HermesTranslator, event: Mapping[str, Any]) -> list[BaseEvent]:
        """Translate ``event``, persisting an interrupt before the client sees it and leaves.

        An interrupt another client has already moved past (a newer approval is stored, or
        every request in it was answered) is withdrawn and the stream continues.
        """
        seq = event.get("seq")
        return await self._interrupted(thread_id, t, t.feed(event), seq if isinstance(seq, int) else t.last_seq)

    async def _interrupted(self, thread_id: str, t: HermesTranslator, items: list[BaseEvent], cursor: int) -> list[BaseEvent]:
        if not t.suspended or await self._suspend(thread_id, t, cursor):
            return items
        logger.info("Dropped a stale approval interrupt of Hermes run %s", t.hermes_run_id)
        t.pending_interrupts.clear()
        t.suspended = False
        return [i for i in items if not isinstance(i, (RunFinishedEvent, SubagentFinishedEvent))]

    async def _update(
        self, thread_id: str, run_id: str, change: Callable[[ThreadRecord], bool | None]
    ) -> bool:
        """Apply ``change`` to the thread's record while it still owns ``run_id``.

        ``change`` returning ``False`` skips the write; the result says whether it was written.
        """
        for _ in range(_CAS_ATTEMPTS):
            record = await self.store.get(thread_id)
            if record is None or record.run_id != run_id or change(record) is False:
                return False
            if await self.store.put_if_run(thread_id, record, run_id):
                return True
        raise StoreConflictError(f"updating thread {thread_id} for {run_id}")

    async def _attach(self, input: RunAgentInput, run_id: str) -> None:
        def attach(record: ThreadRecord) -> None:
            if input.run_id not in record.agui_run_ids:
                record.agui_run_ids.append(input.run_id)

        await self._update(input.thread_id, run_id, attach)

    async def _suspend(self, thread_id: str, t: HermesTranslator, cursor: int) -> bool:
        """Persist ``t``'s interrupt; ``False`` if it is stale and must not reach the client."""
        current = True

        def suspend(record: ThreadRecord) -> bool:
            nonlocal current
            pending = [i for i in t.pending_interrupts if i not in record.answered_interrupts]
            if cursor < record.cursor:  # another client persisted a newer approval meanwhile
                current = any(i in record.pending_interrupts for i in pending)
                return False
            current = bool(pending)
            record.cursor, record.pending_interrupts = cursor, pending
            return current

        await self._update(thread_id, t.hermes_run_id, suspend)
        return current

    async def _release(self, thread_id: str, run_id: str) -> None:
        def release(record: ThreadRecord) -> None:
            record.run_id, record.cursor = None, -1
            record.agui_run_ids, record.pending_interrupts, record.answered_interrupts = [], [], []

        await self._update(thread_id, run_id, release)

    async def _open_events(self, ctx: HookContext, t: HermesTranslator, *, replay: bool) -> list[BaseEvent]:
        input = _input(ctx)
        return t.start(input.thread_id, input.run_id, input.messages, replay=replay) + await self._state(ctx)

    async def _state(self, ctx: HookContext) -> list[BaseEvent]:
        if self.state_provider is None:
            return []
        state = await self.state_provider(ctx)
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
        except HermesError as exc:
            logger.warning("Could not stop replaced Hermes run %s: %s", run_id, exc)

    async def _guard(self, ctx: HookContext, events: AsyncGenerator[BaseEvent, None]) -> AsyncGenerator[BaseEvent, None]:
        """End the stream with ``RUN_ERROR`` on any failure, unless the run already ended."""
        started = ended = False
        try:
            async with aclosing(events):
                async for event in events:
                    started = started or isinstance(event, RunStartedEvent)
                    ended = ended or isinstance(event, (RunFinishedEvent, RunErrorEvent))
                    yield event
        except Exception as exc:
            self._report(ctx, exc)
            if ended:
                return
            if not started:
                yield run_started(ctx.thread_id, ctx.run_id)
            if isinstance(exc, HermesError):
                yield RunErrorEvent(message=exc.public_message, code=exc.code)
            else:
                yield RunErrorEvent(message="Internal error.", code="internal_error")

    def _report(self, ctx: HookContext, exc: Exception) -> None:
        code = exc.code if isinstance(exc, HermesError) else "internal_error"
        if isinstance(exc, (AgentInputError, AgentForbiddenError)):
            logger.info("%s refused (%s) on thread %s: %s", ctx.action, code, ctx.thread_id, exc)
        else:
            level = logging.WARNING if isinstance(exc, HermesError) else logging.ERROR
            logger.log(level, "%s failed (%s) on thread %s run %s", ctx.action, code, ctx.thread_id, ctx.run_id, exc_info=exc)
        if self.on_error is not None:
            try:
                self.on_error(ctx, exc)
            except Exception:
                logger.exception("on_error hook failed")


def _input(ctx: HookContext) -> RunAgentInput:
    assert ctx.input is not None
    return ctx.input


async def _failing(exc: Exception) -> AsyncGenerator[BaseEvent, None]:
    raise exc
    yield  # pragma: no cover


def _approval_choice(entry: ResumeEntry) -> str:
    """Map a resume entry to the Hermes approval choice it asks for.

    Accepts ``{"approved": bool}`` (assistant-ui tool approvals), ``{"choice": str}``
    or a bare choice string. A cancelled entry or ``approved: false`` always denies.
    """
    payload = entry.payload
    if entry.status == "cancelled":
        return "deny"
    if isinstance(payload, str):
        return payload
    if isinstance(payload, Mapping):
        if payload.get("approved") is False:
            return "deny"
        if isinstance(payload.get("choice"), str):
            return str(payload["choice"])
        if payload.get("approved") is True:
            return "once"
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
