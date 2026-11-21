"""Runs AG-UI requests against Hermes ``/v1/runs``."""

from __future__ import annotations

import asyncio
import copy
import inspect
import logging
from collections.abc import AsyncGenerator, Awaitable, Callable, Collection, Coroutine, Mapping, Sequence
from contextlib import aclosing
from functools import partial
from typing import TYPE_CHECKING, Any, Literal, TypeVar, cast

from ag_ui.core import (
    AgentCapabilities,
    BaseEvent,
    HumanInTheLoopCapabilities,
    IdentityCapabilities,
    Message,
    MessagesSnapshotEvent,
    MultiAgentCapabilities,
    MultimodalCapabilities,
    MultimodalInputCapabilities,
    ReasoningCapabilities,
    RunAgentInput,
    RunErrorEvent,
    RunFinishedEvent,
    RunFinishedSuccessOutcome,
    RunStartedEvent,
    StateCapabilities,
    StateDeltaEvent,
    StateSnapshotEvent,
    SubagentFinishedEvent,
    ToolsCapabilities,
    TransportCapabilities,
)

from . import _inputs
from ._config import (
    ApprovalPolicy,
    Authorize,
    CustomEventMapper,
    HookContext,
    InputPolicy,
    OnError,
    RequestLike,
    RunBody,
    StateProvider,
    ToolFilter,
)
from ._patch import json_diff
from .errors import AgentForbiddenError, AgentInputError, HermesAPIError, HermesError, RunStreamGoneError, RunTimeoutError, StoreConflictError
from .store import InMemoryThreadStore, ThreadRecord, ThreadStore
from .translator import TERMINAL_STATUSES, HermesTranslator, UnknownEvents, handles, run_started

if TYPE_CHECKING:
    from .client import RunsClient

logger = logging.getLogger("ag_ui_hermes")

_STARTED_HISTORY = 32
"""Started AG-UI runs per thread whose late retries still find their Hermes run."""
_CAS_ATTEMPTS = 50
"""Compare-and-set retries on one thread record before giving up."""
_T = TypeVar("_T")
_StatusFetch = Callable[[], Awaitable[dict[str, Any]]]


class HermesAgent:
    """AG-UI agent backed by a Hermes API server.

    A client disconnect only closes the event stream; the Hermes run keeps going and
    :meth:`connect` can rejoin it. Only :meth:`stop` (or a new run on the same thread)
    stops it. Every hook may be sync or async.
    """

    def __init__(
        self,
        client: RunsClient,
        *,
        store: ThreadStore | None = None,
        authorize: Authorize | None = None,
        tool_filter: ToolFilter | None = None,
        custom_event_mapper: CustomEventMapper | None = None,
        unknown_events: UnknownEvents = "drop",
        state_provider: StateProvider | None = None,
        state_refresh_tools: Collection[str] = (),
        run_body: RunBody | None = None,
        on_error: OnError | None = None,
        poll_interval: float = 1.0,
        poll_timeout: float | None = 1800.0,
        inputs: InputPolicy = InputPolicy(),  # noqa: B008  # frozen, so sharing the default is safe
        approvals: ApprovalPolicy = ApprovalPolicy(),  # noqa: B008
    ) -> None:
        self.client: RunsClient = client
        self.store: ThreadStore = store or InMemoryThreadStore()
        self.authorize = authorize
        self.tool_filter = tool_filter
        self.custom_event_mapper = custom_event_mapper
        self.unknown_events: UnknownEvents = unknown_events
        """What happens to Hermes events nothing maps: ``drop``, or forward as ``custom`` or ``raw``."""
        self.state_provider = state_provider
        self.state_refresh_tools = frozenset(state_refresh_tools)
        self.run_body = run_body
        self.on_error = on_error
        self.poll_interval = poll_interval
        self.poll_timeout = poll_timeout
        """Seconds to poll a run whose event stream is gone before failing with ``hermes_timeout``."""
        self.inputs = inputs
        self.approvals = approvals
        self._tasks: set[asyncio.Task[Any]] = set()

    async def run(self, input: RunAgentInput, *, request: RequestLike | None = None) -> AsyncGenerator[BaseEvent, None]:
        """Start a Hermes run for the last user message, or answer approvals on ``resume``.

        Every failure, including a refused or invalid request, ends the stream with ``RUN_ERROR``.
        """
        ctx = _context("run", input, request)
        async with aclosing(self._guard(ctx, self._deferred(ctx, self._prepare_run))) as events:
            async for event in events:
                yield event

    async def connect(self, input: RunAgentInput, *, request: RequestLike | None = None) -> AsyncGenerator[BaseEvent, None]:
        """Rejoin the thread's in-flight run by replaying its Hermes backlog."""
        ctx = _context("connect", input, request)
        async with aclosing(self._guard(ctx, self._deferred(ctx, self._prepare_connect))) as events:
            async for event in events:
                yield event

    async def open_run(self, input: RunAgentInput, *, request: RequestLike | None = None) -> AsyncGenerator[BaseEvent, None]:
        """Authorize and validate a run request, then return its event stream.

        Raises :class:`AgentForbiddenError` or :class:`AgentInputError` before anything is
        started or stopped, so an endpoint can answer with HTTP 403/400; any later failure
        ends the stream with ``RUN_ERROR``.
        """
        return await self._open(_context("run", input, request), self._prepare_run)

    async def open_connect(self, input: RunAgentInput, *, request: RequestLike | None = None) -> AsyncGenerator[BaseEvent, None]:
        """Like :meth:`open_run` for :meth:`connect`."""
        return await self._open(_context("connect", input, request), self._prepare_connect)

    async def stop(self, thread_id: str, run_id: str, *, request: RequestLike | None = None) -> bool:
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
            await self._report(ctx, exc)
            raise

    async def capabilities(self, *, request: RequestLike | None = None) -> AgentCapabilities:
        """AG-UI capabilities of this agent, with what Hermes ``/v1/capabilities`` reports."""
        ctx = HookContext("capabilities", None, None, None, request)
        try:
            await self._authorize(ctx)
            # getattr, unlike a runtime Protocol check, also sees methods a client supplies via __getattr__.
            get_capabilities = getattr(self.client, "get_capabilities", None)
            hermes = await get_capabilities() if callable(get_capabilities) else {}
        except Exception as exc:
            await self._report(ctx, exc)
            raise
        features = hermes.get("features")
        features = features if isinstance(features, dict) else {}
        model = hermes.get("model")
        state = self.state_provider is not None
        return AgentCapabilities(
            identity=IdentityCapabilities(name="Hermes Agent", type="hermes", provider="Nous Research"),
            transport=TransportCapabilities(streaming=True),
            tools=ToolsCapabilities(supported=True, client_provided=False),
            state=StateCapabilities(snapshots=state, deltas=state),
            multi_agent=MultiAgentCapabilities(supported=True, delegation=True),
            reasoning=ReasoningCapabilities(supported=features.get("reasoning_streaming") is not False, streaming=False),
            multimodal=MultimodalCapabilities(input=MultimodalInputCapabilities(image=True, audio=False, video=False, pdf=False, file=False)),
            human_in_the_loop=HumanInTheLoopCapabilities(supported=True, approvals=features.get("approval_events") is not False, interrupts=True),
            custom={"hermes": {"model": model}} if isinstance(model, str) and model else None,
        )

    async def _open(self, ctx: HookContext, prepare: Callable[[HookContext], Awaitable[AsyncGenerator[BaseEvent, None]]]) -> AsyncGenerator[BaseEvent, None]:
        try:
            await self._authorize(ctx)
            source = await prepare(ctx)
        except (AgentForbiddenError, AgentInputError) as exc:
            await self._report(ctx, exc)
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
        if self.authorize is not None and not await _resolve(self.authorize(ctx)):
            raise AgentForbiddenError(f"authorize refused {ctx.action} on thread {ctx.thread_id!r}")

    async def _prepare_run(self, ctx: HookContext) -> AsyncGenerator[BaseEvent, None]:
        """Everything that can refuse the request, checked before any Hermes run is touched."""
        input = _input(ctx)
        if input.resume:
            answers = _inputs.answers(input.resume, self.approvals.allowed_choices)
            record = await self.store.get(input.thread_id)
            if record is None or record.run_id is None:
                raise AgentInputError("No interrupted Hermes run for this thread", "unknown_interrupt")
            # An id another client already answered is a late repeat, not an error.
            if not set(answers) <= {*record.pending_interrupts, *record.answered_interrupts}:
                raise AgentInputError("The resume answers an interrupt this thread is not waiting on", "unknown_interrupt")
            return self._resume(ctx, answers, record.run_id)
        record = await self.store.get(input.thread_id) or ThreadRecord()
        earlier = record.started.get(input.run_id)
        if earlier is None and record.run_id and input.run_id in record.agui_run_ids:
            earlier = record.run_id
        if earlier is not None:
            # A transport retry rejoins its own Hermes run, even one a newer run replaced.
            return self._stream(ctx, earlier)
        content, history = _inputs.split_messages(input.messages, self.inputs)
        if not content:
            raise AgentInputError("No user message to send", "no_user_message")
        return self._start(ctx, record, content, history, _inputs.forwarded(input.forwarded_props, self.inputs))

    async def _prepare_connect(self, ctx: HookContext) -> AsyncGenerator[BaseEvent, None]:
        return self._connect(ctx)

    async def _start(
        self,
        ctx: HookContext,
        record: ThreadRecord,
        content: str | list[dict[str, Any]],
        history: list[dict[str, str]],
        forwarded: dict[str, Any],
    ) -> AsyncGenerator[BaseEvent, None]:
        input = _input(ctx)
        extra = {**forwarded, **(await _resolve(self.run_body(ctx)) if self.run_body else {})}
        if instructions := _inputs.instructions(input, extra.get("instructions"), self.inputs):
            extra["instructions"] = instructions
        # A run Hermes accepted is recorded or stopped even if this request is cancelled.
        run_id = await self._shielded(self._launch(input, record, content, history, extra), f"Starting a Hermes run on thread {input.thread_id}")
        async with aclosing(self._stream(ctx, run_id)) as events:
            async for event in events:
                yield event

    async def _launch(
        self,
        input: RunAgentInput,
        record: ThreadRecord,
        content: str | list[dict[str, Any]],
        history: list[dict[str, str]],
        extra: dict[str, Any],
    ) -> str:
        if record.run_id:
            await self._stop_quietly(record.run_id)
        new_session = record.session_id is None
        started = await self.client.start_run(
            content,
            session_id=record.session_id,
            conversation_history=history if new_session else None,
            idempotency_key=_inputs.idempotency_key(input.thread_id, input.run_id),
            extra=extra,
        )
        run_id = started.run_id
        try:
            claimed = await self._claim(input, run_id, stopped=record.run_id, new_session=new_session)
        except Exception:
            await self._abandon(input.thread_id, run_id)
            raise
        if claimed and new_session:
            session = (await self.client.get_run(run_id)).get("session_id")
            session_id = session if isinstance(session, str) and session else None
            await self._update(input.thread_id, run_id, lambda r: setattr(r, "session_id", session_id))
        return run_id

    async def _abandon(self, thread_id: str, run_id: str) -> None:
        """Stop a started run the thread could not record, unless it holds the run after all."""
        try:
            record = await self.store.get(thread_id)
        except Exception:
            record = None
        if record is None or record.run_id != run_id:
            await self._stop_quietly(run_id)

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
            if await self.store.compare_and_set(input.thread_id, record, previous):
                break
        else:
            raise StoreConflictError(f"claiming thread {input.thread_id} for {run_id}")
        if previous and previous not in (run_id, stopped):  # a concurrent start claimed the thread first
            await self._stop_quietly(previous)
        return True

    async def _resume(self, ctx: HookContext, answers: dict[str, str], validated: str) -> AsyncGenerator[BaseEvent, None]:
        input = _input(ctx)
        record = await self.store.get(input.thread_id)
        if record is None or record.run_id != validated:
            raise AgentInputError("The thread moved on to another Hermes run since the resume was validated", "stale_resume")
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
        await self._attach(input, record.run_id)
        async with aclosing(self._stream(ctx, record.run_id, silent_until=record.cursor, reask=unanswered)) as events:
            async for event in events:
                yield event

    async def _connect(self, ctx: HookContext) -> AsyncGenerator[BaseEvent, None]:
        input = _input(ctx)
        record = await self.store.get(input.thread_id)
        if record is None or record.run_id is None:
            yield run_started(input.thread_id, input.run_id)
            # A client that holds no messages gets the session's stored conversation.
            history = await self._history(ctx, record.session_id) if record and record.session_id and not input.messages else []
            if history:
                yield MessagesSnapshotEvent(messages=history)
            yield RunFinishedEvent(thread_id=input.thread_id, run_id=input.run_id, outcome=RunFinishedSuccessOutcome())
            return
        await self._attach(input, record.run_id)
        async with aclosing(self._stream(ctx, record.run_id)) as events:
            async for event in events:
                yield event

    async def _history(self, ctx: HookContext, session_id: str) -> list[Message]:
        """The session's stored conversation, or nothing if the client cannot provide it."""
        session_messages = getattr(self.client, "session_messages", None)
        if not callable(session_messages):
            return []
        try:
            rows = await session_messages(session_id)
        except HermesError as exc:
            logger.warning("Could not load the history of Hermes session %s: %s", session_id, exc)
            return []
        hook = self.tool_filter

        async def view(tool: str, preview: str, event: Mapping[str, Any]) -> tuple[str, str] | None:
            assert hook is not None
            return await _resolve(hook(ctx, tool, preview, event))

        return await _inputs.history_messages(rows, view if hook else None, results=self.inputs.restore_tool_results)

    async def _stream(
        self,
        ctx: HookContext,
        hermes_run_id: str,
        *,
        silent_until: int = -1,
        reask: Collection[str] = (),
    ) -> AsyncGenerator[BaseEvent, None]:
        """Feed the run's events from seq 0; those up to ``silent_until`` only rebuild state.

        Once the replay reaches ``silent_until``, the approval in ``reask`` that Hermes still
        waits on ends the run again. A run that ended releases the thread, even when the
        client left at the end or the last state refresh failed.
        """
        hooks = _TranslatorHooks(ctx, self.tool_filter, self.custom_event_mapper)
        t = HermesTranslator(
            hermes_run_id,
            tool_filter=hooks.view if self.tool_filter else None,
            custom_event_mapper=hooks.mapped if self.custom_event_mapper else None,
            unknown_events=self.unknown_events,
            approval_choices=self.approvals.allowed_choices,
            approval_timeout=self.approvals.timeout,
        )
        thread_id = _input(ctx).thread_id
        try:
            async with aclosing(self._follow(ctx, t, hooks, silent_until, reask)) as events:
                async for event in events:
                    yield event
        finally:
            if t.done:
                try:
                    await self._shielded(self._release(thread_id, hermes_run_id), f"Releasing thread {thread_id}")
                except Exception as exc:
                    await self._report(ctx, exc)

    async def _shielded(self, work: Coroutine[Any, Any, _T], what: str) -> _T:
        """Run ``work`` to its end even if the caller is cancelled; a failure after that is logged."""
        task = asyncio.ensure_future(work)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            task.add_done_callback(partial(_log_orphan, what))
            raise

    async def _follow(
        self, ctx: HookContext, t: HermesTranslator, hooks: _TranslatorHooks, silent_until: int, reask: Collection[str]
    ) -> AsyncGenerator[BaseEvent, None]:
        input = _input(ctx)
        run_id = t.hermes_run_id
        opened = False
        held: list[Mapping[str, Any]] = []
        state: list[Any] = []  # the last state sent in this stream
        try:
            async with aclosing(self.client.events(run_id)) as stream:
                async for event in stream:
                    seq = event.get("seq")
                    # The unsequenced truncation marker emits nothing, so it must not open the
                    # run before a silent replay has restored its open subagents.
                    silent = seq <= silent_until if isinstance(seq, int) else event.get("event") == "replay.truncated"
                    if reask and isinstance(seq, int) and not silent:  # the replay passed the interrupt
                        lost = t.last_seq < 0 and seq > silent_until + 1
                        held, reask = await self._reasked(run_id, held, reask, lost=lost), ()
                        if held:
                            break
                    approval = event.get("event") == "approval.request"
                    if silent or (approval and not await self._approval_pending(run_id, event)):
                        if not approval:
                            await hooks.prepare(t, event)
                            t.feed(event)
                        else:
                            t.skip(event)
                            if silent and event.get("request_id") in reask:
                                held.append(event)
                        if reask and seq == silent_until:
                            held, reask = await self._reasked(run_id, held, reask), ()
                            if held:
                                break
                        continue
                    if not opened:
                        opened = True
                        for opening in await self._open_events(ctx, t, state, replay=silent_until < 0):
                            yield opening
                    items = await self._ending(ctx, t, await self._fed(input.thread_id, t, event, hooks=hooks), state)
                    for item in items:
                        yield item
                    if t.suspended:
                        return
                    if t.completed_tool in self.state_refresh_tools:
                        for item in await self._state(ctx, state):
                            yield item
                    if t.done:
                        break
        except RunStreamGoneError as exc:
            logger.info("Event stream of Hermes run %s is gone (%s); polling its status", run_id, exc)
        if not opened:
            for opening in await self._open_events(ctx, t, state, replay=silent_until < 0):
                yield opening
        if reask and not t.done:
            held = await self._reasked(run_id, held, reask)
        if held:
            for item in await self._interrupted(input.thread_id, t, t.reinterrupt(held), silent_until):
                yield item
            if t.suspended:
                return
        loop = asyncio.get_running_loop()
        deadline = None if self.poll_timeout is None else loop.time() + self.poll_timeout
        poll = partial(self._status, run_id, deadline)
        while not t.done:
            try:
                status = await poll()
            except HermesAPIError as exc:
                if exc.status == 404:  # Hermes forgot the run; release the thread
                    await self._release(input.thread_id, run_id)
                raise
            if (approval := _awaited(status)) is not None:
                for item in await self._fed(input.thread_id, t, approval, recheck=poll):
                    yield item
                if t.suspended:
                    return
            for item in await self._ending(ctx, t, t.finish(status), state):
                yield item
            if not t.done:
                if deadline is not None and loop.time() >= deadline:
                    raise RunTimeoutError(f"Hermes run {run_id} still {status.get('status')!r} after {self.poll_timeout}s")
                await asyncio.sleep(self.poll_interval if deadline is None else min(self.poll_interval, deadline - loop.time()))

    async def _status(self, run_id: str, deadline: float | None) -> dict[str, Any]:
        """``GET /v1/runs/{id}``, failing with ``hermes_timeout`` once ``deadline`` passes."""
        try:
            async with asyncio.timeout_at(deadline) as bound:
                return await self.client.get_run(run_id)
        except TimeoutError as exc:
            if not bound.expired():
                raise
            raise RunTimeoutError(f"Hermes run {run_id} status did not answer within {self.poll_timeout}s") from exc

    async def _fed(
        self,
        thread_id: str,
        t: HermesTranslator,
        event: Mapping[str, Any],
        *,
        hooks: _TranslatorHooks | None = None,
        recheck: _StatusFetch | None = None,
    ) -> list[BaseEvent]:
        """Translate ``event``, persisting an interrupt before the client sees it and leaves.

        An interrupt another client has already moved past (a newer approval is stored, or
        every request in it was answered) is withdrawn and the stream continues. An approval
        polled from the run status has no seq; ``recheck`` fetches that status again.
        """
        if hooks is not None:
            await hooks.prepare(t, event)
        seq = event.get("seq")
        cursor = seq if isinstance(seq, int) else t.last_seq
        return await self._interrupted(thread_id, t, t.feed(event), cursor, recheck=recheck)

    async def _interrupted(
        self, thread_id: str, t: HermesTranslator, items: list[BaseEvent], cursor: int, *, recheck: _StatusFetch | None = None
    ) -> list[BaseEvent]:
        if not t.suspended or await self._suspend(thread_id, t, cursor, recheck=recheck):
            return items
        logger.info("Dropped a stale approval interrupt of Hermes run %s", t.hermes_run_id)
        t.withdraw()
        return [i for i in items if not isinstance(i, (RunFinishedEvent, SubagentFinishedEvent))]

    async def _update(self, thread_id: str, run_id: str, change: Callable[[ThreadRecord], bool | None]) -> bool:
        """Apply ``change`` to the thread's record while it still owns ``run_id``.

        ``change`` returning ``False`` skips the write; the result says whether it was written.
        """
        for _ in range(_CAS_ATTEMPTS):
            record = await self.store.get(thread_id)
            if record is None or record.run_id != run_id or change(record) is False:
                return False
            if await self.store.compare_and_set(thread_id, record, run_id):
                return True
        raise StoreConflictError(f"updating thread {thread_id} for {run_id}")

    async def _attach(self, input: RunAgentInput, run_id: str) -> None:
        def attach(record: ThreadRecord) -> None:
            if input.run_id not in record.agui_run_ids:
                record.agui_run_ids.append(input.run_id)

        await self._update(input.thread_id, run_id, attach)

    async def _suspend(self, thread_id: str, t: HermesTranslator, cursor: int, *, recheck: _StatusFetch | None = None) -> bool:
        """Persist ``t``'s interrupt; ``False`` if it is stale and must not reach the client.

        Stale means answered, older than an approval another client persisted, or of a run
        the thread no longer owns. A polled approval (``recheck`` given) has no seq to order it
        by, so when another approval is stored the run status decides which one Hermes still
        waits on, and the write only lands on the record that check was made against.
        """
        run_id = t.hermes_run_id
        for _ in range(_CAS_ATTEMPTS):
            record = await self.store.get(thread_id)
            if record is None or record.run_id != run_id:
                return False
            pending = [i for i in t.pending_interrupts if i not in record.answered_interrupts]
            if not pending:
                return False
            if recheck is None and cursor < record.cursor:  # another client persisted a newer approval meanwhile
                return any(i in record.pending_interrupts for i in pending)
            if recheck is not None and any(i not in pending for i in record.pending_interrupts):
                approval = _awaited(await recheck())
                if approval is None or approval.get("request_id") not in pending:
                    return False
            record.cursor, record.pending_interrupts = max(cursor, record.cursor), pending
            if await self.store.compare_and_set(thread_id, record, run_id):
                return True
        raise StoreConflictError(f"suspending thread {thread_id} for {run_id}")

    async def _release(self, thread_id: str, run_id: str) -> None:
        def release(record: ThreadRecord) -> None:
            record.run_id, record.cursor = None, -1
            record.agui_run_ids, record.pending_interrupts, record.answered_interrupts = [], [], []

        await self._update(thread_id, run_id, release)

    async def _open_events(self, ctx: HookContext, t: HermesTranslator, sent: list[Any], *, replay: bool) -> list[BaseEvent]:
        input = _input(ctx)
        return t.start(input.thread_id, input.run_id, input.messages, replay=replay) + await self._state(ctx, sent)

    async def _ending(self, ctx: HookContext, t: HermesTranslator, items: list[BaseEvent], sent: list[Any]) -> list[BaseEvent]:
        """``items`` with a last state refresh before the run's terminal event, if it has one."""
        end = next((i for i, e in enumerate(items) if isinstance(e, (RunFinishedEvent, RunErrorEvent))), None)
        if not t.done or end is None:
            return items
        return items[:end] + await self._state(ctx, sent) + items[end:]

    async def _state(self, ctx: HookContext, sent: list[Any]) -> list[BaseEvent]:
        """``STATE_SNAPSHOT`` first in a stream, then ``STATE_DELTA`` against what ``sent`` holds."""
        if self.state_provider is None:
            return []
        state = copy.deepcopy(await _resolve(self.state_provider(ctx)))
        if state is None:
            return []
        if sent and isinstance(sent[0], dict) and isinstance(state, dict):
            ops = json_diff(sent[0], state)
            sent[0] = state
            return [StateDeltaEvent.model_validate({"delta": ops})] if ops else []
        sent[:] = [state]
        return [StateSnapshotEvent(snapshot=state)]

    async def _reasked(self, run_id: str, held: list[Mapping[str, Any]], reask: Collection[str], *, lost: bool = False) -> list[Mapping[str, Any]]:
        """The approval Hermes still waits on, if the live stream will not repeat it.

        That is one of ``reask`` (its replayed event, or the status's if the backlog lost it),
        or any approval when ``lost`` events after the interrupt could have held its request.
        """
        approval = _awaited(await self.client.get_run(run_id))
        if approval is None:
            return []
        if approval.get("request_id") not in reask:
            return [approval] if lost else []
        return [e for e in held if e.get("request_id") == approval["request_id"]] or [approval]

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
            await self._report(ctx, exc)
            if ended:
                return
            if not started:
                input = _input(ctx)
                yield run_started(input.thread_id, input.run_id)
            if isinstance(exc, HermesError):
                yield RunErrorEvent(message=exc.public_message, code=exc.code)
            else:
                yield RunErrorEvent(message="Internal error.", code="internal_error")

    async def _report(self, ctx: HookContext, exc: Exception) -> None:
        code = exc.code if isinstance(exc, HermesError) else "internal_error"
        if isinstance(exc, AgentInputError):
            logger.info("%s refused (%s: %s) on thread %s: %s", ctx.action, code, exc.reason, ctx.thread_id, exc)
        elif isinstance(exc, AgentForbiddenError):
            logger.info("%s refused (%s) on thread %s: %s", ctx.action, code, ctx.thread_id, exc)
        else:
            level = logging.WARNING if isinstance(exc, HermesError) else logging.ERROR
            logger.log(level, "%s failed (%s) on thread %s run %s", ctx.action, code, ctx.thread_id, ctx.run_id, exc_info=exc)
        if self.on_error is not None:
            try:
                await _resolve(self.on_error(ctx, exc))
            except Exception:
                logger.exception("on_error hook failed")


class _TranslatorHooks:
    """Resolves ``tool_filter`` and ``custom_event_mapper`` (sync or async) before the
    translator, which is synchronous, asks for their answer on the event being fed."""

    def __init__(self, ctx: HookContext, tool_filter: ToolFilter | None, mapper: CustomEventMapper | None) -> None:
        self._ctx, self._filter, self._mapper = ctx, tool_filter, mapper
        self._view: tuple[str, str] | None = None
        self._mapped: Sequence[BaseEvent] | None = None

    async def prepare(self, t: HermesTranslator, event: Mapping[str, Any]) -> None:
        self._view = self._mapped = None  # a missed answer hides the tool rather than leaking it
        seq = event.get("seq")
        if t.done or (isinstance(seq, int) and seq <= t.last_seq):
            return  # the translator skips the event
        name = event.get("event")
        if self._filter is not None and name in ("tool.started", "tool.completed"):
            tool, preview = str(event.get("tool") or "tool"), str(event.get("preview") or "")
            self._view = await _resolve(self._filter(self._ctx, tool, preview, event))
        elif self._mapper is not None and not handles(event):
            self._mapped = await _resolve(self._mapper(self._ctx, event))

    def view(self, tool: str, preview: str, event: Mapping[str, Any]) -> tuple[str, str] | None:
        return self._view

    def mapped(self, event: Mapping[str, Any]) -> Sequence[BaseEvent] | None:
        return self._mapped


async def _resolve(value: _T | Awaitable[_T]) -> _T:
    """A hook's result, awaited if the hook was async."""
    if inspect.isawaitable(value):
        return cast("_T", await value)
    return value


def _context(action: Literal["run", "connect"], input: RunAgentInput, request: RequestLike | None) -> HookContext:
    """A context holding a private copy of ``input``, so what is authorized is what runs."""
    pinned = input.model_copy(deep=True)
    return HookContext(action, pinned.thread_id, pinned.run_id, pinned, request)


def _input(ctx: HookContext) -> RunAgentInput:
    assert ctx.input is not None
    return ctx.input


def _log_orphan(what: str, task: asyncio.Task[Any]) -> None:
    if not task.cancelled() and (exc := task.exception()) is not None:
        logger.error("%s failed after its request was cancelled", what, exc_info=exc)


def _awaited(status: Mapping[str, Any]) -> dict[str, Any] | None:
    """The approval a run status says Hermes waits on."""
    approval = status.get("approval")
    return approval if status.get("status") == "waiting_for_approval" and isinstance(approval, dict) else None


async def _failing(exc: Exception) -> AsyncGenerator[BaseEvent, None]:
    raise exc
    yield  # pragma: no cover
