"""Pure translation of Hermes ``/v1/runs`` events into AG-UI events (no I/O)."""

from __future__ import annotations

import sys
import time
from collections.abc import Callable, Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal

from ag_ui.core import (
    PROTOCOL_VERSION,
    AssistantMessage,
    BaseEvent,
    CustomEvent,
    FunctionCall,
    Interrupt,
    Message,
    MessagesSnapshotEvent,
    RawEvent,
    ReasoningEndEvent,
    ReasoningMessage,
    ReasoningMessageContentEvent,
    ReasoningMessageEndEvent,
    ReasoningMessageStartEvent,
    ReasoningStartEvent,
    RunErrorEvent,
    RunFinishedCancelledOutcome,
    RunFinishedEvent,
    RunFinishedInterruptOutcome,
    RunFinishedSuccessOutcome,
    RunStartedEvent,
    SubagentErrorEvent,
    SubagentFinishedEvent,
    SubagentFinishedSuccessOutcome,
    SubagentFinishedSuspendedOutcome,
    SubagentStartedEvent,
    TextMessageContentEvent,
    TextMessageEndEvent,
    TextMessageStartEvent,
    TokenUsage,
    ToolCall,
    ToolCallEndEvent,
    ToolCallResultEvent,
    ToolCallStartEvent,
    ToolMessage,
)

_ToolView = Callable[[str, str, Mapping[str, Any]], "tuple[str, str] | None"]
_EventMapper = Callable[[Mapping[str, Any]], "Sequence[BaseEvent] | None"]
UnknownEvents = Literal["drop", "custom", "raw"]
"""What happens to a Hermes event nothing maps: dropped, ``CUSTOM`` ``hermes.<event>`` or ``RAW``."""

TERMINAL_STATUSES = frozenset({"completed", "failed", "cancelled", "interrupted"})
_IGNORED = frozenset({"approval.responded", "run.waiting_for_approval", "run.steered", "run.stopping"})
_HANDLED = frozenset(
    {
        "message.delta",
        "message.interim",
        "tool.started",
        "tool.completed",
        "reasoning.available",
        "subagent.start",
        "subagent.complete",
        "approval.request",
        "replay.truncated",
        *(f"run.{s}" for s in TERMINAL_STATUSES),
        *_IGNORED,
    }
)


def handles(event: Mapping[str, Any]) -> bool:
    """Whether the translator maps ``event`` itself, without ``custom_event_mapper``."""
    return str(event.get("event") or "") in _HANDLED


_SUBAGENT_KEYS = (
    "goal",
    "task_count",
    "task_index",
    "subagent_id",
    "child_session_id",
    "delegation_id",
    "parent_id",
    "depth",
    "model",
    "tool_count",
    "status",
    "summary",
    "duration_seconds",
    "input_tokens",
    "output_tokens",
    "reasoning_tokens",
    "api_calls",
    "cost_usd",
)
_APPROVAL_KEYS = ("command", "description", "pattern_key", "pattern_keys", "choices", "smart_denied")
_MAX_COUNT = 2**53 - 1


def run_started(thread_id: str, run_id: str) -> RunStartedEvent:
    """``RUN_STARTED`` declaring the AG-UI protocol version this adapter speaks."""
    return RunStartedEvent(thread_id=thread_id, run_id=run_id, protocol_version=PROTOCOL_VERSION)


def token_usage(status: Mapping[str, Any]) -> list[TokenUsage] | None:
    """Hermes ``usage`` and served ``runtime`` of a terminal run as AG-UI token usage.

    Hermes already counts cache reads and writes into ``input_tokens``. It reports a
    counter it lacks as ``0``, so a zero cache count and an all-zero entry are dropped.
    """
    usage, runtime = status.get("usage"), status.get("runtime")
    if not isinstance(usage, Mapping):
        return None

    def count(key: str) -> int | None:
        value = usage.get(key)
        ok = isinstance(value, int) and not isinstance(value, bool) and 0 <= value <= _MAX_COUNT
        return value if ok else None

    entry: dict[str, Any] = {"input_tokens": count("input_tokens"), "output_tokens": count("output_tokens")}
    if not entry["input_tokens"] and not entry["output_tokens"]:
        return None
    if entry["input_tokens"] is not None and entry["output_tokens"] is not None:
        entry["total_tokens"] = min(entry["input_tokens"] + entry["output_tokens"], _MAX_COUNT)
    entry["cached_input_tokens"] = count("cache_read_tokens") or None
    entry["cache_write_input_tokens"] = count("cache_write_tokens") or None
    if isinstance(runtime, Mapping):
        for key in ("provider", "model"):
            if isinstance(runtime.get(key), str) and runtime[key]:
                entry[key] = runtime[key]
    return [TokenUsage(**{k: v for k, v in entry.items() if v is not None})]


@dataclass
class _Segment:
    id: str
    text: str = ""
    open: bool = False
    announced: bool = False
    """``TEXT_MESSAGE_START`` was sent in the current AG-UI run."""
    tool_calls: list[_ToolCall] = field(default_factory=list)


@dataclass
class _ToolCall:
    id: str
    raw_name: str
    name: str
    hidden: bool
    result: str | None = None
    error: bool = False
    source_id: str | None = None
    """The event's own ``tool_call_id``/``id``, which pairs a completion with its start."""

    @property
    def result_id(self) -> str:
        return f"{self.id}:result"


@dataclass
class _Reasoning:
    id: str
    text: str


@dataclass
class _Subagent:
    id: str
    description: str | None
    parent_tool_call_id: str | None
    metadata: dict[str, Any]


class HermesTranslator:
    """Turns one Hermes run's event stream into AG-UI events.

    Provisional: :class:`HermesAgent` is the stable entry point, and this class's API may
    change in a minor release.

    Ids derive from the Hermes run id and each event's ``seq``, so feeding the same
    events again (a replay from seq 0) yields identical ids. Messages passed to
    :meth:`start` are what the client already holds. With ``replay=True`` and a backlog
    from seq 0, a ``MESSAGES_SNAPSHOT`` first drops the client's copy of this run and
    the replay rebuilds it; otherwise what the client holds is skipped and
    ``run.completed`` reconciles it. Call :meth:`start` once per AG-UI run, then
    :meth:`feed` every Hermes event; :meth:`finish` takes a ``GET /v1/runs/{id}``
    status when the stream ended without a terminal event.

    ``tool_filter(tool, preview, event)`` returns ``(tool, preview)`` or ``None``: on
    ``tool.started`` ``None`` hides the whole call, on ``tool.completed`` only the result.
    ``custom_event_mapper(event)`` maps an event the translator does not know, ``None``
    falling through to ``unknown_events``.
    """

    def __init__(
        self,
        hermes_run_id: str,
        *,
        tool_filter: _ToolView | None = None,
        custom_event_mapper: _EventMapper | None = None,
        unknown_events: UnknownEvents = "drop",
        approval_choices: Collection[str] | None = None,
        approval_timeout: float | None = None,
    ) -> None:
        self._hermes_run_id = hermes_run_id
        self._tool_filter = tool_filter
        self._custom_event_mapper = custom_event_mapper
        self._unknown_events = unknown_events
        self._approval_choices = None if approval_choices is None else frozenset(approval_choices)
        """Choices an interrupt's ``responseSchema`` offers, of those Hermes allows; ``None`` offers all."""
        self._approval_timeout = approval_timeout
        """Seconds Hermes waits on an approval, for ``expiresAt``; ``None`` sets none."""
        self._thread_id = ""
        self._run_id = ""
        self._last_seq = -1
        self._done = False
        self._suspended = False
        self._truncated = False
        self._head_unknown = False
        """The backlog lost the start of the current text segment, so its id is unknown."""
        self._pending_interrupts: dict[str, Interrupt] = {}
        self._completed_tool: str | None = None
        self._prior: list[Message] = []
        self._known_text: dict[str, str] = {}
        self._known_calls: set[str] = set()
        self._known_results: dict[str, str] = {}
        self._backlog_start = sys.maxsize
        """First seq of a truncated backlog; calls with a lower seq had their start evicted."""
        self._reset_pending = False
        self._held_stale = False
        """A message the client holds has more or other text than it shows."""
        self._order: list[_Segment | _ToolCall | _Reasoning] = []
        self._segment: _Segment | None = None
        self._open_calls: list[_ToolCall] = []
        self._ids = False
        """A tool event of this run carried a tool-call id, so completions pair by id only."""
        self._subagents: dict[str, _Subagent] = {}
        self._local = 0

    @property
    def hermes_run_id(self) -> str:
        return self._hermes_run_id

    @property
    def last_seq(self) -> int:
        """Highest ``seq`` fed or skipped so far; ``-1`` before any."""
        return self._last_seq

    @property
    def done(self) -> bool:
        """The run reached a terminal status."""
        return self._done

    @property
    def suspended(self) -> bool:
        """The current AG-UI run ended with an interrupt."""
        return self._suspended

    @property
    def pending_interrupts(self) -> list[str]:
        """Ids of the approvals the current AG-UI run is interrupted with."""
        return list(self._pending_interrupts)

    @property
    def completed_tool(self) -> str | None:
        """Raw name of the tool the last :meth:`feed` completed (for state refresh)."""
        return self._completed_tool

    def withdraw(self) -> None:
        """Forget the current interrupt, for one that turned out to be stale."""
        self._pending_interrupts.clear()
        self._suspended = False

    @property
    def id_prefix(self) -> str:
        return f"{self._hermes_run_id}:"

    def start(self, thread_id: str, run_id: str, messages: Iterable[Message] = (), *, replay: bool = False) -> list[BaseEvent]:
        """Open an AG-UI run, re-announcing what is still open from earlier events.

        ``replay`` marks a rejoin that feeds the run from the start of its backlog.
        """
        self._thread_id, self._run_id = thread_id, run_id
        self._prior = list(messages)
        self._reset_pending = replay and any(m.id.startswith(self.id_prefix) for m in self._prior)
        self._known_text, self._known_calls, self._known_results = {}, set(), {}
        for m in self._prior:
            content = getattr(m, "content", None)
            if isinstance(m, ToolMessage) and isinstance(content, str):
                self._known_results[m.id] = content
            elif isinstance(m, (AssistantMessage, ReasoningMessage)):
                self._known_text[m.id] = content if isinstance(content, str) else ""
                self._known_calls.update(c.id for c in getattr(m, "tool_calls", None) or ())
        self._suspended = False
        self._pending_interrupts.clear()
        if self._segment is not None:
            self._segment.announced = False
        return [run_started(thread_id, run_id), *(self._subagent_started(s) for s in self._subagents.values())]

    def feed(self, event: Mapping[str, Any]) -> list[BaseEvent]:
        self._completed_tool = None
        if self._done:
            return []
        seq = event.get("seq")
        reset: list[BaseEvent] = []
        if isinstance(seq, int):
            if not self.skip(event):
                return []
            key = str(seq)
            if self._reset_pending:
                reset = self._reset()
        else:
            self._local += 1
            key = f"x{self._local}"
        return reset + self._dispatch(event, key)

    def skip(self, event: Mapping[str, Any]) -> bool:
        """Count ``event`` as received without translating it; ``False`` if already seen."""
        seq = event.get("seq")
        if not isinstance(seq, int) or seq <= self._last_seq:
            return False
        if self._last_seq < 0 and seq > 0:
            self._backlog_start = seq
            self._truncate()
        self._last_seq = seq
        return True

    def _reset(self) -> list[BaseEvent]:
        """Drop the client's copy of this run so a full replay can rebuild it.

        Both ``@ag-ui/client`` (appends to a held id) and assistant-ui (drops a stream
        into a held id) mishandle events for messages the client already holds.
        """
        self._reset_pending = False
        if self._truncated:
            return []
        self._prior = [m for m in self._prior if not m.id.startswith(self.id_prefix)]
        self._known_text, self._known_calls, self._known_results = {}, set(), {}
        return [MessagesSnapshotEvent(messages=list(self._prior))]

    def _dispatch(self, event: Mapping[str, Any], key: str) -> list[BaseEvent]:
        name = str(event.get("event") or "")
        if name == "message.delta":
            delta = event.get("delta")
            return self._text(delta, key) if isinstance(delta, str) and delta else []
        if name == "message.interim":
            text = event.get("text")
            if event.get("already_streamed") or not isinstance(text, str) or not text.strip():
                return []
            self._head_unknown = False
            out = self._close_text()
            out += self._text(text, key)
            return out + self._close_text()
        if name == "tool.started":
            self._head_unknown = False
            return self._tool_started(event, key)
        if name == "tool.completed":
            return self._tool_completed(event, key)
        if name == "reasoning.available":
            return self._reasoning(event, key)
        if name == "subagent.start":
            return self._subagent_start(event, key)
        if name == "subagent.complete":
            return self._subagent_complete(event)
        if name == "approval.request":
            return self._approval_request(event, key)
        if name.startswith("run.") and name[4:] in TERMINAL_STATUSES:
            return self.finish({**event, "status": name[4:]})
        if name == "replay.truncated":
            self._truncate()
            return []
        if name in _IGNORED:
            if name == "approval.responded" and isinstance(event.get("request_id"), str):
                self._pending_interrupts.pop(event["request_id"], None)
            return []
        if self._custom_event_mapper is not None:
            mapped = self._custom_event_mapper(event)
            if mapped is not None:
                return list(mapped)
        if self._unknown_events == "raw":
            return [RawEvent(event=dict(event), source="hermes")]
        if self._unknown_events == "custom":
            return [CustomEvent(name=f"hermes.{name}", value=dict(event))]
        return []

    def interrupt(self) -> list[BaseEvent]:
        """End the AG-UI run with an interrupt for every pending approval."""
        if not self._pending_interrupts or self._done:
            return []
        events = self._close_text()
        # Hermes names no subagent, so only a lone open subagent can own the approvals.
        ids = list(self._pending_interrupts) if len(self._subagents) == 1 else None
        for sub in self._subagents.values():
            events.append(
                SubagentFinishedEvent(
                    subagent_run_id=sub.id,
                    outcome=SubagentFinishedSuspendedOutcome(interrupt_ids=ids),
                )
            )
        events.append(
            RunFinishedEvent(
                thread_id=self._thread_id,
                run_id=self._run_id,
                outcome=RunFinishedInterruptOutcome(interrupts=list(self._pending_interrupts.values())),
            )
        )
        self._suspended = True
        return events

    def finish(self, status: Mapping[str, Any]) -> list[BaseEvent]:
        """Close the run from a terminal status (``run.*`` event or ``GET /v1/runs/{id}``)."""
        state = str(status.get("status") or "")
        if self._done or state not in TERMINAL_STATUSES:
            return []
        self._done = True
        events: list[BaseEvent] = []
        if state == "completed":
            events += self._fix_up(status.get("output"))
        events += self._close_text()
        for sub in self._subagents.values():
            events.append(SubagentErrorEvent(subagent_run_id=sub.id, message=f"Run {state} before the subagent finished"))
        self._subagents.clear()
        usage = token_usage(status)
        if state == "completed":
            outcome: Any = RunFinishedSuccessOutcome()
        elif state == "cancelled":
            outcome = RunFinishedCancelledOutcome()
        else:
            events.append(RunErrorEvent(message=f"The Hermes run {state}.", code=state, usage=usage))
            return events
        events.append(RunFinishedEvent(thread_id=self._thread_id, run_id=self._run_id, outcome=outcome, usage=usage))
        return events

    def messages(self) -> list[Message]:
        """AG-UI messages for everything translated so far, in order."""
        out: list[Message] = []
        for item in self._order:
            if isinstance(item, _Segment):
                calls = [ToolCall(id=c.id, function=FunctionCall(name=c.name, arguments="{}")) for c in item.tool_calls if not c.hidden]
                if item.text or calls:
                    out.append(AssistantMessage(id=item.id, content=item.text or None, tool_calls=calls or None))
            elif isinstance(item, _ToolCall):
                out.append(
                    ToolMessage(
                        id=item.result_id,
                        tool_call_id=item.id,
                        content=item.result or "",
                        error=(item.result or "error") if item.error else None,
                    )
                )
            else:
                out.append(ReasoningMessage(id=item.id, content=item.text))
        return out

    # -- text -----------------------------------------------------------------

    def _truncate(self) -> None:
        self._truncated = True
        self._head_unknown = self._head_unknown or not self._order

    def _text(self, delta: str, key: str) -> list[BaseEvent]:
        events: list[BaseEvent] = []
        seg = self._segment
        if seg is None or not seg.open or seg.tool_calls:
            if self._head_unknown:
                # Inventing an id would duplicate the client's message; run.completed's
                # output reconciles the final answer.
                return []
            events += self._close_text()
            seg = self._new_segment(f"{self.id_prefix}msg:{key}")
            seg.open = True
        return events + self._append(seg, delta)

    def _append(self, seg: _Segment, delta: str) -> list[BaseEvent]:
        """Add ``delta`` to ``seg``; a message the client holds is left to ``run.completed``."""
        seg.text += delta
        if seg.id in self._known_text:
            self._held_stale = self._held_stale or self._known_text[seg.id] != seg.text
            return []
        events: list[BaseEvent] = []
        if not seg.announced:
            seg.announced = True
            events.append(TextMessageStartEvent(message_id=seg.id, role="assistant"))
        events.append(TextMessageContentEvent(message_id=seg.id, delta=delta))
        return events

    def _close_text(self) -> list[BaseEvent]:
        seg = self._segment
        if seg is None or not seg.open:
            return []
        seg.open = False
        return [TextMessageEndEvent(message_id=seg.id)] if seg.announced else []

    def _new_segment(self, message_id: str) -> _Segment:
        seg = _Segment(id=message_id)
        self._order.append(seg)
        self._segment = seg
        return seg

    def _fix_up(self, output: Any) -> list[BaseEvent]:
        """Reconcile streamed text with the canonical ``output`` of a completed run."""
        if not isinstance(output, str) or not output.strip():
            return []
        seg = self._segment
        final = seg if seg is not None and not seg.tool_calls else None
        streamed = final.text if final is not None else ""
        held = final is not None and final.id in self._known_text
        if streamed.strip() == output.strip() and not self._held_stale:
            return []
        if final is not None and final.open and not held and not self._held_stale and output.startswith(streamed):
            return self._append(final, output[len(streamed) :])
        known = {m.id for m in self.messages()}
        # Text the replay could not rebuild (its start was evicted) is replaced by the output.
        stale = [m for m in self._prior if m.id.startswith(self.id_prefix) and m.id not in known and isinstance(m, AssistantMessage) and not m.tool_calls]
        if final is None and not stale and not self._held_stale:
            seg = self._new_segment(f"{self.id_prefix}final")
            seg.open = True
            return self._append(seg, output) + self._close_text()
        events = self._close_text()
        if final is None:
            final = self._new_segment(f"{self.id_prefix}final")
        final.text = output
        events.append(MessagesSnapshotEvent(messages=self._snapshot(stale)))
        return events

    def _snapshot(self, drop: Sequence[Message]) -> list[Message]:
        """The client's messages with this run's rebuilt ones replaced by ours."""
        ours = self.messages()
        ids = {m.id for m in ours}
        calls = {c.id for m in ours if isinstance(m, AssistantMessage) for c in m.tool_calls or ()}
        prior: list[Message] = []
        for m in self._prior:
            if m.id in ids or m in drop:
                continue
            if isinstance(m, AssistantMessage) and m.tool_calls:
                m = m.model_copy(update={"tool_calls": [c for c in m.tool_calls if c.id not in calls] or None})
            prior.append(m)
        return prior + ours

    # -- tools ----------------------------------------------------------------

    def _view(self, event: Mapping[str, Any]) -> tuple[str, str] | None:
        name, preview = str(event.get("tool") or "tool"), str(event.get("preview") or "")
        return self._tool_filter(name, preview, event) if self._tool_filter else (name, preview)

    def _tool_started(self, event: Mapping[str, Any], key: str) -> list[BaseEvent]:
        events = self._close_text()
        view = self._view(event)
        raw = str(event.get("tool") or "tool")
        source = _source_id(event)
        self._ids = self._ids or source is not None
        # The event's id survives in the call id, so a completion still finds its call after
        # the backlog lost the start.
        cid = f"{self.id_prefix}tool:{key}" + (f":id:{source}" if source is not None else "")
        call = _ToolCall(id=cid, raw_name=raw, name=view[0] if view else raw, hidden=view is None, source_id=source)
        seg = self._segment or self._new_segment(f"{self.id_prefix}msg:{key}")
        seg.tool_calls.append(call)
        self._open_calls.append(call)
        if call.hidden or view is None or call.id in self._known_calls:
            return events
        metadata = {"preview": view[1]} if view[1] else None
        events.append(
            ToolCallStartEvent(
                tool_call_id=call.id,
                tool_call_name=call.name,
                parent_message_id=seg.id,
                metadata=metadata,
            )
        )
        events.append(ToolCallEndEvent(tool_call_id=call.id))
        return events

    def _tool_completed(self, event: Mapping[str, Any], key: str) -> list[BaseEvent]:
        raw = str(event.get("tool") or "tool")
        self._completed_tool = raw
        source = _source_id(event)
        self._ids = self._ids or source is not None
        if self._by_id():
            call = next((c for c in self._open_calls if source is not None and c.source_id == source), None)
        elif not self._truncated:
            call = next((c for c in self._open_calls if c.raw_name == raw), None)
        else:
            # A start the truncation lost, maybe of a hidden call, could own the completion.
            call = None
        events: list[BaseEvent] = []
        if call is None and self._truncated:
            call = self._held_call(event)
            if call is None:
                return []
        elif call is None:
            events = self._tool_started(event, key)
            call = self._open_calls[-1]
        if call in self._open_calls:
            self._open_calls.remove(call)
        for rid, interrupt in list(self._pending_interrupts.items()):
            if interrupt.tool_call_id == call.id:
                del self._pending_interrupts[rid]
        view = None if call.hidden else self._view(event)
        if view is None:
            return events
        call.result, call.error = view[1], bool(event.get("error"))
        self._order.append(call)
        if self._known_results.get(call.result_id) == call.result:
            return events
        events.append(
            ToolCallResultEvent(
                message_id=call.result_id,
                tool_call_id=call.id,
                content=call.result,
                role="tool",
                metadata={"error": call.error, "duration": event.get("duration")},
            )
        )
        return events

    def _held_calls(self) -> list[ToolCall]:
        return [c for m in self._prior if isinstance(m, AssistantMessage) and m.id.startswith(self.id_prefix) for c in m.tool_calls or ()]

    def _by_id(self) -> bool:
        """Completions pair by id only once the run carried one, here or in the evicted part."""
        return self._ids or (self._truncated and any(self._call_key(c.id)[1] is not None for c in self._held_calls()))

    def _call_key(self, call_id: str) -> tuple[int | None, str | None]:
        """The seq and the event's id a call id was made from."""
        seq, _, source = call_id.removeprefix(f"{self.id_prefix}tool:").partition(":id:")
        return (int(seq) if seq.isdigit() else None), source or None

    def _held_call(self, event: Mapping[str, Any]) -> _ToolCall | None:
        """The unfinished call the client holds for a completion whose start was evicted.

        Only a tool-call id can tell: the completion goes to the one unfinished call with its
        id that started before the backlog. Without ids, or without exactly one such call, the
        result is dropped rather than put on a call it may not belong to.
        """
        source = _source_id(event)
        if source is None or not self._by_id():
            return None
        held_results = {m.tool_call_id for m in self._prior if isinstance(m, ToolMessage)}
        done = {c.id for c in self._order if isinstance(c, _ToolCall)}

        def owns(c: ToolCall) -> bool:
            seq, held_source = self._call_key(c.id)
            return seq is not None and seq < self._backlog_start and held_source == source

        unfinished = [c for c in self._held_calls() if c.id not in done and c.id not in held_results and owns(c)]
        if len(unfinished) != 1:
            return None
        return _ToolCall(unfinished[0].id, str(event.get("tool") or "tool"), unfinished[0].function.name, False)

    def _open_visible_call(self, prefer: str | None = None) -> _ToolCall | None:
        visible = [c for c in self._open_calls if not c.hidden]
        preferred = [c for c in visible if c.raw_name == prefer]
        return (preferred or visible)[-1] if visible else None

    # -- reasoning, subagents, approvals --------------------------------------

    def _reasoning(self, event: Mapping[str, Any], key: str) -> list[BaseEvent]:
        text = event.get("text")
        if not isinstance(text, str) or not text.strip():
            return []
        self._head_unknown = False
        events = self._close_text()
        rid = f"{self.id_prefix}reasoning:{key}"
        self._order.append(_Reasoning(id=rid, text=text))
        known = self._known_text.get(rid)
        if known is not None:
            self._held_stale = self._held_stale or known != text
            return events
        return [
            *events,
            ReasoningStartEvent(message_id=rid),
            ReasoningMessageStartEvent(message_id=rid, role="reasoning"),
            ReasoningMessageContentEvent(message_id=rid, delta=text),
            ReasoningMessageEndEvent(message_id=rid),
            ReasoningEndEvent(message_id=rid),
        ]

    def _subagent_key(self, event: Mapping[str, Any]) -> str | None:
        if event.get("subagent_id") is not None:
            return str(event["subagent_id"])
        if event.get("delegation_id") is not None:
            return f"{event['delegation_id']}:{event.get('task_index', 0)}"
        return None

    def _subagent_started(self, sub: _Subagent) -> SubagentStartedEvent:
        return SubagentStartedEvent(
            subagent_run_id=sub.id,
            name="subagent",
            description=sub.description,
            parent_tool_call_id=sub.parent_tool_call_id,
            metadata=sub.metadata or None,
        )

    def _subagent_start(self, event: Mapping[str, Any], key: str) -> list[BaseEvent]:
        sid = f"{self.id_prefix}subagent:{self._subagent_key(event) or key}"
        if sid in self._subagents:
            return []
        parent = self._open_visible_call(prefer="delegate_task")
        goal = event.get("goal")
        sub = _Subagent(
            id=sid,
            description=goal if isinstance(goal, str) else None,
            parent_tool_call_id=parent.id if parent else None,
            metadata={k: event[k] for k in _SUBAGENT_KEYS if event.get(k) is not None},
        )
        self._subagents[sid] = sub
        return [self._subagent_started(sub)]

    def _subagent_complete(self, event: Mapping[str, Any]) -> list[BaseEvent]:
        skey = self._subagent_key(event)
        sid = f"{self.id_prefix}subagent:{skey}" if skey else next(iter(self._subagents), None)
        sub = self._subagents.pop(sid, None) if sid else None
        if sub is None:
            return []
        metadata = {k: event[k] for k in _SUBAGENT_KEYS if event.get(k) is not None}
        status = str(event.get("status") or "completed")
        summary = event.get("summary") or event.get("preview")
        if status == "completed":
            return [
                SubagentFinishedEvent(
                    subagent_run_id=sub.id,
                    result=summary,
                    outcome=SubagentFinishedSuccessOutcome(),
                    metadata=metadata or None,
                )
            ]
        return [
            SubagentErrorEvent(
                subagent_run_id=sub.id,
                message=str(summary or f"Subagent {status}"),
                code=status,
                metadata=metadata or None,
            )
        ]

    def reinterrupt(self, approvals: Iterable[Mapping[str, Any]]) -> list[BaseEvent]:
        """End the AG-UI run again for ``approval.request`` events a resume left unanswered.

        Feed or skip the backlog up to them first, so tool-call binding sees the same state.
        """
        for event in approvals:
            self._hold_approval(event, str(event.get("seq")))
        return self.interrupt()

    def _approval_request(self, event: Mapping[str, Any], key: str) -> list[BaseEvent]:
        self._hold_approval(event, key)
        return self.interrupt()

    def _hold_approval(self, event: Mapping[str, Any], key: str) -> None:
        request_id = event.get("request_id")
        iid = request_id if isinstance(request_id, str) and request_id else f"{self.id_prefix}approval:{key}"
        # Hermes names no tool call, so bind only when exactly one call is open and no
        # evicted part of the backlog could have opened another.
        bound = self._open_calls[0] if len(self._open_calls) == 1 and not self._truncated else None
        tool = event.get("tool")
        if bound and (bound.hidden or (isinstance(tool, str) and tool and tool != bound.raw_name)):
            bound = None
        description, command = event.get("description"), event.get("command")
        metadata = {k: event[k] for k in _APPROVAL_KEYS if event.get(k) is not None}
        metadata["hermesRunId"] = self._hermes_run_id
        parts = [str(p) for p in (description, command) if p]
        self._pending_interrupts[iid] = Interrupt(
            id=iid,
            reason="tool_call" if bound else "confirmation",
            message=": ".join(parts) or "Approval required",
            tool_call_id=bound.id if bound else None,
            response_schema=self._response_schema(event),
            expires_at=self._expires_at(event),
            metadata=metadata,
        )

    def _response_schema(self, event: Mapping[str, Any]) -> dict[str, Any]:
        """``{"choice": ...}`` over the choices Hermes offers and the host allows."""
        offered = event.get("choices")
        if not isinstance(offered, list) or not offered:
            offered = ["once", "deny"]
        allowed = self._approval_choices
        choices = [c for c in offered if isinstance(c, str) and (allowed is None or c in allowed or c == "deny")]
        return {
            "type": "object",
            "properties": {"choice": {"type": "string", "enum": choices or ["deny"]}},
            "required": ["choice"],
        }

    def _expires_at(self, event: Mapping[str, Any]) -> str | None:
        if self._approval_timeout is None:
            return None
        sent = event.get("timestamp")
        sent = sent if isinstance(sent, (int, float)) and not isinstance(sent, bool) else time.time()
        expires = datetime.fromtimestamp(sent + self._approval_timeout, UTC)
        return expires.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _source_id(event: Mapping[str, Any]) -> str | None:
    value = event.get("tool_call_id") or event.get("id")
    return str(value) if isinstance(value, (str, int)) and not isinstance(value, bool) else None
