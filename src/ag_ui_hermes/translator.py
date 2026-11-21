"""Pure translation of Hermes ``/v1/runs`` events into AG-UI events (no I/O)."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from ag_ui.core import (
    AssistantMessage,
    BaseEvent,
    CustomEvent,
    FunctionCall,
    Interrupt,
    Message,
    MessagesSnapshotEvent,
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
    ToolCall,
    ToolCallEndEvent,
    ToolCallResultEvent,
    ToolCallStartEvent,
    ToolMessage,
)

ToolFilter = Callable[[str, str, Mapping[str, Any]], "tuple[str, str] | None"]
"""``(tool, preview, raw_event) -> (tool, preview)`` to rewrite, or ``None`` to drop.

Called on ``tool.started`` (dropping hides the whole call) and on ``tool.completed``
(dropping hides only the result)."""

CustomEventMapper = Callable[[Mapping[str, Any]], "Sequence[BaseEvent] | None"]
"""Maps an event the translator does not know; ``None`` falls through to the default."""

TERMINAL_STATUSES = frozenset({"completed", "failed", "cancelled", "interrupted"})
_IGNORED = frozenset(
    {"approval.responded", "run.waiting_for_approval", "run.steered", "run.stopping"}
)
_SUBAGENT_KEYS = (
    "goal", "task_count", "task_index", "subagent_id", "child_session_id", "delegation_id",
    "parent_id", "depth", "model", "tool_count", "status", "summary", "duration_seconds",
    "input_tokens", "output_tokens", "reasoning_tokens", "api_calls", "cost_usd",
)
_APPROVAL_KEYS = ("command", "description", "pattern_key", "pattern_keys", "choices", "smart_denied")


@dataclass
class _Segment:
    id: str
    text: str = ""
    open: bool = False
    tool_calls: list[_ToolCall] = field(default_factory=list)


@dataclass
class _ToolCall:
    id: str
    raw_name: str
    name: str
    hidden: bool
    result: str | None = None
    error: bool = False

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

    Ids derive from the Hermes run id and each event's ``seq``, so feeding the same
    events again (a replay from seq 0) yields identical ids. Call :meth:`start` once
    per AG-UI run, then :meth:`feed` every Hermes event; :meth:`finish` takes a
    ``GET /v1/runs/{id}`` status when the stream ended without a terminal event.
    """

    def __init__(
        self,
        hermes_run_id: str,
        *,
        tool_filter: ToolFilter | None = None,
        custom_event_mapper: CustomEventMapper | None = None,
        passthrough_unknown: bool = False,
    ) -> None:
        self.hermes_run_id = hermes_run_id
        self.tool_filter = tool_filter
        self.custom_event_mapper = custom_event_mapper
        self.passthrough_unknown = passthrough_unknown
        self.thread_id = ""
        self.run_id = ""
        self.last_seq = -1
        self.done = False
        self.suspended = False
        self.truncated = False
        self.pending_interrupts: dict[str, Interrupt] = {}
        self.completed_tool: str | None = None
        """Raw name of the tool the last :meth:`feed` completed (for state refresh)."""
        self._prior: list[Message] = []
        self._order: list[_Segment | _ToolCall | _Reasoning] = []
        self._segment: _Segment | None = None
        self._open_calls: list[_ToolCall] = []
        self._subagents: dict[str, _Subagent] = {}
        self._local = 0

    @property
    def id_prefix(self) -> str:
        return f"{self.hermes_run_id}:"

    def start(
        self, thread_id: str, run_id: str, messages: Iterable[Message] = ()
    ) -> list[BaseEvent]:
        """Open an AG-UI run, re-announcing what is still open from earlier events."""
        self.thread_id, self.run_id = thread_id, run_id
        self._prior = list(messages)
        self.suspended = False
        self.pending_interrupts.clear()
        events: list[BaseEvent] = [RunStartedEvent(thread_id=thread_id, run_id=run_id)]
        for sub in self._subagents.values():
            events.append(self._subagent_started(sub))
        if self._segment is not None and self._segment.open:
            events.append(TextMessageStartEvent(message_id=self._segment.id, role="assistant"))
        return events

    def feed(self, event: Mapping[str, Any]) -> list[BaseEvent]:
        self.completed_tool = None
        if self.done:
            return []
        seq = event.get("seq")
        if isinstance(seq, int):
            if seq <= self.last_seq:
                return []
            self.last_seq = seq
            key = str(seq)
        else:
            self._local += 1
            key = f"x{self._local}"
        name = str(event.get("event") or "")
        if name == "message.delta":
            delta = event.get("delta")
            return self._text(delta, key) if isinstance(delta, str) and delta else []
        if name == "message.interim":
            text = event.get("text")
            if event.get("already_streamed") or not isinstance(text, str) or not text.strip():
                return []
            out = self._close_text()
            out += self._text(text, key)
            return out + self._close_text()
        if name == "tool.started":
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
            self.truncated = True
            return []
        if name in _IGNORED:
            if name == "approval.responded" and isinstance(event.get("request_id"), str):
                self.pending_interrupts.pop(event["request_id"], None)
            return []
        if self.custom_event_mapper is not None:
            mapped = self.custom_event_mapper(event)
            if mapped is not None:
                return list(mapped)
        if self.passthrough_unknown:
            return [CustomEvent(name=f"hermes.{name}", value=dict(event))]
        return []

    def interrupt(self) -> list[BaseEvent]:
        """End the AG-UI run with an interrupt for every pending approval."""
        if not self.pending_interrupts or self.done:
            return []
        events = self._close_text()
        ids = list(self.pending_interrupts)
        for sub in self._subagents.values():
            events.append(
                SubagentFinishedEvent(
                    subagent_run_id=sub.id,
                    outcome=SubagentFinishedSuspendedOutcome(interrupt_ids=ids),
                )
            )
        events.append(
            RunFinishedEvent(
                thread_id=self.thread_id,
                run_id=self.run_id,
                outcome=RunFinishedInterruptOutcome(
                    interrupts=list(self.pending_interrupts.values())
                ),
            )
        )
        self.suspended = True
        return events

    def finish(self, status: Mapping[str, Any]) -> list[BaseEvent]:
        """Close the run from a terminal status (``run.*`` event or ``GET /v1/runs/{id}``)."""
        state = str(status.get("status") or "")
        if self.done or state not in TERMINAL_STATUSES:
            return []
        self.done = True
        events: list[BaseEvent] = []
        if state == "completed":
            events += self._fix_up(status.get("output"))
        events += self._close_text()
        for sub in self._subagents.values():
            events.append(
                SubagentErrorEvent(
                    subagent_run_id=sub.id, message=f"Run {state} before the subagent finished"
                )
            )
        self._subagents.clear()
        error = str(status.get("error") or f"Hermes run {state}")
        if state == "completed":
            outcome: Any = RunFinishedSuccessOutcome()
        elif state == "cancelled":
            outcome = RunFinishedCancelledOutcome()
        else:
            events.append(RunErrorEvent(message=error, code=state))
            return events
        events.append(RunFinishedEvent(thread_id=self.thread_id, run_id=self.run_id, outcome=outcome))
        return events

    def messages(self) -> list[Message]:
        """AG-UI messages for everything translated so far, in order."""
        out: list[Message] = []
        for item in self._order:
            if isinstance(item, _Segment):
                calls = [
                    ToolCall(id=c.id, function=FunctionCall(name=c.name, arguments="{}"))
                    for c in item.tool_calls
                    if not c.hidden
                ]
                if item.text or calls:
                    out.append(
                        AssistantMessage(id=item.id, content=item.text or None, tool_calls=calls or None)
                    )
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

    def _text(self, delta: str, key: str) -> list[BaseEvent]:
        events: list[BaseEvent] = []
        seg = self._segment
        if seg is None or not seg.open or seg.tool_calls:
            events += self._close_text()
            seg = self._new_segment(f"{self.id_prefix}msg:{key}")
            seg.open = True
            events.append(TextMessageStartEvent(message_id=seg.id, role="assistant"))
        seg.text += delta
        events.append(TextMessageContentEvent(message_id=seg.id, delta=delta))
        return events

    def _close_text(self) -> list[BaseEvent]:
        seg = self._segment
        if seg is None or not seg.open:
            return []
        seg.open = False
        return [TextMessageEndEvent(message_id=seg.id)]

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
        if streamed.strip() == output.strip():
            return []
        if final is not None and final.open and output.startswith(streamed):
            final.text = output
            suffix = output[len(streamed):]
            return [TextMessageContentEvent(message_id=final.id, delta=suffix)]
        known = {m.id for m in self.messages()}
        stale = [m for m in self._prior if m.id.startswith(self.id_prefix) and m.id not in known]
        if final is None and not stale:
            seg = self._new_segment(f"{self.id_prefix}final")
            seg.text = output
            return [
                TextMessageStartEvent(message_id=seg.id, role="assistant"),
                TextMessageContentEvent(message_id=seg.id, delta=output),
                TextMessageEndEvent(message_id=seg.id),
            ]
        events = self._close_text()
        if final is None:
            final = self._new_segment(f"{self.id_prefix}final")
        final.text = output
        ours = self.messages()
        ids = {m.id for m in ours}
        prior = [m for m in self._prior if m.id not in ids and m not in stale]
        events.append(MessagesSnapshotEvent(messages=prior + ours))
        return events

    # -- tools ----------------------------------------------------------------

    def _view(self, event: Mapping[str, Any]) -> tuple[str, str] | None:
        name, preview = str(event.get("tool") or "tool"), str(event.get("preview") or "")
        return self.tool_filter(name, preview, event) if self.tool_filter else (name, preview)

    def _tool_started(self, event: Mapping[str, Any], key: str) -> list[BaseEvent]:
        events = self._close_text()
        view = self._view(event)
        raw = str(event.get("tool") or "tool")
        call = _ToolCall(
            id=f"{self.id_prefix}tool:{key}", raw_name=raw, name=view[0] if view else raw,
            hidden=view is None,
        )
        seg = self._segment or self._new_segment(f"{self.id_prefix}msg:{key}")
        seg.tool_calls.append(call)
        self._open_calls.append(call)
        if call.hidden or view is None:
            return events
        metadata = {"preview": view[1]} if view[1] else None
        events.append(
            ToolCallStartEvent(
                tool_call_id=call.id, tool_call_name=call.name, parent_message_id=seg.id,
                metadata=metadata,
            )
        )
        events.append(ToolCallEndEvent(tool_call_id=call.id))
        return events

    def _tool_completed(self, event: Mapping[str, Any], key: str) -> list[BaseEvent]:
        raw = str(event.get("tool") or "tool")
        call = next((c for c in self._open_calls if c.raw_name == raw), None)
        events: list[BaseEvent] = []
        if call is None:
            events = self._tool_started(event, key)
            call = self._open_calls[-1]
        self._open_calls.remove(call)
        self.completed_tool = raw
        for rid, interrupt in list(self.pending_interrupts.items()):
            if interrupt.tool_call_id == call.id:
                del self.pending_interrupts[rid]
        view = None if call.hidden else self._view(event)
        if view is None:
            return events
        call.result, call.error = view[1], bool(event.get("error"))
        self._order.append(call)
        events.append(
            ToolCallResultEvent(
                message_id=call.result_id, tool_call_id=call.id, content=call.result, role="tool",
                metadata={"error": call.error, "duration": event.get("duration")},
            )
        )
        return events

    def _open_visible_call(self, prefer: str | None = None) -> _ToolCall | None:
        visible = [c for c in self._open_calls if not c.hidden]
        preferred = [c for c in visible if c.raw_name == prefer]
        return (preferred or visible)[-1] if visible else None

    # -- reasoning, subagents, approvals --------------------------------------

    def _reasoning(self, event: Mapping[str, Any], key: str) -> list[BaseEvent]:
        text = event.get("text")
        if not isinstance(text, str) or not text.strip():
            return []
        events = self._close_text()
        rid = f"{self.id_prefix}reasoning:{key}"
        self._order.append(_Reasoning(id=rid, text=text))
        return events + [
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
            subagent_run_id=sub.id, name="subagent", description=sub.description,
            parent_tool_call_id=sub.parent_tool_call_id, metadata=sub.metadata or None,
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
                    subagent_run_id=sub.id, result=summary,
                    outcome=SubagentFinishedSuccessOutcome(), metadata=metadata or None,
                )
            ]
        return [
            SubagentErrorEvent(
                subagent_run_id=sub.id, message=str(summary or f"Subagent {status}"), code=status,
                metadata=metadata or None,
            )
        ]

    def _approval_request(self, event: Mapping[str, Any], key: str) -> list[BaseEvent]:
        request_id = event.get("request_id")
        iid = request_id if isinstance(request_id, str) and request_id else f"{self.id_prefix}approval:{key}"
        # Hermes names no tool call, so bind only when exactly one call is open.
        bound = self._open_calls[0] if len(self._open_calls) == 1 else None
        tool = event.get("tool")
        if bound and (bound.hidden or (isinstance(tool, str) and tool and tool != bound.raw_name)):
            bound = None
        description, command = event.get("description"), event.get("command")
        metadata = {k: event[k] for k in _APPROVAL_KEYS if event.get(k) is not None}
        metadata["hermesRunId"] = self.hermes_run_id
        parts = [str(p) for p in (description, command) if p]
        self.pending_interrupts[iid] = Interrupt(
            id=iid,
            reason="tool_call" if bound else "confirmation",
            message=": ".join(parts) or "Approval required",
            tool_call_id=bound.id if bound else None,
            metadata=metadata,
        )
        return self.interrupt()
