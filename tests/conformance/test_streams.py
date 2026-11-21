"""AG-UI 1.0 conformance of the translator's output.

``streams/`` holds fixtures copied verbatim from the spec repo
(ag-ui-protocol/ag-ui ``spec/1.0/conformance/streams`` at c31e1e8). They test consumers, so
:func:`verify` mirrors the stream rules the reference client enforces; the fixtures pin it
to the same verdicts and error messages, and then it checks what the translator emits.
"""

import json
from pathlib import Path
from typing import Any

import pytest
from ag_ui.core import (
    AssistantMessage,
    BaseEvent,
    Event,
    FunctionCall,
    Message,
    SubagentFinishedEvent,
    ToolCall,
    UserMessage,
)
from pydantic import BaseModel, TypeAdapter

from ag_ui_hermes import HermesTranslator
from ag_ui_hermes.translator import run_started
from tests.fake_hermes import FakeHermes
from tests.test_agent import agent_input, answer, collect, make_agent
from tests.test_translator import RUN, hide_secrets

STREAMS = Path(__file__).parent / "streams"
_EVENT: TypeAdapter[Any] = TypeAdapter(Event)


def verify(stream: list[dict[str, Any]]) -> None:
    """Raise ``ValueError`` on a stream the reference client rejects."""
    run: str | None = None  # "active", "finished", "errored"
    text: set[str] = set()
    tools: set[str] = set()
    reasoning: set[str] = set()
    subagents: set[str] = set()
    opens = {"TEXT_MESSAGE_START": text, "TOOL_CALL_START": tools, "REASONING_MESSAGE_START": reasoning}
    needs = {
        "TEXT_MESSAGE_CONTENT": (text, "text message"),
        "TEXT_MESSAGE_END": (text, "text message"),
        "TOOL_CALL_ARGS": (tools, "tool call"),
        "TOOL_CALL_END": (tools, "tool call"),
        "REASONING_MESSAGE_CONTENT": (reasoning, "reasoning message"),
        "REASONING_MESSAGE_END": (reasoning, "reasoning message"),
    }
    for event in stream:
        kind = event["type"]
        if run is None and kind not in ("RUN_STARTED", "RUN_ERROR"):
            raise ValueError("First event must be 'RUN_STARTED'")
        if kind == "RUN_STARTED":
            if run == "active":
                raise ValueError("Cannot send 'RUN_STARTED' while a run is still active")
            run = "active"
            continue
        if kind == "RUN_ERROR":
            run = "errored"
            continue
        if run == "finished":
            raise ValueError("The run has already finished with 'RUN_FINISHED'")
        if run == "errored":
            raise ValueError("The run has already errored with 'RUN_ERROR'")
        if kind in opens:
            ident = event.get("messageId") or event["toolCallId"]
            if ident in opens[kind]:
                raise ValueError(f"'{kind}' for '{ident}' while it is still active")
            opens[kind].add(ident)
        elif kind in needs:
            active, what = needs[kind]
            ident = event.get("messageId") or event["toolCallId"]
            if ident not in active:
                raise ValueError(f"No active {what} found with ID '{ident}'")
            if kind.endswith("_END"):
                active.discard(ident)
        elif kind == "SUBAGENT_STARTED":
            subagents.add(event["subagentRunId"])
        elif kind in ("SUBAGENT_FINISHED", "SUBAGENT_ERROR"):
            if event["subagentRunId"] not in subagents:
                raise ValueError(f"No active subagent found with ID '{event['subagentRunId']}'")
            subagents.discard(event["subagentRunId"])
        elif kind == "RUN_FINISHED":
            for name, active in (
                ("text messages", text),
                ("tool calls", tools),
                ("reasoning messages", reasoning),
                ("subagents", subagents),
            ):
                if active:
                    raise ValueError(f"Cannot send 'RUN_FINISHED' while {name} are still active: {sorted(active)}")
            run = "finished"


def fixture(name: str) -> dict[str, Any]:
    data: dict[str, Any] = json.loads((STREAMS / f"{name}.json").read_text())
    return data


@pytest.mark.parametrize("name", sorted(p.stem for p in STREAMS.glob("*.json")))
def test_verify_agrees_with_the_spec_fixture(name: str) -> None:
    case = fixture(name)
    expect = case["expect"]
    if expect["outcome"] == "failed":
        with pytest.raises(ValueError, match=expect["errorContains"].replace("'", ".")):
            verify(case["stream"])
    else:
        verify(case["stream"])


def wire(events: list[BaseEvent]) -> list[dict[str, Any]]:
    """Serialize as on the wire and check each event against the protocol models."""
    out = []
    for event in events:
        data = event.model_dump(mode="json", by_alias=True, exclude_none=True)
        parsed = _EVENT.validate_python(data)
        assert parsed.model_dump(mode="json", by_alias=True, exclude_none=True) == data
        assert not _extras(parsed), data
        out.append(data)
    return out


def _extras(value: Any) -> bool:
    if isinstance(value, BaseModel):
        return bool(value.model_extra) or any(_extras(getattr(value, f)) for f in type(value).model_fields)
    if isinstance(value, list):
        return any(_extras(v) for v in value)
    return False


def feed(
    t: HermesTranslator, run_id: str, events: list[dict[str, Any]], start: int = 0, held: list[Message] | None = None, *, replay: bool = False
) -> list[BaseEvent]:
    out = t.start("thread", run_id, held or [UserMessage(id="u1", content="Hi")], replay=replay)
    for seq, event in enumerate(events, start=start):
        out += t.feed({**event, "seq": seq})
    return out


def test_translator_output_conforms() -> None:
    full = HermesTranslator("run_1", tool_filter=hide_secrets, passthrough_unknown=True, raw_unknown=True)
    streams = [feed(full, "a1", RUN)]

    # An approval while a subagent works suspends the run; the resume is a second AG-UI run.
    suspended = HermesTranslator("r2", approval_timeout=60)
    head: list[dict[str, Any]] = [
        {"event": "tool.started", "tool": "delegate_task"},
        {"event": "subagent.start", "subagent_id": "s1", "goal": "look"},
        {"event": "message.delta", "delta": "Delegating"},
        {"event": "approval.request", "request_id": "q1", "command": "rm -rf /tmp/x", "choices": ["once", "deny"]},
    ]
    tail: list[dict[str, Any]] = [
        {"event": "approval.responded", "request_id": "q1", "choice": "once"},
        {"event": "message.delta", "delta": " more"},
        {"event": "subagent.complete", "subagent_id": "s1", "status": "completed", "summary": "done"},
        {"event": "tool.completed", "tool": "delegate_task", "preview": "done"},
        {"event": "run.completed", "output": "Finished", "usage": {"input_tokens": 5, "output_tokens": 2}},
    ]
    streams.append(feed(suspended, "a2", head) + feed(suspended, "a3", tail, start=len(head)))
    owned = [e for e in streams[-1] if isinstance(e, SubagentFinishedEvent) and e.outcome]
    assert [e.outcome.model_dump(exclude_none=True) for e in owned if e.outcome] == [
        {"type": "suspended", "interrupt_ids": ["q1"]},
        {"type": "success"},
    ]

    streams += [
        feed(
            HermesTranslator("r3"),
            "a4",
            [
                {"event": "message.delta", "delta": "Half"},
                {"event": "subagent.start", "subagent_id": "s1"},
                {"event": "reasoning.available", "text": "hmm"},
                {"event": f"run.{status}", "error": "boom"},
            ],
        )
        for status in ("failed", "cancelled")
    ]

    # A rejoin whose backlog lost its head reconciles from the final output.
    call = ToolCall(id="r4:tool:0", function=FunctionCall(name="a", arguments="{}"))
    held: list[Message] = [
        UserMessage(id="u1", content="Hi"),
        AssistantMessage(id="r4:msg:0", tool_calls=[call]),
        AssistantMessage(id="r4:msg:2", content="Hel"),
    ]
    streams.append(
        feed(
            HermesTranslator("r4"),
            "a5",
            [
                {"event": "tool.completed", "tool": "a", "preview": "ok"},
                {"event": "message.delta", "delta": "lo"},
                {"event": "run.completed", "output": "Hello"},
            ],
            start=3,
            held=held,
            replay=True,
        )
    )

    declared = fixture("current-peer-declares-protocol-version")["stream"][0]
    for stream in streams:
        events = wire(stream)
        verify(events)
        assert events[0].keys() == declared.keys()
        assert events[0]["protocolVersion"] == declared["protocolVersion"]
    assert wire([run_started("t", "r")])[0]["protocolVersion"] == "1.0"


async def test_a_resume_after_a_truncated_backlog_conforms() -> None:
    fake = FakeHermes(
        [
            {"event": "message.delta", "delta": "Delegating"},
            {"event": "tool.started", "tool": "delegate_task"},
            {"event": "subagent.start", "subagent_id": "s1", "goal": "look"},
            {"event": "approval.request", "request_id": "q1", "command": "ls"},
            {"event": "subagent.complete", "subagent_id": "s1", "status": "completed", "summary": "done"},
            {"event": "tool.completed", "tool": "delegate_task", "preview": "done"},
            {"event": "run.completed", "output": "Finished"},
        ]
    )
    agent = make_agent(fake)
    verify(wire(await collect(agent.run(agent_input()))))
    fake.runs["run_1"].evicted = 1  # the resume's replay opens with an unsequenced replay.truncated
    resumed = wire(await collect(agent.run(answer("r2", "q1"))))
    verify(resumed)
    assert [e["type"] for e in resumed if e["type"].startswith("SUBAGENT")] == ["SUBAGENT_STARTED", "SUBAGENT_FINISHED"]
