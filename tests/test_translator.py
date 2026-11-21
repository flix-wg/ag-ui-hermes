from typing import Any

import pytest
from ag_ui.core import (
    AssistantMessage,
    BaseEvent,
    CustomEvent,
    FunctionCall,
    Message,
    MessagesSnapshotEvent,
    RawEvent,
    ReasoningMessage,
    RunFinishedEvent,
    TextMessageContentEvent,
    ToolCall,
    ToolCallResultEvent,
    ToolCallStartEvent,
    ToolMessage,
    UserMessage,
)

from ag_ui_hermes import HermesTranslator


def feed_all(t: HermesTranslator, events: list[dict[str, Any]]) -> list[BaseEvent]:
    out = t.start("thread", "agui-run", [UserMessage(id="u1", content="Lights on?")])
    for seq, event in enumerate(events):
        out += t.feed({**event, "seq": seq})
    return out


RUN: list[dict[str, Any]] = [
    {"event": "message.delta", "delta": "Let me "},
    {"event": "message.delta", "delta": "check."},
    {"event": "tool.started", "tool": "light.turn_on", "preview": "kitchen"},
    {"event": "tool.started", "tool": "light.turn_on", "preview": "hall"},
    {"event": "tool.started", "tool": "secret_lookup", "preview": "token"},
    {"event": "reasoning.available", "text": "Both lights requested."},
    {"event": "tool.completed", "tool": "light.turn_on", "duration": 0.2, "error": False, "preview": "kitchen on"},
    {"event": "tool.completed", "tool": "secret_lookup", "duration": 0.1, "error": False, "preview": "abc"},
    {"event": "tool.completed", "tool": "light.turn_on", "duration": 0.3, "error": True, "preview": "hall offline"},
    {"event": "dashboard.scene", "value": 1},
    {"event": "message.delta", "delta": "Kitchen is on."},
    {
        "event": "run.completed",
        "output": "Kitchen is on.",
        "completed": True,
        "usage": {"input_tokens": 120, "output_tokens": 30, "total_tokens": 150, "cache_read_tokens": 100, "cache_write_tokens": 0},
        "runtime": {"provider": "openrouter", "model": "glm-5", "route_source": "global"},
    },
]


def hide_secrets(tool: str, preview: str, _event: object) -> tuple[str, str] | None:
    return None if tool == "secret_lookup" else (tool, preview)


def test_maps_text_tools_reasoning_and_replays_with_identical_ids() -> None:
    events = feed_all(HermesTranslator("run_1", tool_filter=hide_secrets), RUN)

    assert [e.type.value for e in events] == [
        "RUN_STARTED",
        "TEXT_MESSAGE_START",
        "TEXT_MESSAGE_CONTENT",
        "TEXT_MESSAGE_CONTENT",
        "TEXT_MESSAGE_END",
        "TOOL_CALL_START",
        "TOOL_CALL_END",
        "TOOL_CALL_START",
        "TOOL_CALL_END",
        "REASONING_START",
        "REASONING_MESSAGE_START",
        "REASONING_MESSAGE_CONTENT",
        "REASONING_MESSAGE_END",
        "REASONING_END",
        "TOOL_CALL_RESULT",
        "TOOL_CALL_RESULT",
        "TEXT_MESSAGE_START",
        "TEXT_MESSAGE_CONTENT",
        "TEXT_MESSAGE_END",
        "RUN_FINISHED",
    ]
    starts = [e for e in events if isinstance(e, ToolCallStartEvent)]
    results = [e for e in events if isinstance(e, ToolCallResultEvent)]
    # Hermes sends no tool-call ids: completions pair FIFO per tool name.
    assert [r.tool_call_id for r in results] == [s.tool_call_id for s in starts]
    assert [(r.content, r.metadata and r.metadata["error"]) for r in results] == [("kitchen on", False), ("hall offline", True)]
    assert starts[0].parent_message_id == "run_1:msg:0"
    finished = events[-1]
    assert isinstance(finished, RunFinishedEvent)
    assert finished.outcome
    assert finished.outcome.type == "success"
    assert events[0].model_dump(by_alias=True, exclude_none=True)["protocolVersion"] == "1.0"
    assert finished.usage
    assert finished.usage[0].model_dump(by_alias=True, exclude_none=True) == {
        "provider": "openrouter",
        "model": "glm-5",
        "inputTokens": 120,
        "outputTokens": 30,
        "totalTokens": 150,
        "cachedInputTokens": 100,
    }

    replayed = feed_all(HermesTranslator("run_1", tool_filter=hide_secrets), RUN)
    assert [e.model_dump() for e in replayed] == [e.model_dump() for e in events]

    unknown = [e for mode in ("raw", "custom") for e in feed_all(HermesTranslator("run_1", unknown_events=mode), RUN) if isinstance(e, (RawEvent, CustomEvent))]
    assert [(e.type.value, e.event if isinstance(e, RawEvent) else e.name) for e in unknown] == [
        ("RAW", {"event": "dashboard.scene", "value": 1, "seq": 9}),
        ("CUSTOM", "hermes.dashboard.scene"),
    ]
    assert isinstance(unknown[0], RawEvent)
    assert unknown[0].source == "hermes"


def test_canonical_output_fixes_streamed_text() -> None:
    suffix = feed_all(
        HermesTranslator("r"),
        [
            {"event": "message.delta", "delta": "Hel"},
            {"event": "run.completed", "output": "Hello"},
        ],
    )
    assert [e.delta for e in suffix if isinstance(e, TextMessageContentEvent)] == ["Hel", "lo"]

    unstreamed = feed_all(
        HermesTranslator("r"),
        [
            {"event": "tool.started", "tool": "t"},
            {"event": "tool.completed", "tool": "t", "preview": "x"},
            {"event": "run.completed", "output": "Answer"},
        ],
    )
    final = [e for e in unstreamed if isinstance(e, TextMessageContentEvent)]
    assert [(e.message_id, e.delta) for e in final] == [("r:final", "Answer")]

    rewritten = feed_all(
        HermesTranslator("r"),
        [
            {"event": "message.delta", "delta": "Draft answer"},
            {"event": "run.completed", "output": "Final answer"},
        ],
    )
    snapshot = next(e for e in rewritten if isinstance(e, MessagesSnapshotEvent))
    assert [m.id for m in snapshot.messages] == ["u1", "r:msg:0"]
    assert isinstance(snapshot.messages[1], AssistantMessage)
    assert snapshot.messages[1].content == "Final answer"
    assert rewritten[-1].type.value == "RUN_FINISHED"


def test_approval_binds_a_tool_call_only_when_unambiguous() -> None:
    t = HermesTranslator("r", tool_filter=hide_secrets, approval_choices={"once"}, approval_timeout=300)
    events = feed_all(
        t,
        [
            {"event": "tool.started", "tool": "terminal", "preview": "ls"},
            {"event": "tool.started", "tool": "secret_lookup", "preview": "token"},
            {
                "event": "approval.request",
                "request_id": "q1",
                "command": "cat ~/.ssh/id_rsa",
                "description": "read a private key",
                "choices": ["once", "session", "always", "deny"],
                "timestamp": 1795000000.25,
            },
        ],
    )
    finished = events[-1]
    assert isinstance(finished, RunFinishedEvent)
    assert finished.outcome
    assert finished.outcome.type == "interrupt"
    interrupt = finished.outcome.interrupts[0]
    assert (interrupt.reason, interrupt.tool_call_id) == ("confirmation", None)
    assert interrupt.message == "read a private key: cat ~/.ssh/id_rsa"
    assert interrupt.metadata
    assert interrupt.metadata["command"] == "cat ~/.ssh/id_rsa"
    assert interrupt.response_schema == {
        "type": "object",
        "properties": {"choice": {"type": "string", "enum": ["once", "deny"]}},
        "required": ["choice"],
    }
    assert interrupt.expires_at == "2026-11-18T11:11:40.250Z"


def test_rejoin_replaces_the_clients_copy_with_a_full_replay() -> None:
    seen = HermesTranslator("run_1", tool_filter=hide_secrets)
    feed_all(seen, RUN[:7])
    # The client holds an early message that diverged and the later ones as streamed.
    held = [UserMessage(id="u1", content="Lights on?"), *seen.messages()]
    held[1] = AssistantMessage.model_validate({**held[1].model_dump(), "content": "Draft"})

    t = HermesTranslator("run_1", tool_filter=hide_secrets)
    events = t.start("thread", "agui-run", held, replay=True)
    for seq, event in enumerate(RUN):
        events += t.feed({**event, "seq": seq})

    reset = events[1]
    assert isinstance(reset, MessagesSnapshotEvent)
    assert [m.id for m in reset.messages] == ["u1"]
    fresh = feed_all(HermesTranslator("run_1", tool_filter=hide_secrets), RUN)
    assert [e.model_dump() for e in events[2:]] == [e.model_dump() for e in fresh[1:]]


def test_truncated_backlog_reconciles_from_the_final_output() -> None:
    t = HermesTranslator("r")
    held: list[Message] = [
        UserMessage(id="u1", content="Hi"),
        AssistantMessage(id="r:msg:0", tool_calls=[ToolCall(id="r:tool:0", function=FunctionCall(name="a", arguments="{}"))]),
        ToolMessage(id="r:tool:0:result", tool_call_id="r:tool:0", content="done"),
        AssistantMessage(id="r:msg:2", content="Hel"),
    ]
    events = t.start("thread", "agui-run", held, replay=True)
    for seq, event in enumerate(
        [
            {"event": "message.delta", "delta": "lo wor"},  # the segment opened at seq 2, now evicted
            {"event": "message.delta", "delta": "ld"},
            {"event": "run.completed", "output": "Hello world"},
        ],
        start=5,
    ):
        events += t.feed({**event, "seq": seq})

    assert not [e for e in events if isinstance(e, TextMessageContentEvent)]
    snapshots = [e for e in events if isinstance(e, MessagesSnapshotEvent)]
    assert len(snapshots) == 1
    assert [m.id for m in snapshots[0].messages] == ["u1", "r:msg:0", "r:tool:0:result", "r:final"]
    assert snapshots[0].messages[-1].content == "Hello world"

    # Without ids, a completion whose start was evicted could belong to any held call of its tool.
    tools = HermesTranslator("r")
    tools.start("thread", "agui-run", held[:2], replay=True)
    assert tools.feed({"event": "tool.completed", "tool": "a", "preview": "done", "seq": 5}) == []

    # Stale held text is replaced even when the fresh final segment already equals the output.
    stale = HermesTranslator("r")
    out = stale.start("thread", "agui-run", [held[0], ReasoningMessage(id="r:reasoning:5", content="old")], replay=True)
    for seq, event in enumerate(
        [
            {"event": "reasoning.available", "text": "new"},
            {"event": "message.delta", "delta": "Hello"},
            {"event": "run.completed", "output": "Hello"},
        ],
        start=5,
    ):
        out += stale.feed({**event, "seq": seq})
    assert [e.delta for e in out if isinstance(e, TextMessageContentEvent)] == ["Hello"]
    fixed = next(e for e in out if isinstance(e, MessagesSnapshotEvent))
    assert [m.content for m in fixed.messages] == ["Hi", "new", "Hello"]

    # Another call may have opened in the evicted part, so the approval is not bound.
    approval = HermesTranslator("r")
    approval.start("thread", "agui-run")
    approval.feed({"event": "tool.started", "tool": "terminal", "seq": 7})
    finished = approval.feed({"event": "approval.request", "request_id": "q1", "tool": "terminal", "seq": 8})[-1]
    assert isinstance(finished, RunFinishedEvent)
    assert finished.outcome
    assert finished.outcome.type == "interrupt"
    assert (finished.outcome.interrupts[0].reason, finished.outcome.interrupts[0].tool_call_id) == ("confirmation", None)


def test_evicted_completion_never_lands_on_a_call_it_does_not_own() -> None:
    def call(i: int) -> ToolCall:
        return ToolCall(id=f"r:tool:{i}", function=FunctionCall(name="a", arguments="{}"))

    def replay(held: list[Message], events: list[dict[str, Any]], start: int) -> list[tuple[str, Any]]:
        t = HermesTranslator("r")
        out = t.start("thread", "agui-run", held, replay=True)
        for seq, event in enumerate(events, start=start):
            out += t.feed({**event, "seq": seq})
        return [(e.tool_call_id, e.content) for e in out if isinstance(e, ToolCallResultEvent)]

    first = ToolMessage(id="r:tool:0:result", tool_call_id="r:tool:0", content="first")
    # The backlog opens on A's completion; B starts later, inside the backlog.
    later: list[Message] = [UserMessage(id="u1", content="Hi"), AssistantMessage(id="r:msg:0", tool_calls=[call(0), call(3)]), first]
    assert replay(
        later,
        [
            {"event": "tool.completed", "tool": "a", "preview": "first"},
            {"event": "tool.started", "tool": "a"},
            {"event": "tool.completed", "tool": "a", "preview": "second"},
        ],
        start=2,
    ) == [("r:tool:3", "second")]
    # B started before the backlog: without ids its completion cannot be told from A's.
    earlier: list[Message] = [later[0], AssistantMessage(id="r:msg:0", tool_calls=[call(0), call(1)]), first]
    assert replay(earlier, [{"event": "tool.completed", "tool": "a", "preview": "second"}], start=2) == []


def test_completions_pair_with_their_start_by_id() -> None:
    events = feed_all(
        HermesTranslator("run_1"),
        [
            {"event": "tool.started", "tool": "light", "preview": "kitchen", "id": "c1"},
            {"event": "tool.started", "tool": "light", "preview": "hall", "tool_call_id": "c2"},
            {"event": "tool.completed", "tool": "light", "preview": "hall on", "tool_call_id": "c2"},
            {"event": "tool.completed", "tool": "light", "preview": "kitchen on", "id": "c1"},
        ],
    )
    starts = {e.metadata["preview"]: e.tool_call_id for e in events if isinstance(e, ToolCallStartEvent) and e.metadata}
    results = {e.content: e.tool_call_id for e in events if isinstance(e, ToolCallResultEvent)}
    assert results == {"hall on": starts["hall"], "kitchen on": starts["kitchen"]}


def test_evicted_completions_pair_by_id_and_never_by_guess() -> None:
    def results(starts: list[dict[str, Any]], completions: list[dict[str, Any]]) -> list[tuple[str, Any]]:
        shown = HermesTranslator("r")
        shown.start("thread", "agui-run")
        for seq, event in enumerate(starts):
            shown.feed({**event, "event": "tool.started", "tool": "a", "seq": seq})
        t = HermesTranslator("r")
        out = t.start("thread", "agui-run", [UserMessage(id="u1", content="Hi"), *shown.messages()], replay=True)
        for seq, event in enumerate(completions, start=5):  # both starts were evicted
            out += t.feed({**event, "event": "tool.completed", "tool": "a", "seq": seq})
        return [(e.tool_call_id, e.content) for e in out if isinstance(e, ToolCallResultEvent)]

    assert results([{"id": "c1"}, {"id": "c2"}], [{"id": "c2", "preview": "two"}, {"id": "c1", "preview": "one"}]) == [
        ("r:tool:1:id:c2", "two"),
        ("r:tool:0:id:c1", "one"),
    ]
    assert results([{"id": "c1"}, {"id": "c2"}], [{"id": "c1", "preview": "one"}, {"id": "c1", "preview": "again"}]) == [("r:tool:0:id:c1", "one")]
    assert results([{}], [{"preview": "one"}]) == []  # without ids nothing ties the completion to the call


def secret(tool: str, preview: str, _event: object) -> tuple[str, str] | None:
    return None if preview == "secret" else (tool, preview)


@pytest.mark.parametrize(
    ("run", "held", "backlog"),
    [
        # An identified completion whose start was evicted, while an anonymous call is open.
        ([{"event": "tool.started", "id": "c1"}, {"event": "tool.started"}, {"event": "tool.completed", "id": "c1", "preview": "one"}], 2, 1),
        # An anonymous completion of a hidden call, before a held call with an id starts in the backlog.
        ([{"event": "tool.started", "preview": "secret"}, {"event": "tool.completed", "preview": "one"}, {"event": "tool.started", "id": "c2"}], 3, 1),
        # Hermes reuses the id of a call whose start was evicted.
        (
            [
                {"event": "tool.started", "id": "c1"},
                {"event": "tool.completed", "id": "c1", "preview": "one"},
                {"event": "tool.started", "id": "c1"},
                {"event": "tool.completed", "id": "c1", "preview": "two"},
            ],
            3,
            2,
        ),
        # The visible call's result and the start of a later hidden call of the same tool were evicted.
        (
            [
                {"event": "tool.started"},
                {"event": "tool.completed", "preview": "one"},
                {"event": "tool.started", "preview": "secret"},
                {"event": "tool.completed", "preview": "hidden"},
            ],
            1,
            3,
        ),
    ],
)
def test_a_truncated_replay_puts_results_where_the_live_run_did(run: list[dict[str, Any]], held: int, backlog: int) -> None:
    def results(events: list[BaseEvent]) -> list[tuple[str, Any]]:
        return [(e.tool_call_id, e.content) for e in events if isinstance(e, ToolCallResultEvent)]

    live, replay = HermesTranslator("r", tool_filter=secret), HermesTranslator("r", tool_filter=secret)
    live.start("thread", "agui-run")
    after: list[BaseEvent] = []
    for seq, event in enumerate(run):
        out = live.feed({"tool": "a", **event, "seq": seq})
        after += out if seq >= backlog else []
        if seq == held - 1:
            client = live.messages()
    out = replay.start("thread", "agui-run", [UserMessage(id="u1", content="Hi"), *client], replay=True)
    for seq in range(backlog, len(run)):
        out += replay.feed({"tool": "a", **run[seq], "seq": seq})
    assert results(out) == results(after)
