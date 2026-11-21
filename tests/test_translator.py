from typing import Any

from ag_ui.core import (
    AssistantMessage,
    BaseEvent,
    CustomEvent,
    MessagesSnapshotEvent,
    RunFinishedEvent,
    TextMessageContentEvent,
    ToolCallResultEvent,
    ToolCallStartEvent,
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
    {"event": "run.completed", "output": "Kitchen is on.", "completed": True},
]


def hide_secrets(tool: str, preview: str, _event: object) -> tuple[str, str] | None:
    return None if tool == "secret_lookup" else (tool, preview)


def test_maps_text_tools_reasoning_and_replays_with_identical_ids() -> None:
    events = feed_all(HermesTranslator("run_1", tool_filter=hide_secrets), RUN)

    assert [e.type.value for e in events] == [
        "RUN_STARTED",
        "TEXT_MESSAGE_START", "TEXT_MESSAGE_CONTENT", "TEXT_MESSAGE_CONTENT", "TEXT_MESSAGE_END",
        "TOOL_CALL_START", "TOOL_CALL_END", "TOOL_CALL_START", "TOOL_CALL_END",
        "REASONING_START", "REASONING_MESSAGE_START", "REASONING_MESSAGE_CONTENT",
        "REASONING_MESSAGE_END", "REASONING_END",
        "TOOL_CALL_RESULT", "TOOL_CALL_RESULT",
        "TEXT_MESSAGE_START", "TEXT_MESSAGE_CONTENT", "TEXT_MESSAGE_END",
        "RUN_FINISHED",
    ]
    starts = [e for e in events if isinstance(e, ToolCallStartEvent)]
    results = [e for e in events if isinstance(e, ToolCallResultEvent)]
    # Hermes sends no tool-call ids: completions pair FIFO per tool name.
    assert [r.tool_call_id for r in results] == [s.tool_call_id for s in starts]
    assert [(r.content, r.metadata and r.metadata["error"]) for r in results] == [
        ("kitchen on", False), ("hall offline", True)
    ]
    assert starts[0].parent_message_id == "run_1:msg:0"
    finished = events[-1]
    assert isinstance(finished, RunFinishedEvent) and finished.outcome
    assert finished.outcome.type == "success"

    replayed = feed_all(HermesTranslator("run_1", tool_filter=hide_secrets), RUN)
    assert [e.model_dump() for e in replayed] == [e.model_dump() for e in events]

    passthrough = feed_all(HermesTranslator("run_1", passthrough_unknown=True), RUN)
    custom = [e for e in passthrough if isinstance(e, CustomEvent)]
    assert [c.name for c in custom] == ["hermes.dashboard.scene"]


def test_canonical_output_fixes_streamed_text() -> None:
    suffix = feed_all(HermesTranslator("r"), [
        {"event": "message.delta", "delta": "Hel"},
        {"event": "run.completed", "output": "Hello"},
    ])
    assert [e.delta for e in suffix if isinstance(e, TextMessageContentEvent)] == ["Hel", "lo"]

    unstreamed = feed_all(HermesTranslator("r"), [
        {"event": "tool.started", "tool": "t"},
        {"event": "tool.completed", "tool": "t", "preview": "x"},
        {"event": "run.completed", "output": "Answer"},
    ])
    final = [e for e in unstreamed if isinstance(e, TextMessageContentEvent)]
    assert [(e.message_id, e.delta) for e in final] == [("r:final", "Answer")]

    rewritten = feed_all(HermesTranslator("r"), [
        {"event": "message.delta", "delta": "Draft answer"},
        {"event": "run.completed", "output": "Final answer"},
    ])
    snapshot = next(e for e in rewritten if isinstance(e, MessagesSnapshotEvent))
    assert [m.id for m in snapshot.messages] == ["u1", "r:msg:0"]
    assert isinstance(snapshot.messages[1], AssistantMessage)
    assert snapshot.messages[1].content == "Final answer"
    assert rewritten[-1].type.value == "RUN_FINISHED"
