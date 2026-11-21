"""Connect streams for the JS probe (js/src/connect.test.ts): ``python -m tests.connect_cases``."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import httpx
from ag_ui.core import AssistantMessage, Message, RunAgentInput, UserMessage

from ag_ui_hermes import HermesAgent, HermesRunsClient, HermesTranslator

from .fake_hermes import FakeHermes

FIXTURE = Path(__file__).parent.parent / "js" / "src" / "connect-cases.json"

SEGMENT_THEN_LIVE: list[dict[str, Any]] = [
    {"event": "message.delta", "delta": "Let me check."},
    {"event": "tool.started", "tool": "light", "preview": "kitchen"},
    {"event": "tool.completed", "tool": "light", "preview": "on"},
    {"event": "message.delta", "delta": "Hel"},
]
DONE = [{"event": "message.delta", "delta": "lo"}, {"event": "run.completed", "output": "Hello"}]

# name -> (events the client saw before it detached, events after, diverge the first held message)
CASES: dict[str, tuple[list[dict[str, Any]], list[dict[str, Any]], bool]] = {
    "partial": ([{"event": "message.delta", "delta": "Hel"}], DONE, False),
    "segment_then_live": (SEGMENT_THEN_LIVE, DONE, False),
    "held_tools": (
        [
            {"event": "tool.started", "tool": "a", "preview": "first"},
            {"event": "tool.completed", "tool": "a", "preview": "done a"},
            {"event": "tool.started", "tool": "b", "preview": "second"},
        ],
        [
            {"event": "tool.completed", "tool": "b", "preview": "done b"},
            {"event": "message.delta", "delta": "Hello"},
            {"event": "run.completed", "output": "Hello"},
        ],
        False,
    ),
    "early_diverged": (SEGMENT_THEN_LIVE, DONE, True),
}


def _dump(model: Any) -> dict[str, Any]:
    return dict(model.model_dump(mode="json", by_alias=True, exclude_none=True))


async def _case(seen: list[dict[str, Any]], rest: list[dict[str, Any]], diverge: bool) -> dict[str, Any]:
    fake = FakeHermes([*seen, *rest])
    http = httpx.AsyncClient(transport=fake.transport())
    agent = HermesAgent(HermesRunsClient("http://hermes", "secret", http=http))
    user = UserMessage(id="u1", content="Hi")
    stream = agent.run(RunAgentInput(thread_id="t1", run_id="r1", messages=[user], state={}, tools=[],
                                     context=[], forwarded_props={}))
    async for _ in stream:
        break
    await stream.aclose()
    assert fake.runs["run_1"].task is not None
    await fake.runs["run_1"].task

    shown = HermesTranslator("run_1")
    shown.start("t1", "r1")
    for seq, event in enumerate(seen):
        shown.feed({**event, "seq": seq})
    held: list[Message] = [user, *shown.messages()]
    if diverge:
        held[1] = AssistantMessage.model_validate({**held[1].model_dump(), "content": "Draft"})
    input = RunAgentInput(thread_id="t1", run_id="r2", messages=held, state={}, tools=[], context=[],
                          forwarded_props={})
    events = [_dump(e) async for e in agent.connect(input)]
    return {"held": [_dump(m) for m in held], "events": events}


async def build() -> dict[str, Any]:
    return {name: await _case(*case) for name, case in CASES.items()}


if __name__ == "__main__":
    FIXTURE.write_text(json.dumps(asyncio.run(build()), indent=1) + "\n")
