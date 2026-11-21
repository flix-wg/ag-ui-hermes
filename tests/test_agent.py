import asyncio
from collections.abc import AsyncIterator
from typing import Any

import httpx
from ag_ui.core import (
    BaseEvent,
    ResumeEntry,
    RunAgentInput,
    RunFinishedEvent,
    TextMessageContentEvent,
    TextMessageStartEvent,
    ToolCallResultEvent,
    ToolCallStartEvent,
    UserMessage,
)
from fastapi import FastAPI

from ag_ui_hermes import HermesAgent, HermesRunsClient
from ag_ui_hermes.endpoint import add_hermes_fastapi_endpoint

from .fake_hermes import FakeHermes, Step


def make_agent(fake: FakeHermes) -> HermesAgent:
    http = httpx.AsyncClient(transport=fake.transport())
    return HermesAgent(HermesRunsClient("http://hermes", "secret", http=http), poll_interval=0.01)


def agent_input(run_id: str = "r1", **kwargs: Any) -> RunAgentInput:
    return RunAgentInput(
        thread_id="t1", run_id=run_id, messages=[UserMessage(id="u1", content="Hi")], **kwargs
    )


async def collect(events: AsyncIterator[BaseEvent]) -> list[BaseEvent]:
    return [event async for event in events]


def text(events: list[BaseEvent]) -> str:
    return "".join(e.delta for e in events if isinstance(e, TextMessageContentEvent))


async def test_approval_interrupts_and_resume_continues_the_same_run() -> None:
    script: list[Step] = [
        {"event": "message.delta", "delta": "Running it."},
        {"event": "tool.started", "tool": "terminal", "preview": "rm -rf build"},
        {"event": "approval.request", "request_id": "req-1", "command": "rm -rf build",
         "description": "recursive delete", "choices": ["once", "session", "always", "deny"]},
        {"event": "tool.completed", "tool": "terminal", "duration": 1.0, "error": False, "preview": "ok"},
        {"event": "message.delta", "delta": "Deleted."},
        {"event": "run.completed", "output": "Deleted.", "completed": True},
    ]
    fake = FakeHermes(script)
    agent = make_agent(fake)

    first = await collect(agent.run(agent_input()))
    finished = first[-1]
    assert isinstance(finished, RunFinishedEvent) and finished.outcome
    assert finished.outcome.type == "interrupt"
    interrupt = finished.outcome.interrupts[0]
    tool_call = next(e for e in first if isinstance(e, ToolCallStartEvent))
    assert (interrupt.id, interrupt.reason, interrupt.tool_call_id) == ("req-1", "tool_call", tool_call.tool_call_id)
    assert interrupt.metadata and interrupt.metadata["choices"] == ["once", "session", "always", "deny"]

    resume = [ResumeEntry(interrupt_id="req-1", status="resolved", payload={"approved": True})]
    second = await collect(agent.run(agent_input("r2", resume=resume)))
    assert ("POST", "/v1/runs/run_1/approval", {"request_id": "req-1", "choice": "once"}) in fake.calls
    assert len([c for c in fake.calls if c[:2] == ("POST", "/v1/runs")]) == 1
    result = next(e for e in second if isinstance(e, ToolCallResultEvent))
    assert result.tool_call_id == tool_call.tool_call_id
    assert text(first) == "Running it." and text(second) == "Deleted."
    assert isinstance(second[-1], RunFinishedEvent) and second[-1].outcome
    assert second[-1].outcome.type == "success"


async def test_disconnect_keeps_the_run_and_connect_replays_without_duplicates() -> None:
    gate = asyncio.Event()
    fake = FakeHermes([
        {"event": "message.delta", "delta": "Hello "},
        gate,
        {"event": "message.delta", "delta": "again"},
        {"event": "run.completed", "output": "Hello again", "completed": True},
    ])
    agent = make_agent(fake)

    stream = agent.run(agent_input())
    first_id = None
    async for event in stream:
        if isinstance(event, TextMessageContentEvent):
            first_id = event.message_id
            break
    await stream.aclose()  # the browser went away
    assert not any(path.endswith("/stop") for _, path, _ in fake.calls)

    gate.set()
    rejoined = await collect(agent.connect(agent_input("r2")))
    starts = [e for e in rejoined if isinstance(e, TextMessageStartEvent)]
    assert [s.message_id for s in starts] == [first_id]
    assert text(rejoined) == "Hello again"
    assert rejoined[0].type.value == "RUN_STARTED" and rejoined[-1].type.value == "RUN_FINISHED"

    idle = await collect(agent.connect(agent_input("r3")))
    assert [e.type.value for e in idle] == ["RUN_STARTED", "RUN_FINISHED"]


async def test_dropped_stream_resumes_from_last_event_id() -> None:
    fake = FakeHermes(
        [
            {"event": "message.delta", "delta": "one "},
            {"event": "message.delta", "delta": "two"},
            {"event": "run.completed", "output": "one two", "completed": True},
        ],
        drop_once_after=0,
    )
    events = await collect(make_agent(fake).run(agent_input()))
    assert fake.event_cursors == [-1, 0]
    assert text(events) == "one two"
    assert events[-1].type.value == "RUN_FINISHED"


async def test_stop_endpoint_cancels_the_run() -> None:
    gate = asyncio.Event()
    fake = FakeHermes([{"event": "message.delta", "delta": "Working"}, gate])
    app = FastAPI()
    add_hermes_fastapi_endpoint(app, make_agent(fake), "/agent")
    body = agent_input().model_dump(mode="json", by_alias=True)

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://app") as client:
        run = asyncio.create_task(client.post("/agent", json=body))
        while not fake.runs or not fake.runs["run_1"].backlog:
            await asyncio.sleep(0.01)
        stopped = await client.post("/agent/stop", json={"threadId": "t1"})
        response = await run

    assert stopped.json() == {"stopped": True}
    assert '"type":"RUN_STARTED"' in response.text
    assert '"outcome":{"type":"cancelled"}' in response.text
