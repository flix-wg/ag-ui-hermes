import asyncio
import json
from collections.abc import AsyncIterator
from typing import Any

import httpx
from ag_ui.core import (
    AssistantMessage,
    BaseEvent,
    Message,
    MessagesSnapshotEvent,
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

from ag_ui_hermes import (
    HermesAgent, HermesRunsClient, HermesTranslator, InMemoryThreadStore, ThreadRecord, approval_choice,
)
from ag_ui_hermes.endpoint import add_hermes_fastapi_endpoint

from . import connect_cases
from .fake_hermes import FakeHermes, Step


class SlowStore(InMemoryThreadStore):  # a shared store: reads and writes interleave
    async def get(self, thread_id: str) -> ThreadRecord | None:
        record = await super().get(thread_id)
        await asyncio.sleep(0)
        return record


def make_agent(fake: FakeHermes) -> HermesAgent:
    http = httpx.AsyncClient(transport=fake.transport())
    return HermesAgent(HermesRunsClient("http://hermes", "secret", http=http), poll_interval=0.01)


def agent_input(run_id: str = "r1", messages: list[Message] | None = None, **kwargs: Any) -> RunAgentInput:
    messages = messages or [UserMessage(id="u1", content="Hi")]
    return RunAgentInput(thread_id="t1", run_id=run_id, messages=messages, **kwargs)


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

    first: list[BaseEvent] = []
    stream = agent.run(agent_input())
    async for event in stream:
        first.append(event)
        if isinstance(event, RunFinishedEvent):
            break
    await stream.aclose()  # the client leaves as soon as it sees the interrupt
    finished = first[-1]
    assert isinstance(finished, RunFinishedEvent) and finished.outcome
    assert finished.outcome.type == "interrupt"
    interrupt = finished.outcome.interrupts[0]
    tool_call = next(e for e in first if isinstance(e, ToolCallStartEvent))
    assert (interrupt.id, interrupt.reason, interrupt.tool_call_id) == ("req-1", "tool_call", tool_call.tool_call_id)
    assert interrupt.metadata and interrupt.metadata["choices"] == ["once", "session", "always", "deny"]

    always = [ResumeEntry(interrupt_id="req-1", status="resolved", payload={"choice": "always"})]
    rejected = await collect(agent.run(agent_input("r2", resume=always)))
    assert [e.type.value for e in rejected] == ["RUN_STARTED", "RUN_ERROR"]
    assert approval_choice(
        ResumeEntry(interrupt_id="req-1", status="resolved", payload={"choice": "always", "approved": False})
    ) == "deny"

    resume = [
        ResumeEntry(interrupt_id="req-1", status="resolved", payload={"approved": True}),
        ResumeEntry(interrupt_id="req-1", status="resolved", payload="once"),
        ResumeEntry(interrupt_id="req-stale", status="resolved", payload={"approved": True}),
    ]
    second = await collect(agent.run(agent_input("r3", resume=resume)))
    approvals = [c for c in fake.calls if c[1].endswith("/approval")]
    assert approvals == [("POST", "/v1/runs/run_1/approval", {"request_id": "req-1", "choice": "once"})]
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
    assert first_id is not None
    # The client shows part of the first delta: a snapshot drops it, the replay rebuilds it.
    held: list[Message] = [UserMessage(id="u1", content="Hi"), AssistantMessage(id=first_id, content="Hel")]
    rejoined = await collect(agent.connect(agent_input("r2", held)))
    reset = rejoined[1]
    assert isinstance(reset, MessagesSnapshotEvent) and [m.id for m in reset.messages] == ["u1"]
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


async def test_retries_rejoin_their_own_run_even_after_it_was_replaced() -> None:
    fake = FakeHermes([{"event": "message.delta", "delta": "Working"}, asyncio.Event()],
                      [{"event": "message.delta", "delta": "Newer"}, asyncio.Event()])
    agent = make_agent(fake)
    first = asyncio.create_task(collect(agent.run(agent_input())))
    while not fake.runs or not fake.runs["run_1"].backlog:
        await asyncio.sleep(0.01)

    retry = agent.run(agent_input())  # same AG-UI run id, e.g. a transport retry
    async for event in retry:
        if isinstance(event, TextMessageContentEvent):
            break
    await retry.aclose()
    assert [c[:2] for c in fake.calls if c[0] == "POST"] == [("POST", "/v1/runs")]

    second = asyncio.create_task(collect(agent.run(agent_input("r2"))))  # replaces run_1
    while "run_2" not in fake.runs or not fake.runs["run_2"].backlog:
        await asyncio.sleep(0.01)
    late = await collect(agent.run(agent_input()))  # a retry of r1 that arrives after r2
    assert text(late) == "Working"
    assert isinstance(late[-1], RunFinishedEvent) and late[-1].outcome
    assert late[-1].outcome.type == "cancelled"
    assert [c[:2] for c in fake.calls if c[0] == "POST"] == [
        ("POST", "/v1/runs"), ("POST", "/v1/runs/run_1/stop"), ("POST", "/v1/runs")
    ]

    assert text(await first) == "Working"
    record = await agent.store.get("t1")  # the finished run_1 did not release run_2
    assert record and record.run_id == "run_2" and record.agui_run_ids == ["r2"]
    second.cancel()


async def test_concurrent_starts_keep_both_retry_mappings() -> None:
    fake = FakeHermes([asyncio.Event()], [asyncio.Event()])
    agent = make_agent(fake)
    agent.store = SlowStore()
    runs = [asyncio.create_task(collect(agent.run(agent_input(r)))) for r in ("r1", "r2")]
    async with asyncio.timeout(2):
        while not any(c[1].endswith("/stop") for c in fake.calls):
            await asyncio.sleep(0.01)
    record = await agent.store.get("t1")
    assert record and record.started == {"r1": "run_1", "r2": "run_2"}
    stopped = next(c[1] for c in fake.calls if c[1].endswith("/stop"))
    assert stopped != f"/v1/runs/{record.run_id}/stop"  # the run that lost the record was stopped
    for task in runs:
        task.cancel()


async def test_suspend_never_restores_an_answered_or_older_approval() -> None:
    agent = make_agent(FakeHermes())
    t = HermesTranslator("run_1")
    t.start("t1", "r1")
    t.feed({"event": "approval.request", "request_id": "A", "seq": 2})
    for cursor, pending in [(5, ["B"]), (2, [])]:  # B persisted later / A answered by another client
        await agent.store.put_if_run("t1", ThreadRecord(run_id="run_1", cursor=cursor, pending_interrupts=pending), None)
        await agent._suspend("t1", t, 2)
        record = await agent.store.get("t1")
        assert record and (record.cursor, record.pending_interrupts) == (cursor, pending)
        agent.store = InMemoryThreadStore()


async def test_answered_approval_at_seq_0_does_not_look_like_truncation() -> None:
    gate = asyncio.Event()
    fake = FakeHermes([
        {"event": "approval.request", "request_id": "req-1", "command": "ls"},
        {"event": "message.delta", "delta": "Hel"},
        gate,
        {"event": "message.delta", "delta": "lo"},
        {"event": "run.completed", "output": "Hello", "completed": True},
    ])
    agent = make_agent(fake)
    await collect(agent.run(agent_input()))
    resume = [ResumeEntry(interrupt_id="req-1", status="resolved", payload="once")]
    stream = agent.run(agent_input("r2", resume=resume))
    message_id = None
    async for event in stream:
        if isinstance(event, TextMessageContentEvent):
            message_id = event.message_id
            break
    await stream.aclose()
    gate.set()
    assert message_id is not None
    held: list[Message] = [UserMessage(id="u1", content="Hi"), AssistantMessage(id=message_id, content="Hel")]
    rejoined = await collect(agent.connect(agent_input("r3", held)))
    assert isinstance(rejoined[1], MessagesSnapshotEvent) and [m.id for m in rejoined[1].messages] == ["u1"]
    assert text(rejoined) == "Hello"


async def test_concurrent_rejoins_both_own_the_run() -> None:
    fake = FakeHermes([{"event": "message.delta", "delta": "Working"}, asyncio.Event()])
    agent = make_agent(fake)
    agent.store = SlowStore()
    first = asyncio.create_task(collect(agent.run(agent_input())))
    while not fake.runs or not fake.runs["run_1"].backlog:
        await asyncio.sleep(0.01)

    async def rejoin(run_id: str) -> None:
        stream = agent.connect(agent_input(run_id))
        async for event in stream:
            if isinstance(event, TextMessageContentEvent):
                break
        await stream.aclose()

    await asyncio.gather(rejoin("r2"), rejoin("r3"))
    record = await agent.store.get("t1")
    assert record and sorted(record.agui_run_ids) == ["r1", "r2", "r3"]
    first.cancel()


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
        late = await client.post("/agent/stop", json={"threadId": "t1", "runId": "r0"})
        assert late.json() == {"stopped": False}
        unnamed = await client.post("/agent/stop", json={"threadId": "t1"})
        assert unnamed.status_code == 422
        stopped = await client.post("/agent/stop", json={"threadId": "t1", "runId": "r1"})
        response = await run

    assert stopped.json() == {"stopped": True}
    assert '"type":"RUN_STARTED"' in response.text
    assert '"outcome":{"type":"cancelled"}' in response.text


async def test_js_connect_probe_fixture_is_current() -> None:
    assert await connect_cases.build() == json.loads(connect_cases.FIXTURE.read_text())
