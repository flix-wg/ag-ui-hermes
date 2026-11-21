import asyncio
import inspect
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import httpx
from ag_ui.core import (
    AssistantMessage,
    BaseEvent,
    Message,
    MessagesSnapshotEvent,
    ResumeEntry,
    RunAgentInput,
    RunErrorEvent,
    RunFinishedEvent,
    TextMessageContentEvent,
    TextMessageStartEvent,
    ToolCallResultEvent,
    ToolCallStartEvent,
    UserMessage,
)
from fastapi import FastAPI

from ag_ui_hermes import (
    HermesAgent,
    HermesRunsClient,
    HermesTranslator,
    HookContext,
    InMemoryThreadStore,
    ThreadRecord,
)
from ag_ui_hermes.agent import _approval_choice
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


async def until(check: Callable[[], object]) -> None:
    """Wait for ``check`` (sync or async) to hold."""
    async with asyncio.timeout(2):
        while True:
            result = check()
            if await result if inspect.isawaitable(result) else result:
                return
            await asyncio.sleep(0.01)


def streaming(fake: FakeHermes, run_id: str = "run_1") -> Callable[[], bool]:
    return lambda: run_id in fake.runs and bool(fake.runs[run_id].backlog)


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
    assert isinstance(finished, RunFinishedEvent)
    assert finished.outcome
    assert finished.outcome.type == "interrupt"
    interrupt = finished.outcome.interrupts[0]
    tool_call = next(e for e in first if isinstance(e, ToolCallStartEvent))
    assert (interrupt.id, interrupt.reason, interrupt.tool_call_id) == ("req-1", "tool_call", tool_call.tool_call_id)
    assert interrupt.metadata
    assert interrupt.metadata["choices"] == ["once", "session", "always", "deny"]

    always = [ResumeEntry(interrupt_id="req-1", status="resolved", payload={"choice": "always"})]
    rejected = await collect(agent.run(agent_input("r2", resume=always)))
    assert [e.type.value for e in rejected] == ["RUN_STARTED", "RUN_ERROR"]
    assert _approval_choice(
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
    assert text(first) == "Running it."
    assert text(second) == "Deleted."
    assert isinstance(second[-1], RunFinishedEvent)
    assert second[-1].outcome
    assert second[-1].outcome.type == "success"


async def test_resume_that_leaves_an_approval_open_interrupts_again() -> None:
    fake = FakeHermes([
        {"event": "approval.request", "request_id": "req-1", "command": "ls"},
        {"event": "run.completed", "output": "done", "completed": True},
    ])
    agent = make_agent(fake)
    await collect(agent.run(agent_input()))
    stale = [ResumeEntry(interrupt_id="req-old", status="resolved", payload="once")]
    again = await asyncio.wait_for(collect(agent.run(agent_input("r2", resume=stale))), 2)
    finished = again[-1]
    assert isinstance(finished, RunFinishedEvent)
    assert finished.outcome
    assert finished.outcome.type == "interrupt"
    assert [i.id for i in finished.outcome.interrupts] == ["req-1"]
    assert not any(path.endswith("/approval") for _, path, _ in fake.calls)
    resume = [ResumeEntry(interrupt_id="req-1", status="resolved", payload="once")]
    assert text(await collect(agent.run(agent_input("r3", resume=resume)))) == "done"


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
    assert isinstance(reset, MessagesSnapshotEvent)
    assert [m.id for m in reset.messages] == ["u1"]
    starts = [e for e in rejoined if isinstance(e, TextMessageStartEvent)]
    assert [s.message_id for s in starts] == [first_id]
    assert text(rejoined) == "Hello again"
    assert rejoined[0].type.value == "RUN_STARTED"
    assert rejoined[-1].type.value == "RUN_FINISHED"

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
    await until(streaming(fake))

    retry = agent.run(agent_input())  # same AG-UI run id, e.g. a transport retry
    async for event in retry:
        if isinstance(event, TextMessageContentEvent):
            break
    await retry.aclose()
    assert [c[:2] for c in fake.calls if c[0] == "POST"] == [("POST", "/v1/runs")]

    second = asyncio.create_task(collect(agent.run(agent_input("r2"))))  # replaces run_1
    await until(streaming(fake, "run_2"))
    late = await collect(agent.run(agent_input()))  # a retry of r1 that arrives after r2
    assert text(late) == "Working"
    assert isinstance(late[-1], RunFinishedEvent)
    assert late[-1].outcome
    assert late[-1].outcome.type == "cancelled"
    assert [c[:2] for c in fake.calls if c[0] == "POST"] == [
        ("POST", "/v1/runs"), ("POST", "/v1/runs/run_1/stop"), ("POST", "/v1/runs")
    ]

    assert text(await first) == "Working"
    record = await agent.store.get("t1")  # the finished run_1 did not release run_2
    assert record
    assert record.run_id == "run_2"
    assert record.agui_run_ids == ["r2"]
    second.cancel()


async def test_concurrent_starts_keep_both_retry_mappings() -> None:
    fake = FakeHermes([asyncio.Event()], [asyncio.Event()])
    agent = make_agent(fake)
    agent.store = SlowStore()
    runs = [asyncio.create_task(collect(agent.run(agent_input(r)))) for r in ("r1", "r2")]
    await until(lambda: any(c[1].endswith("/stop") for c in fake.calls))
    record = await agent.store.get("t1")
    assert record
    assert record.started == {"r1": "run_1", "r2": "run_2"}
    stopped = next(c[1] for c in fake.calls if c[1].endswith("/stop"))
    assert stopped != f"/v1/runs/{record.run_id}/stop"  # the run that lost the record was stopped
    for task in runs:
        task.cancel()


async def test_suspend_never_restores_an_answered_or_older_approval() -> None:
    agent = make_agent(FakeHermes())
    t = HermesTranslator("run_1")
    t.start("t1", "r1")
    t.feed({"event": "approval.request", "request_id": "A", "seq": 2})
    # B persisted later / A answered by another client
    for cursor, pending, answered in [(5, ["B"], []), (2, [], ["A"])]:
        stale = ThreadRecord(run_id="run_1", cursor=cursor, pending_interrupts=pending, answered_interrupts=answered)
        await agent.store.put_if_run("t1", stale, None)
        assert await agent._suspend("t1", t, 2) is False
        record = await agent.store.get("t1")
        assert record
        assert (record.cursor, record.pending_interrupts) == (cursor, pending)
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
    assert isinstance(rejoined[1], MessagesSnapshotEvent)
    assert [m.id for m in rejoined[1].messages] == ["u1"]
    assert text(rejoined) == "Hello"


async def test_concurrent_rejoins_both_own_the_run() -> None:
    fake = FakeHermes([{"event": "message.delta", "delta": "Working"}, asyncio.Event()])
    agent = make_agent(fake)
    agent.store = SlowStore()
    first = asyncio.create_task(collect(agent.run(agent_input())))
    await until(streaming(fake))

    async def rejoin(run_id: str) -> None:
        stream = agent.connect(agent_input(run_id))
        async for event in stream:
            if isinstance(event, TextMessageContentEvent):
                break
        await stream.aclose()

    await asyncio.gather(rejoin("r2"), rejoin("r3"))
    record = await agent.store.get("t1")
    assert record
    assert sorted(record.agui_run_ids) == ["r1", "r2", "r3"]
    first.cancel()


async def test_stop_endpoint_cancels_the_run() -> None:
    gate = asyncio.Event()
    fake = FakeHermes([{"event": "message.delta", "delta": "Working"}, gate])
    app = FastAPI()
    add_hermes_fastapi_endpoint(app, make_agent(fake), "/agent")
    body = agent_input().model_dump(mode="json", by_alias=True)

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://app") as client:
        run = asyncio.create_task(client.post("/agent", json=body))
        await until(streaming(fake))
        late = await client.post("/agent/stop", json={"threadId": "t1", "runId": "r0"})
        assert late.json() == {"stopped": False}
        unnamed = await client.post("/agent/stop", json={"threadId": "t1"})
        assert unnamed.status_code == 422
        stopped = await client.post("/agent/stop", json={"threadId": "t1", "runId": "r1"})
        response = await run

    assert stopped.json() == {"stopped": True}
    assert '"type":"RUN_STARTED"' in response.text
    assert '"outcome":{"type":"cancelled"}' in response.text


async def test_endpoint_authorizes_and_validates_before_touching_the_run() -> None:
    gate = asyncio.Event()
    fake = FakeHermes([{"event": "message.delta", "delta": "Working"}, gate,
                       {"event": "run.completed", "output": "Working", "completed": True}])
    seen: list[tuple[str, str | None]] = []

    async def authorize(ctx: HookContext) -> bool:
        user = ctx.request.headers.get("x-user") if ctx.request else None
        seen.append((ctx.action, user))
        return user == "felix"

    agent = make_agent(fake)
    agent.authorize = authorize
    app = FastAPI()
    add_hermes_fastapi_endpoint(app, agent, "/agent", tags=["agent"])
    assert sum(getattr(route, "tags", None) == ["agent"] for route in app.routes) == 3
    owner, eve = {"x-user": "felix"}, {"x-user": "eve"}
    body = agent_input().model_dump(mode="json", by_alias=True)
    no_text = agent_input("r2", [AssistantMessage(id="a1", content="?")]).model_dump(mode="json", by_alias=True)

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://app") as client:
        run = asyncio.create_task(client.post("/agent", json=body, headers=owner))
        await until(streaming(fake))
        for route, payload in [("/agent", body), ("/agent/connect", body), ("/agent/stop", {"threadId": "t1", "runId": "r1"})]:
            denied = await client.post(route, json=payload, headers=eve)
            assert denied.status_code == 403
            assert denied.json()["error"]["code"] == "forbidden"
        invalid = await client.post("/agent", json=no_text, headers=owner)
        assert invalid.status_code == 400
        assert invalid.json()["error"]["code"] == "invalid_input"
        assert not any(path.endswith("/stop") for _, path, _ in fake.calls)  # validated before stopping run_1
        rejoin = asyncio.create_task(
            client.post("/agent/connect", json=agent_input("r3").model_dump(mode="json", by_alias=True), headers=owner)
        )
        await asyncio.sleep(0.05)
        gate.set()
        response, rejoined = await run, await rejoin

    for text_ in (response.text, rejoined.text):
        assert "Working" in text_
        assert '"outcome":{"type":"success"}' in text_
    assert {("run", "eve"), ("connect", "eve"), ("stop", "eve"), ("connect", "felix")} <= set(seen)
    refused = await collect(agent.run(agent_input("r4")))
    assert isinstance(refused[-1], RunErrorEvent)
    assert refused[-1].code == "forbidden"


class IdempotentHermes(FakeHermes):
    """Replays starts by Idempotency-Key and can hold the nth start's response."""

    def __init__(self, *scripts: list[Step]) -> None:
        super().__init__(*scripts)
        self.keys: dict[str, str] = {}
        self.holds = {1: asyncio.Event(), 2: asyncio.Event()}
        self.posts = 0

    async def handle(self, request: httpx.Request) -> httpx.Response:
        if request.method != "POST" or request.url.path != "/v1/runs":
            return await super().handle(request)
        self.posts += 1
        n, key = self.posts, request.headers.get("idempotency-key", "")
        if key in self.keys:
            response = httpx.Response(202, json={"run_id": self.keys[key], "status": "started", "replayed": True})
        else:
            response = await super().handle(request)
            self.keys[key] = response.json()["run_id"]
        if n in self.holds:
            await self.holds[n].wait()
        return response


async def test_overlapping_retry_never_displaces_a_newer_run() -> None:
    fake = IdempotentHermes([asyncio.Event()], [asyncio.Event()])
    agent = make_agent(fake)

    async def owner(run: str) -> bool:
        record = await agent.store.get("t1")
        return record is not None and record.run_id == run

    original = asyncio.create_task(collect(agent.run(agent_input())))
    retry = asyncio.create_task(collect(agent.run(agent_input())))  # its POST answers after r2 took over
    await until(lambda: fake.posts == 2)
    fake.holds[1].set()
    await until(lambda: owner("run_1"))
    newer = asyncio.create_task(collect(agent.run(agent_input("r2"))))
    await until(lambda: owner("run_2"))
    fake.holds[2].set()
    late = await asyncio.wait_for(retry, 2)
    assert isinstance(late[-1], RunFinishedEvent)
    assert late[-1].outcome
    assert late[-1].outcome.type == "cancelled"
    assert await owner("run_2")
    assert not any(path == "/v1/runs/run_2/stop" for _, path, _ in fake.calls)
    for task in (original, newer):
        task.cancel()


async def test_the_run_that_wins_a_fresh_thread_keeps_its_own_session() -> None:
    fake = IdempotentHermes([asyncio.Event()], [asyncio.Event()])
    agent = make_agent(fake)

    async def session(value: str) -> bool:
        record = await agent.store.get("t1")
        return record is not None and record.session_id == value

    tasks = [asyncio.create_task(collect(agent.run(agent_input(r)))) for r in ("r1", "r2")]
    await until(lambda: fake.posts == 2)
    fake.holds[1].set()
    await until(lambda: session("run_1"))
    fake.holds[2].set()  # r2 claims the thread after r1 saved its session
    await until(lambda: session("run_2"))
    for task in tasks:
        task.cancel()


class StreamGoneHermes(FakeHermes):
    async def handle(self, request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/events"):
            return self._error(404, "run_not_found")
        return await super().handle(request)


async def test_polled_approval_can_be_answered() -> None:
    fake = StreamGoneHermes([
        {"event": "approval.request", "request_id": "req-1", "command": "ls"},
        {"event": "run.completed", "output": "done", "completed": True},
    ])
    agent = make_agent(fake)
    first = await collect(agent.run(agent_input()))
    assert isinstance(first[-1], RunFinishedEvent)
    assert first[-1].outcome
    assert first[-1].outcome.type == "interrupt"
    resume = [ResumeEntry(interrupt_id="req-1", status="resolved", payload="once")]
    second = await collect(agent.run(agent_input("r2", resume=resume)))
    assert [path for _, path, _ in fake.calls if path.endswith("/approval")] == ["/v1/runs/run_1/approval"]
    assert text(second) == "done"


class RacingClient(HermesRunsClient):
    hook: Callable[[], Awaitable[None]] | None = None

    async def get_run(self, run_id: str) -> dict[str, Any]:
        status = await super().get_run(run_id)
        if self.hook is not None:
            hook, self.hook = self.hook, None
            await hook()
        return status


async def test_rejoin_withdraws_an_approval_another_client_answered() -> None:
    fake = FakeHermes([
        {"event": "approval.request", "request_id": "A", "command": "ls"},
        {"event": "message.delta", "delta": "one"},
        {"event": "approval.request", "request_id": "B", "command": "rm"},
        {"event": "run.completed", "output": "one", "completed": True},
    ])
    client = RacingClient("http://hermes", "secret", http=httpx.AsyncClient(transport=fake.transport()))
    agent = HermesAgent(client, poll_interval=0.01)
    await collect(agent.run(agent_input()))

    async def other_client_answers_a() -> None:  # after the rejoin found A still pending
        resume = [ResumeEntry(interrupt_id="A", status="resolved", payload="once")]
        await collect(agent.run(agent_input("r3", resume=resume)))

    client.hook = other_client_answers_a
    rejoined = await collect(agent.connect(agent_input("r2")))
    last = rejoined[-1]
    assert isinstance(last, RunFinishedEvent)
    assert last.outcome
    assert last.outcome.type == "interrupt"
    assert [i.id for i in last.outcome.interrupts] == ["B"]


async def test_js_connect_probe_fixture_is_current() -> None:
    assert await connect_cases.build() == json.loads(connect_cases.FIXTURE.read_text())
