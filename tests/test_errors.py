import asyncio
import logging
from collections.abc import Callable

import httpx
import pytest
from ag_ui.core import RunErrorEvent

from ag_ui_hermes import HermesAgent, HermesRunsClient, HookContext, InMemoryThreadStore, ThreadRecord

from .fake_hermes import FakeHermes
from .test_agent import agent_input, collect, make_agent

SECRET = "hermes.internal:8642"


def unreachable(request: httpx.Request) -> httpx.Response:
    raise httpx.ConnectError(f"connect to http://{SECRET} failed", request=request)


def rejected(request: httpx.Request) -> httpx.Response:
    return httpx.Response(500, json={"error": {"message": f"Traceback at {SECRET}", "code": "boom"}})


def not_json(request: httpx.Request) -> httpx.Response:
    return httpx.Response(200, text=f"<html>{SECRET}</html>")


@pytest.mark.parametrize(
    ("reply", "code", "posts"),
    [(unreachable, "hermes_unreachable", 3), (rejected, "hermes_rejected", 1), (not_json, "hermes_bad_response", 1)],
)
async def test_run_error_hides_the_cause_and_the_log_keeps_it(
    reply: Callable[[httpx.Request], httpx.Response], code: str, posts: int, caplog: pytest.LogCaptureFixture
) -> None:
    calls: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return reply(request)

    errors: list[tuple[HookContext, Exception]] = []
    http = httpx.AsyncClient(transport=httpx.MockTransport(handle))
    agent = HermesAgent(HermesRunsClient(f"http://{SECRET}", "secret", http=http),
                        on_error=lambda ctx, exc: errors.append((ctx, exc)))
    with caplog.at_level(logging.WARNING, logger="ag_ui_hermes"):
        events = await collect(agent.run(agent_input()))

    assert [e.type.value for e in events] == ["RUN_STARTED", "RUN_ERROR"]
    error = events[-1]
    assert isinstance(error, RunErrorEvent)
    assert error.code == code
    assert SECRET not in error.model_dump_json()
    assert len(calls) == posts  # only transport failures retry the idempotent start
    [(ctx, exc)] = errors
    assert ctx.action == "run"
    assert ctx.thread_id == "t1"
    assert str(exc) in caplog.text
    assert code in caplog.text


class BrokenStore(InMemoryThreadStore):
    def __init__(self, refuse: bool) -> None:
        super().__init__()
        self.refuse = refuse

    async def put_if_run(self, thread_id: str, record: ThreadRecord, run_id: str | None) -> bool:
        if self.refuse:
            return False
        raise RuntimeError("store down: password=hunter2")


@pytest.mark.parametrize(("refuse", "code"), [(False, "internal_error"), (True, "store_conflict")])
async def test_store_failures_end_the_stream_with_run_error(
    refuse: bool, code: str, caplog: pytest.LogCaptureFixture
) -> None:
    agent = make_agent(FakeHermes([asyncio.Event()]))
    agent.store = BrokenStore(refuse)
    with caplog.at_level(logging.WARNING, logger="ag_ui_hermes"):
        events = await asyncio.wait_for(collect(agent.run(agent_input())), 2)
    assert [e.type.value for e in events] == ["RUN_STARTED", "RUN_ERROR"]
    error = events[-1]
    assert isinstance(error, RunErrorEvent)
    assert error.code == code
    assert "hunter2" not in error.model_dump_json()
    assert ("hunter2" in caplog.text) is not refuse


async def test_thread_records_round_trip_and_the_memory_store_is_bounded() -> None:
    record = ThreadRecord(session_id="s", run_id="run_1", agui_run_ids=["r1"], started={"r1": "run_1"}, version=3)
    data = record.to_dict()
    assert data["schema_version"] == 1
    assert ThreadRecord.from_dict(data) == record
    with pytest.raises(ValueError, match="schema_version"):
        ThreadRecord.from_dict({**data, "schema_version": 99})

    store = InMemoryThreadStore(max_threads=2)
    await store.put_if_run("busy", ThreadRecord(run_id="run_1"), None)
    for thread in ("idle", "newer"):
        await store.put_if_run(thread, ThreadRecord(), None)
    assert await store.get("idle") is None
    assert await store.get("busy") is not None
