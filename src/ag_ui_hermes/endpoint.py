"""FastAPI routes for a :class:`HermesAgent` (install the ``fastapi`` extra)."""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import Any

from ag_ui.core import BaseEvent, RunAgentInput
from ag_ui.encoder import EventEncoder
from fastapi import APIRouter, FastAPI, Request
from fastapi.params import Depends
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ConfigDict, Field

from .agent import HermesAgent


class StopRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    thread_id: str = Field(alias="threadId")
    run_id: str | None = Field(default=None, alias="runId")


def add_hermes_fastapi_endpoint(
    app: FastAPI | APIRouter,
    agent: HermesAgent,
    path: str = "/",
    *,
    dependencies: Sequence[Depends] | None = None,
) -> None:
    """Register ``POST path`` (run), ``POST path/connect`` (rejoin) and ``POST path/stop``.

    Closing a run or connect response only detaches the client; ``/stop``
    (``{"threadId", "runId"}``) stops the Hermes run if that AG-UI run owns it. Authentication is the host's job, for example through ``dependencies``.
    """
    base = path.rstrip("/")
    deps = list(dependencies or [])

    def stream(request: Request, events: AsyncIterator[BaseEvent]) -> StreamingResponse:
        encoder = EventEncoder(accept=request.headers.get("accept", ""))

        async def body() -> AsyncIterator[str]:
            async for event in events:
                yield encoder.encode(event)

        return StreamingResponse(body(), media_type=encoder.get_content_type())

    @app.post(path, dependencies=deps)
    async def run(input_data: RunAgentInput, request: Request) -> StreamingResponse:
        return stream(request, agent.run(input_data))

    @app.post(f"{base}/connect", dependencies=deps)
    async def connect(input_data: RunAgentInput, request: Request) -> StreamingResponse:
        return stream(request, agent.connect(input_data))

    @app.post(f"{base}/stop", dependencies=deps)
    async def stop(body: StopRequest) -> dict[str, Any]:
        return {"stopped": await agent.stop(body.thread_id, body.run_id)}
