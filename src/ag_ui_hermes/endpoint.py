"""FastAPI routes for a :class:`HermesAgent` (install the ``fastapi`` extra)."""

from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import aclosing
from typing import Any

from ag_ui.core import BaseEvent, RunAgentInput
from ag_ui.encoder import EventEncoder
from fastapi import APIRouter, FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from pydantic import BaseModel, ConfigDict, Field

from .agent import AgentForbiddenError, AgentInputError, HermesAgent
from .client import HermesError


class StopRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    thread_id: str = Field(alias="threadId")
    run_id: str = Field(alias="runId")


def add_hermes_fastapi_endpoint(
    app: FastAPI | APIRouter,
    agent: HermesAgent,
    path: str = "/",
    **route_kwargs: Any,
) -> None:
    """Register ``POST path`` (run), ``POST path/connect`` (rejoin) and ``POST path/stop``.

    Closing a run or connect response only detaches the client; ``/stop``
    (``{"threadId", "runId"}``) stops the Hermes run if that AG-UI run owns it.
    ``route_kwargs`` (``dependencies``, ``tags``, ...) go to every route. A request the
    agent's ``authorize`` hook refuses gets HTTP 403 and an invalid run request HTTP 400,
    both with ``{"error": {"code", "message"}}`` and before any event is streamed.
    """
    base = path.rstrip("/")

    def stream(request: Request, events: AsyncGenerator[BaseEvent, None]) -> StreamingResponse:
        encoder = EventEncoder(accept=request.headers.get("accept", ""))

        async def body() -> AsyncIterator[str]:
            async with aclosing(events):
                async for event in events:
                    yield encoder.encode(event)

        return StreamingResponse(body(), media_type=encoder.get_content_type())

    @app.post(path, **route_kwargs)
    async def run(input_data: RunAgentInput, request: Request) -> Response:
        try:
            events = await agent.open_run(input_data, request=request)
        except (AgentForbiddenError, AgentInputError) as exc:
            return _error(exc)
        return stream(request, events)

    @app.post(f"{base}/connect", **route_kwargs)
    async def connect(input_data: RunAgentInput, request: Request) -> Response:
        try:
            events = await agent.open_connect(input_data, request=request)
        except AgentForbiddenError as exc:
            return _error(exc)
        return stream(request, events)

    @app.post(f"{base}/stop", **route_kwargs)
    async def stop(body: StopRequest, request: Request) -> Response:
        try:
            stopped = await agent.stop(body.thread_id, body.run_id, request=request)
        except HermesError as exc:
            return _error(exc)
        return JSONResponse({"stopped": stopped})


def _error(exc: HermesError) -> JSONResponse:
    status = 403 if isinstance(exc, AgentForbiddenError) else 400 if isinstance(exc, AgentInputError) else 502
    return JSONResponse({"error": {"code": exc.code, "message": exc.public_message}}, status_code=status)
