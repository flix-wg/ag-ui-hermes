"""FastAPI routes for a :class:`HermesAgent` (install the ``fastapi`` extra)."""

from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import aclosing
from typing import Any, TypeVar

from ag_ui.core import BaseEvent, RunAgentInput
from ag_ui.encoder import EventEncoder
from fastapi import APIRouter, FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from .agent import HermesAgent
from .errors import AgentForbiddenError, AgentInputError, HermesError

_Body = TypeVar("_Body", bound=BaseModel)


class _StopRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    thread_id: str = Field(alias="threadId")
    run_id: str = Field(alias="runId")


def add_hermes_fastapi_endpoint(
    app: FastAPI | APIRouter,
    agent: HermesAgent,
    path: str = "/",
    **route_kwargs: Any,
) -> None:
    """Register ``POST path`` (run), ``POST path/connect`` (rejoin), ``POST path/stop`` and
    ``GET path/capabilities`` (AG-UI ``AgentCapabilities``).

    Closing a run or connect response only detaches the client; ``/stop``
    (``{"threadId", "runId"}``) stops the Hermes run if that AG-UI run owns it.
    ``route_kwargs`` (``dependencies``, ``tags``, ...) go to every route. Before any event is
    streamed, a request the agent's ``authorize`` hook refuses gets HTTP 403 and a malformed
    or invalid one HTTP 400, with ``{"error": {"code", "message"}}`` and, for 400, ``reason``.
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
    async def run(request: Request) -> Response:
        try:
            events = await agent.open_run(await _parse(request, RunAgentInput), request=request)
        except (AgentForbiddenError, AgentInputError) as exc:
            return _error(exc)
        return stream(request, events)

    @app.post(f"{base}/connect", **route_kwargs)
    async def connect(request: Request) -> Response:
        try:
            events = await agent.open_connect(await _parse(request, RunAgentInput), request=request)
        except (AgentForbiddenError, AgentInputError) as exc:
            return _error(exc)
        return stream(request, events)

    @app.get(f"{base}/capabilities", **route_kwargs)
    async def capabilities(request: Request) -> Response:
        try:
            caps = await agent.capabilities(request=request)
        except HermesError as exc:
            return _error(exc)
        return JSONResponse(caps.model_dump(mode="json", by_alias=True, exclude_none=True))

    @app.post(f"{base}/stop", **route_kwargs)
    async def stop(request: Request) -> Response:
        try:
            body = await _parse(request, _StopRequest)
            stopped = await agent.stop(body.thread_id, body.run_id, request=request)
        except HermesError as exc:
            return _error(exc)
        return JSONResponse({"stopped": stopped})


async def _parse(request: Request, model: type[_Body]) -> _Body:
    """The JSON body as ``model``; anything else is ``invalid_input`` with reason ``malformed_body``."""
    try:
        return model.model_validate_json(await request.body())
    except ValidationError as exc:
        raise AgentInputError(f"Malformed {model.__name__} body ({exc.error_count()} errors)", "malformed_body") from None


def _error(exc: HermesError) -> JSONResponse:
    status = 403 if isinstance(exc, AgentForbiddenError) else 400 if isinstance(exc, AgentInputError) else 502
    error = {"code": exc.code, "message": exc.public_message}
    if isinstance(exc, AgentInputError):
        error["reason"] = exc.reason
    return JSONResponse({"error": error}, status_code=status)
