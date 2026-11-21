"""Async client for the Hermes ``/v1/runs`` HTTP API."""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar, Protocol, Self
from urllib.parse import quote

import httpx

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator, Mapping

logger = logging.getLogger("ag_ui_hermes")


class HermesError(Exception):
    """Base error of this package.

    ``code`` and ``public_message`` are what a ``RUN_ERROR`` carries; ``str(exc)`` holds the
    cause and only goes to the log.
    """

    code: ClassVar[str] = "hermes_error"
    public_message: ClassVar[str] = "The Hermes request failed."


class HermesUnreachableError(HermesError):
    """Hermes could not be reached (connection, TLS or timeout failure)."""

    code = "hermes_unreachable"
    public_message = "The Hermes server is unreachable."


class HermesAPIError(HermesError):
    """Hermes answered with an HTTP error status."""

    code = "hermes_rejected"
    public_message = "The Hermes server rejected the request."

    def __init__(self, status: int, message: str, upstream_code: str | None = None) -> None:
        super().__init__(f"Hermes API {status}: {message}")
        self.status, self.message, self.upstream_code = status, message, upstream_code


class HermesResponseError(HermesError):
    """Hermes answered 2xx with a body that is not the expected JSON object."""

    code = "hermes_bad_response"
    public_message = "The Hermes server sent an invalid response."


class RunStreamGoneError(HermesError):
    """The run's event stream is no longer available (finished and swept, or unknown)."""

    code = "hermes_stream_gone"
    public_message = "The Hermes run's event stream is gone."


@dataclass(frozen=True)
class StartedRun:
    run_id: str
    status: str
    replayed: bool


class RunsClient(Protocol):
    """What :class:`HermesAgent` needs from a Hermes runs backend.

    :class:`HermesRunsClient` talks to Hermes directly; a host can back the agent with its
    own bridge instead. Errors follow Hermes: raise :class:`HermesAPIError` with the HTTP
    status (``404`` unknown run, ``409`` approval already answered or not pending),
    :class:`HermesUnreachableError` for transport failures and :class:`HermesResponseError`
    for malformed answers. ``start_run`` gets the user turn as text, or as ``text`` and
    ``image_url`` content parts when it carries images. ``events`` yields Hermes-shaped events (``event``, ``seq``) from
    ``last_seq + 1``, returns when the stream closes and raises :class:`RunStreamGoneError`
    once the run's stream is no longer available, after which the agent polls ``get_run``
    (``status``, ``output``, ``error``, ``approval``) until it is terminal.
    """

    async def start_run(
        self,
        input: str | list[dict[str, Any]],
        *,
        session_id: str | None = None,
        conversation_history: list[dict[str, str]] | None = None,
        idempotency_key: str | None = None,
        extra: Mapping[str, Any] | None = None,
    ) -> StartedRun: ...

    async def get_run(self, run_id: str) -> dict[str, Any]: ...

    async def stop_run(self, run_id: str) -> dict[str, Any]: ...

    async def respond_approval(self, run_id: str, *, request_id: str, choice: str) -> dict[str, Any]: ...

    def events(self, run_id: str, *, last_seq: int = -1) -> AsyncGenerator[dict[str, Any], None]: ...

    # Optional, looked up with getattr so bridges without them keep working:
    # ``async def session_messages(self, session_id: str) -> list[dict[str, Any]]`` (the
    # ``data`` rows of ``GET /api/sessions/{id}/messages``) and
    # ``async def get_capabilities(self) -> dict[str, Any]`` (``GET /v1/capabilities``).


class HermesRunsClient:
    """Talks to ``POST /v1/runs`` and its status, events, stop and approval routes.

    ``base_url`` is the API root without ``/v1`` (for example ``http://127.0.0.1:8642``,
    or a profile prefix such as ``http://host:8642/p/work``). Use it as an async context
    manager or call :meth:`aclose`. ``start_run`` with an idempotency key is retried
    ``start_retries`` times on transport errors.
    """

    def __init__(
        self,
        base_url: str,
        api_key: str | None = None,
        *,
        headers: Mapping[str, str] | None = None,
        http: httpx.AsyncClient | None = None,
        timeout: float = 30.0,
        stream_read_timeout: float = 120.0,
        max_reconnects: int = 5,
        start_retries: int = 2,
    ) -> None:
        self._base = base_url.rstrip("/")
        self._headers = dict(headers or {})
        if api_key:
            self._headers["Authorization"] = f"Bearer {api_key}"
        self._http = http or httpx.AsyncClient(timeout=timeout)
        self._owns_http = http is None
        self._stream_timeout = httpx.Timeout(timeout, read=stream_read_timeout)
        self.max_reconnects = max_reconnects
        self.start_retries = start_retries

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._owns_http:
            await self._http.aclose()

    async def _request(self, method: str, path: str, *, json_body: Any = None, headers: Mapping[str, str] | None = None) -> dict[str, Any]:
        try:
            response = await self._http.request(method, self._base + path, json=json_body, headers={**self._headers, **(headers or {})})
        except httpx.TransportError as exc:
            raise HermesUnreachableError(f"{method} {path}: {exc!r}") from exc
        _raise_for_status(response, response.content)
        try:
            data = response.json()
        except ValueError as exc:
            raise HermesResponseError(f"{method} {path}: {response.status_code} body is not JSON") from exc
        if not isinstance(data, dict):
            raise HermesResponseError(f"{method} {path}: {response.status_code} body is not a JSON object")
        return data

    async def start_run(
        self,
        input: str | list[dict[str, Any]],
        *,
        session_id: str | None = None,
        conversation_history: list[dict[str, str]] | None = None,
        idempotency_key: str | None = None,
        extra: Mapping[str, Any] | None = None,
    ) -> StartedRun:
        user = input if isinstance(input, str) else [{"role": "user", "content": input}]
        body: dict[str, Any] = {**(extra or {}), "input": user}
        if session_id:
            body["session_id"] = session_id
        if conversation_history:
            body["conversation_history"] = conversation_history
        headers = {"Idempotency-Key": idempotency_key} if idempotency_key else None
        data = await self._start(body, headers, retries=self.start_retries if idempotency_key else 0)
        run_id = data.get("run_id")
        if not isinstance(run_id, str) or not run_id:
            raise HermesResponseError("POST /v1/runs: no run_id in the response")
        return StartedRun(run_id, str(data.get("status", "")), bool(data.get("replayed")))

    async def _start(self, body: dict[str, Any], headers: Mapping[str, str] | None, *, retries: int) -> dict[str, Any]:
        for attempt in range(retries):
            try:
                return await self._request("POST", "/v1/runs", json_body=body, headers=headers)
            except HermesUnreachableError as exc:
                logger.warning("Starting a Hermes run failed, retrying (%d/%d): %s", attempt + 1, retries, exc)
                await asyncio.sleep(min(0.25 * 2**attempt, 2.0))
        return await self._request("POST", "/v1/runs", json_body=body, headers=headers)

    async def get_run(self, run_id: str) -> dict[str, Any]:
        return await self._request("GET", f"/v1/runs/{run_id}")

    async def stop_run(self, run_id: str) -> dict[str, Any]:
        return await self._request("POST", f"/v1/runs/{run_id}/stop")

    async def respond_approval(self, run_id: str, *, request_id: str, choice: str) -> dict[str, Any]:
        body = {"request_id": request_id, "choice": choice}
        return await self._request("POST", f"/v1/runs/{run_id}/approval", json_body=body)

    async def session_messages(self, session_id: str) -> list[dict[str, Any]]:
        """The latest (up to 500) stored messages of a Hermes session, oldest first."""
        data = (await self._request("GET", f"/api/sessions/{quote(session_id, safe='')}/messages")).get("data")
        if not isinstance(data, list):
            raise HermesResponseError(f"GET /api/sessions/{session_id}/messages: no data list")
        return [row for row in data if isinstance(row, dict)]

    async def get_capabilities(self) -> dict[str, Any]:
        return await self._request("GET", "/v1/capabilities")

    async def events(self, run_id: str, *, last_seq: int = -1) -> AsyncGenerator[dict[str, Any], None]:
        """Yield run events, reconnecting with ``Last-Event-ID`` until the stream closes.

        Raises :class:`RunStreamGoneError` once the server no longer has the stream.
        """
        failures = 0
        while True:
            headers = {**self._headers, "Accept": "text/event-stream"}
            if last_seq >= 0:
                headers["Last-Event-ID"] = str(last_seq)
            try:
                async with self._http.stream(
                    "GET",
                    f"{self._base}/v1/runs/{run_id}/events",
                    headers=headers,
                    timeout=self._stream_timeout,
                ) as response:
                    if response.status_code == 404:
                        raise RunStreamGoneError(run_id)
                    if response.status_code >= 400:
                        _raise_for_status(response, await response.aread())
                    async for event in _parse_sse(response.aiter_lines()):
                        if event is None:
                            return
                        failures = 0
                        if isinstance(event.get("seq"), int):
                            last_seq = event["seq"]
                        yield event
                failure: str = "closed without the end marker"
            except httpx.TransportError as exc:
                failure = repr(exc)
            failures += 1
            if failures > self.max_reconnects:
                raise RunStreamGoneError(f"{run_id}: stream kept dropping ({failure})")
            logger.info(
                "Event stream of Hermes run %s dropped (%s), reconnecting after seq %d (%d/%d)",
                run_id,
                failure,
                last_seq,
                failures,
                self.max_reconnects,
            )
            await asyncio.sleep(min(0.25 * 2**failures, 5.0))


async def _parse_sse(lines: AsyncIterator[str]) -> AsyncGenerator[dict[str, Any] | None, None]:
    """Parse SSE frames into JSON payloads; ``None`` marks Hermes's ``: stream closed``."""
    data: list[str] = []
    event_id: str | None = None
    async for line in lines:
        if not line:
            if data:
                try:
                    payload = json.loads("\n".join(data))
                except json.JSONDecodeError:
                    payload = None
                if isinstance(payload, dict):
                    if "seq" not in payload and event_id and event_id.isdigit():
                        payload["seq"] = int(event_id)
                    yield payload
                else:
                    logger.warning("Dropped a Hermes SSE frame that is not a JSON object (id %s)", event_id)
            data, event_id = [], None
        elif line.startswith(":"):
            if line[1:].strip() == "stream closed":
                yield None
                return
        elif line.startswith("data:"):
            data.append(line[5:].removeprefix(" "))
        elif line.startswith("id:"):
            event_id = line[3:].strip()


def _raise_for_status(response: httpx.Response, body: bytes) -> None:
    if response.status_code < 400:
        return
    message, code = response.reason_phrase or "error", None
    try:
        error = json.loads(body).get("error", {})
        message, code = str(error.get("message") or message), error.get("code")
    except (ValueError, AttributeError):
        pass
    raise HermesAPIError(response.status_code, message, code if isinstance(code, str) else None)
