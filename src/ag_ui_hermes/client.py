"""Async client for the Hermes ``/v1/runs`` HTTP API."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncGenerator, AsyncIterator, Mapping
from dataclasses import dataclass
from typing import Any

import httpx


class HermesError(Exception):
    """Base error for this client."""


class HermesAPIError(HermesError):
    def __init__(self, status: int, message: str, code: str | None = None) -> None:
        super().__init__(f"Hermes API {status}: {message}")
        self.status, self.message, self.code = status, message, code


class RunStreamGone(HermesError):
    """The run's event stream is no longer available (finished and swept, or unknown)."""


@dataclass(frozen=True)
class StartedRun:
    run_id: str
    status: str
    replayed: bool


class HermesRunsClient:
    """Talks to ``POST /v1/runs`` and its status, events, stop and approval routes.

    ``base_url`` is the API root without ``/v1`` (for example ``http://127.0.0.1:8642``,
    or a profile prefix such as ``http://host:8642/p/work``).
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
    ) -> None:
        self._base = base_url.rstrip("/")
        self._headers = dict(headers or {})
        if api_key:
            self._headers["Authorization"] = f"Bearer {api_key}"
        self._http = http or httpx.AsyncClient(timeout=timeout)
        self._owns_http = http is None
        self._stream_timeout = httpx.Timeout(timeout, read=stream_read_timeout)
        self.max_reconnects = max_reconnects

    async def aclose(self) -> None:
        if self._owns_http:
            await self._http.aclose()

    async def _request(
        self, method: str, path: str, *, json_body: Any = None, headers: Mapping[str, str] | None = None
    ) -> dict[str, Any]:
        try:
            response = await self._http.request(
                method, self._base + path, json=json_body, headers={**self._headers, **(headers or {})}
            )
        except httpx.TransportError as exc:
            raise HermesError(f"Hermes API unreachable: {exc!r}") from exc
        _raise_for_status(response, response.content)
        data = response.json()
        return data if isinstance(data, dict) else {}

    async def start_run(
        self,
        input: str,
        *,
        session_id: str | None = None,
        conversation_history: list[dict[str, str]] | None = None,
        idempotency_key: str | None = None,
        extra: Mapping[str, Any] | None = None,
    ) -> StartedRun:
        body: dict[str, Any] = {**(extra or {}), "input": input}
        if session_id:
            body["session_id"] = session_id
        if conversation_history:
            body["conversation_history"] = conversation_history
        headers = {"Idempotency-Key": idempotency_key} if idempotency_key else None
        data = await self._request("POST", "/v1/runs", json_body=body, headers=headers)
        return StartedRun(str(data["run_id"]), str(data.get("status", "")), bool(data.get("replayed")))

    async def get_run(self, run_id: str) -> dict[str, Any]:
        return await self._request("GET", f"/v1/runs/{run_id}")

    async def stop_run(self, run_id: str) -> dict[str, Any]:
        return await self._request("POST", f"/v1/runs/{run_id}/stop")

    async def respond_approval(self, run_id: str, *, request_id: str, choice: str) -> dict[str, Any]:
        body = {"request_id": request_id, "choice": choice}
        return await self._request("POST", f"/v1/runs/{run_id}/approval", json_body=body)

    async def events(self, run_id: str, *, last_seq: int = -1) -> AsyncGenerator[dict[str, Any], None]:
        """Yield run events, reconnecting with ``Last-Event-ID`` until the stream closes.

        Raises :class:`RunStreamGone` once the server no longer has the stream.
        """
        failures = 0
        while True:
            headers = {**self._headers, "Accept": "text/event-stream"}
            if last_seq >= 0:
                headers["Last-Event-ID"] = str(last_seq)
            try:
                async with self._http.stream(
                    "GET", f"{self._base}/v1/runs/{run_id}/events", headers=headers,
                    timeout=self._stream_timeout,
                ) as response:
                    if response.status_code == 404:
                        raise RunStreamGone(run_id)
                    if response.status_code >= 400:
                        _raise_for_status(response, await response.aread())
                    async for event in _parse_sse(response.aiter_lines()):
                        if event is None:
                            return
                        failures = 0
                        if isinstance(event.get("seq"), int):
                            last_seq = event["seq"]
                        yield event
            except httpx.TransportError:
                pass
            failures += 1
            if failures > self.max_reconnects:
                raise RunStreamGone(f"{run_id}: stream kept dropping")
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
    raise HermesAPIError(response.status_code, message, code)
