"""FastAPI server that exposes a Hermes Agent as an AG-UI agent at `/agent`."""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

from fastapi import FastAPI

from ag_ui_hermes import HermesAgent, HermesRunsClient, HookContext
from ag_ui_hermes.endpoint import add_hermes_fastapi_endpoint

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

client = HermesRunsClient(
    os.environ.get("HERMES_URL", "http://127.0.0.1:8642"),
    api_key=os.environ.get("HERMES_API_KEY"),  # the API_SERVER_KEY of the Hermes API server
)

# Toy ownership map: thread id -> user. The first user to touch a thread owns it.
# It lives in memory, like the default thread store. Use your database in a real app.
owners: dict[str, str] = {}


async def authorize(ctx: HookContext) -> bool:
    # Stands in for real authentication (a session cookie, a JWT). The web app sends a
    # random per-browser id in this header.
    user: str | None = ctx.request.headers.get("x-demo-user") if ctx.request is not None else None
    if not user:
        return False
    if ctx.action == "capabilities":  # concerns no thread
        return True
    return owners.setdefault(ctx.thread_id, user) == user


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    yield
    await client.aclose()


app = FastAPI(lifespan=lifespan)
add_hermes_fastapi_endpoint(app, HermesAgent(client, authorize=authorize), "/agent")


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
