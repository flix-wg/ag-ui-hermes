"""Serve the test suite's fake Hermes `/v1/runs` API over HTTP, for a demo without Hermes.

Every run plays the same script: some text, a `terminal` tool call that needs approval,
then the tool result and an answer, or a short refusal if the approval was denied.
"""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path
from typing import Any

import httpx
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import Response, StreamingResponse

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))  # repo root, for tests.fake_hermes
from tests.fake_hermes import FakeHermes, Step


class Pause(asyncio.Event):
    """A script step that waits a little, so the text visibly streams."""

    def __init__(self, seconds: float) -> None:
        super().__init__()
        self.seconds = seconds

    async def wait(self) -> bool:  # type: ignore[override]
        await asyncio.sleep(self.seconds)
        return True


class Branch(asyncio.Event):
    """Appends the approved or the denied ending to the running script."""

    def __init__(self, fake: FakeHermes, script: list[Step], approved: list[Step], denied: list[Step]) -> None:
        super().__init__()
        self.fake, self.script, self.approved, self.denied = fake, script, approved, denied

    async def wait(self) -> bool:  # type: ignore[override]
        answers = [body for _, path, body in self.fake.calls if path.endswith("/approval")]
        deny = bool(answers) and answers[-1].get("choice") == "deny"
        self.script.extend(self.denied if deny else self.approved)
        return True


def say(text: str, **final: Any) -> list[Step]:
    """Stream `text` word by word, then complete the run with it as the output."""
    steps: list[Step] = []
    for word in text.split(" "):
        steps += [{"event": "message.delta", "delta": word + " "}, Pause(0.05)]
    return [*steps, {"event": "run.completed", "completed": True, "output": text, **final}]


class Scripts(list[list[Step]]):
    """Hands every new run a fresh copy of the demo script."""

    def __init__(self, fake: FakeHermes) -> None:
        super().__init__()
        self.fake = fake
        self.runs = 0

    def pop(self, index: Any = -1) -> list[Step]:
        self.runs += 1
        approved: list[Step] = [
            Pause(0.5),
            {"event": "tool.completed", "tool": "terminal", "duration": 0.4, "error": False, "preview": "README.md  docs  examples  js  src  tests"},
            *say(
                "The directory holds the README, docs, examples, the JS package, the sources and the tests.", usage={"input_tokens": 120, "output_tokens": 40}
            ),
        ]
        denied: list[Step] = [
            {"event": "tool.completed", "tool": "terminal", "duration": 0.0, "error": True, "preview": "denied"},
            *say("The command was denied, so I did not run it."),
        ]
        script: list[Step] = [Pause(0.2)]
        script += say("Let me look at the files first.")[:-1]
        script += [
            {"event": "tool.started", "tool": "terminal", "preview": "ls"},
            {
                "event": "approval.request",
                "request_id": f"req-{self.runs}",
                "tool": "terminal",
                "command": "ls",
                "description": "list the working directory",
                "choices": ["once", "session", "always", "deny"],
            },
        ]
        script.append(Branch(self.fake, script, approved, denied))
        return script


key = os.environ.get("HERMES_API_KEY", "demo-key")
fake = FakeHermes(key=key)
fake.scripts = Scripts(fake)
app = FastAPI()


@app.api_route("/{path:path}", methods=["GET", "POST"])
async def proxy(path: str, request: Request) -> Response:
    forwarded = httpx.Request(request.method, str(request.url), headers=request.headers.raw, content=await request.body())
    answer = await fake.handle(forwarded)
    if answer.headers.get("content-type") == "text/event-stream":
        return StreamingResponse(answer.aiter_raw(), media_type="text/event-stream")
    return Response(answer.content, status_code=answer.status_code, media_type="application/json")


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=int(os.environ.get("PORT", "8642")))
