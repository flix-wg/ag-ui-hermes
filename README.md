# ag-ui-hermes

Exposes a [Hermes Agent](https://github.com/NousResearch/hermes-agent) API server as an
[AG-UI](https://docs.ag-ui.com) 1.0 agent. It runs outside the Hermes process and uses the
existing `/v1/runs` HTTP API, so any AG-UI frontend, such as assistant-ui with
`@assistant-ui/react-ag-ui`, can drive Hermes.

It has two parts:

- the Python package `ag-ui-hermes`: `HermesAgent`, `HermesRunsClient`, the pure
  `HermesTranslator` and a FastAPI endpoint;
- the npm package `ag-ui-hermes` in [`js/`](js/README.md): a `HermesAgent` that extends
  `HttpAgent` from `@ag-ui/client` with `connect()`, a server-side stop and dynamic headers.

## Features

| Feature | AG-UI |
| --- | --- |
| Streaming text | `TEXT_MESSAGE_START/CONTENT/END` |
| Server-side tool calls and results | `TOOL_CALL_START/END`, `TOOL_CALL_RESULT` |
| Reasoning, as one block per Hermes reasoning event | `REASONING_START`, `REASONING_MESSAGE_*`, `REASONING_END` |
| Delegated subagents | `SUBAGENT_STARTED/FINISHED/ERROR` |
| Command approvals | `RUN_FINISHED` with an `interrupt` outcome, answered through `resume` |
| Rejoining a run after a reload or thread switch | `connect`, `MESSAGES_SNAPSHOT` |
| Host state | `STATE_SNAPSHOT`, `STATE_DELTA` |
| Image input | `image` input parts |
| Token usage of the Hermes run | `usage` on `RUN_FINISHED` / `RUN_ERROR` |
| Capabilities | `GET <path>/capabilities`, `AgentCapabilities` |

The full event table is in [docs/events.md](docs/events.md).

## Install

Not on PyPI or npm yet. Install from source:

```bash
pip install "ag-ui-hermes[fastapi] @ git+https://github.com/flix-wg/ag-ui-hermes"
```

After the first release this becomes `pip install "ag-ui-hermes[fastapi]"` and
`npm install ag-ui-hermes`. Until then, build the JS package from `js/` (see
[js/README.md](js/README.md)) or use it through a `file:` dependency as the
[example](examples/fastapi-assistant-ui) does.

## Quickstart

You need a running Hermes API server and its `API_SERVER_KEY`.

```python
# app.py
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI

from ag_ui_hermes import HermesAgent, HermesRunsClient, HookContext
from ag_ui_hermes.endpoint import add_hermes_fastapi_endpoint

client = HermesRunsClient("http://127.0.0.1:8642", api_key=os.environ["HERMES_API_KEY"])
owners: dict[str, str] = {}  # thread id -> user; use your database


async def authorize(ctx: HookContext) -> bool:
    user = ctx.request.headers.get("x-user")  # replace with your real authentication
    if not user:
        return False
    if ctx.action == "capabilities":
        return True
    return owners.setdefault(ctx.thread_id, user) == user


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await client.aclose()


app = FastAPI(lifespan=lifespan)
add_hermes_fastapi_endpoint(app, HermesAgent(client, authorize=authorize), "/agent")
```

```bash
pip install uvicorn
HERMES_API_KEY=... uvicorn app:app --port 8000
```

This registers four routes:

| Route | |
| --- | --- |
| `POST /agent` | Start a run, or resume one after an interrupt. SSE response. |
| `POST /agent/connect` | Rejoin the thread's run. SSE response. |
| `POST /agent/stop` | `{"threadId", "runId"}`: stop the Hermes run if that AG-UI run owns it. Answers `{"stopped": bool}`. |
| `GET /agent/capabilities` | AG-UI `AgentCapabilities`. |

Extra keyword arguments to `add_hermes_fastapi_endpoint` (`dependencies`, `tags`, ...) go to
every route. Without FastAPI, call `HermesAgent.open_run` / `open_connect` / `stop` /
`capabilities` from your own routes.

[examples/fastapi-assistant-ui](examples/fastapi-assistant-ui) is a complete server and chat UI
that also runs against a fake Hermes.

## Frontend

```tsx
import { AssistantRuntimeProvider } from "@assistant-ui/react";
import { useAgUiRuntime } from "@assistant-ui/react-ag-ui";
import { HermesAgent } from "ag-ui-hermes";

const agent = new HermesAgent({
  url: "/agent",
  getHeaders: async () => ({ Authorization: `Bearer ${await getToken()}` }),
});

export function Chat() {
  const runtime = useAgUiRuntime({ agent });
  return <AssistantRuntimeProvider runtime={runtime}>{/* thread UI */}</AssistantRuntimeProvider>;
}
```

Approvals arrive as interrupts: read them with `useAgUiInterrupts()` and answer with
`useAgUiSubmitInterruptResponses()`, for example
`[{ interruptId, status: "resolved", payload: { approved: true } }]`. See
[docs/configuration.md](docs/configuration.md#approvals) for the accepted payloads.

## Configuration and hooks

`HermesAgent(client, **options)`. Every hook gets a `HookContext` first, with `action`
(`"run"`, `"connect"`, `"stop"` or `"capabilities"`), `thread_id`, `run_id`, `input` (the
`RunAgentInput`) and `request` (the Starlette `Request` from the endpoint).

| Option | Default | |
| --- | --- | --- |
| `authorize` | none (allow all) | `async (ctx) -> bool`; `False` refuses with HTTP 403. |
| `store` | `InMemoryThreadStore()` | Thread to Hermes session/run mapping. Use a shared store for several workers. |
| `tool_filter` | none | Rename, rewrite or hide tool calls and results. |
| `state_provider` | none | `async (ctx) -> state`, sent as `STATE_SNAPSHOT` at the start of each stream. |
| `state_refresh_tools` | `()` | Tools after which the state is read again and sent as `STATE_DELTA`. |
| `run_body` | none | Extra `POST /v1/runs` body fields, such as `instructions`. |
| `allowed_choices` | `{"once", "deny"}` | Approval choices a client may send. |
| `forwarded_props` | `()` | Which of `model`, `provider`, `model_options` a client may set. |
| `on_error` | none | Sees every error, for example to report it. |

All options, the run input mapping, approvals and the thread store contract are in
[docs/configuration.md](docs/configuration.md).

## Errors

The endpoint refuses a request before streaming with HTTP 400 (`invalid_input`) or 403
(`forbidden`) and a body `{"error": {"code", "message"}}`. Any later failure ends the stream
with `RUN_ERROR`. Its `message` is fixed and its `code` is one of:

| `code` | Meaning |
| --- | --- |
| `invalid_input` | Nothing to send or resume, an unsupported input part, or a disallowed or conflicting approval choice. |
| `forbidden` | `authorize` refused the request. |
| `hermes_unreachable` | Transport failure talking to Hermes. A start with an idempotency key is retried first. |
| `hermes_rejected` | Hermes answered with an HTTP error status. |
| `hermes_bad_response` | Hermes answered 2xx with a body that is not a JSON object. |
| `hermes_timeout` | The run did not finish within `poll_timeout` while its status was polled. |
| `store_conflict` | The thread store kept refusing compare-and-set writes. |
| `internal_error` | Any other exception: a store or hook failure, or a bug. |
| `failed`, `interrupted` | The Hermes run itself ended that way. |

`RUN_ERROR` never carries exception text, Hermes URLs or Hermes error bodies. The cause goes
to the `ag_ui_hermes` logger and to `on_error`. A Hermes error on `/capabilities` or `/stop`
answers HTTP 502. All exceptions derive from `HermesError`.

## Reconnect and stop

Closing the browser or switching threads only detaches: the Hermes run keeps going.
`connect()` rejoins it by replaying the run from its first event, after a `MESSAGES_SNAPSHOT`
that drops the client's copy of that run, so nothing is duplicated. On a thread with no live
run and an empty client, `connect()` restores the Hermes session's messages instead.

An explicit cancel (`abortRun()` in JS) posts `{threadId, runId}` to `/agent/stop`. The server
stops the Hermes run only if that AG-UI run started, resumed or rejoined it, so a late stop
cannot cancel a newer run. A new run on a thread stops that thread's unfinished Hermes run.

Details, including truncated backlogs and lost streams: [docs/connect.md](docs/connect.md).

## Limitations

- **Frontend tools.** Not supported. Hermes runs every tool on the server, and `/v1/runs` cannot
  hand a call to the client and wait for its result, so `RunAgentInput.tools` is ignored. This
  needs a change in Hermes.
- **Tool arguments and ids.** Hermes tool events carry neither. Ids are derived from the run id
  and event `seq`, arguments are always `{}`, and the `preview` summary is in
  `TOOL_CALL_START.metadata`. Completions pair with starts in order per tool name, which is wrong
  if two calls of the same tool finish out of order. History restored on connect has the real
  ids and arguments.
- **Reasoning.** Not streamed: Hermes sends each reasoning block once, complete.
- **`ACTIVITY_*` and `STEP_*` events** are not emitted.
- **Usage** covers the whole Hermes run, reported once by the AG-UI run that sees it end, as the
  total across every resume. There is no per-step or per-message usage.
- **Only the last user message is sent.** History lives in the Hermes session. On a thread
  without a session, earlier text messages go as `conversation_history`, without images.
- **Hermes keeps 1000 events per run** and drops an unsubscribed run's stream about five minutes
  after it started. A rejoin after that falls back to polling and shows only the final output.
- **Hermes version.** Tested against Hermes Agent v0.22.2. The adapter needs the `/v1/runs` API
  with SSE events (`approval.request`, `run.*`) and `POST /v1/runs/{id}/approval`; older
  versions without them do not work.

## Compatibility

| | |
| --- | --- |
| Python | 3.11, 3.12, 3.13 |
| `ag-ui-protocol` (Python) | 1.x |
| `@ag-ui/client` (JS peer) | `~1.0.3` (tested with 1.0.3 and 1.0.4) |
| `@assistant-ui/react-ag-ui` | 0.1.9 or later for `connect()`; tested with 0.1.11 |
| Node | 20 or later |
| Hermes Agent | tested with v0.22.2 |

## More

- [docs/configuration.md](docs/configuration.md): options, run input, approvals, state, thread store
- [docs/events.md](docs/events.md): Hermes to AG-UI event mapping and ids
- [docs/connect.md](docs/connect.md): rejoin, resume and stop in detail
- [docs/security.md](docs/security.md): threat model and what to configure
- [CONTRIBUTING.md](CONTRIBUTING.md), [CHANGELOG.md](CHANGELOG.md), [SECURITY.md](SECURITY.md)

MIT licensed, see [LICENSE](LICENSE).
