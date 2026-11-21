# ag-ui-hermes

Exposes a [Hermes Agent](https://github.com/NousResearch/hermes-agent) API server as an
[AG-UI](https://docs.ag-ui.com) 1.0 agent. It runs outside the Hermes process and talks to the
existing `/v1/runs` HTTP API (start, status, SSE events with `Last-Event-ID` replay, stop,
approval), so any AG-UI frontend, such as `@assistant-ui/react-ag-ui`, can drive Hermes.

- `HermesTranslator`: pure, I/O-free mapping from Hermes run events to AG-UI events. You can feed
  it any Hermes-shaped event stream.
- `HermesRunsClient`: async httpx client for `/v1/runs`. `HermesAgent` only needs the
  `RunsClient` protocol it implements, so a host can back the agent with its own bridge.
- `HermesAgent`: AG-UI `run` / `connect` / `stop` with a pluggable thread store.
- `ag_ui_hermes.endpoint.add_hermes_fastapi_endpoint` (extra `[fastapi]`).
- `js/`: npm package `ag-ui-hermes` with a `HermesAgent` (an `HttpAgent` that implements `connect()`).

## Quickstart

```bash
pip install "ag-ui-hermes[fastapi]"
```

```python
import os
from fastapi import FastAPI
from ag_ui_hermes import HermesAgent, HermesRunsClient, HookContext
from ag_ui_hermes.endpoint import add_hermes_fastapi_endpoint

async def authorize(ctx: HookContext) -> bool:
    return await user_owns_thread(ctx.request, ctx.thread_id)  # your check, see Authorization

client = HermesRunsClient("http://127.0.0.1:8642", api_key=os.environ["API_SERVER_KEY"])
app = FastAPI()
add_hermes_fastapi_endpoint(app, HermesAgent(client, authorize=authorize), "/agent")
# POST /agent (run), POST /agent/connect (rejoin), POST /agent/stop {"threadId", "runId"}
```

`add_hermes_fastapi_endpoint` passes extra keyword arguments (`dependencies`, `tags`, ...) to
every route. `HermesRunsClient` is an async context manager (or call `aclose()`).

```tsx
import { HermesAgent } from "ag-ui-hermes";
import { useAgUiRuntime } from "@assistant-ui/react-ag-ui";
import { AssistantRuntimeProvider } from "@assistant-ui/react";

const agent = new HermesAgent({
  url: "/agent",
  getHeaders: () => ({ "X-CSRF-Token": readCsrfToken() }), // resolved per request
});

export function Chat() {
  const runtime = useAgUiRuntime({ agent });
  return <AssistantRuntimeProvider runtime={runtime}>{/* thread UI */}</AssistantRuntimeProvider>;
}
```

Closing the browser or switching threads only detaches: the Hermes run keeps going, and
`connect()` rejoins it later (see [docs/connect.md](docs/connect.md)). An explicit cancel calls
`abortRun()`, which posts the thread and its last run id to `/agent/stop`; the server stops the
Hermes run only if that AG-UI run started, resumed or rejoined it, so a late stop cannot cancel a
newer run. `runId` is required; before its first run or connect the agent posts no stop.

## Event mapping

| Hermes event | AG-UI events |
| --- | --- |
| `message.delta` | `TEXT_MESSAGE_START` (first delta of a segment), `TEXT_MESSAGE_CONTENT` |
| `message.interim` (`already_streamed: false`) | its own `TEXT_MESSAGE_START/CONTENT/END` |
| `tool.started` | `TOOL_CALL_START` (`metadata.preview`), `TOOL_CALL_END`; closes the open text segment |
| `tool.completed` | `TOOL_CALL_RESULT` (`content` = preview, `metadata.error`, `metadata.duration`) |
| `reasoning.available` | `REASONING_START`, `REASONING_MESSAGE_START/CONTENT/END`, `REASONING_END` |
| `subagent.start` | `SUBAGENT_STARTED` (`description` = goal, `parentToolCallId` = open `delegate_task` call) |
| `subagent.complete` | `SUBAGENT_FINISHED` (status `completed`, `result` = summary), else `SUBAGENT_ERROR` |
| `approval.request` | `RUN_FINISHED` with `outcome: interrupt` (see below) |
| `approval.responded` | clears the pending interrupt |
| `run.completed` | output fix-up (below), `RUN_FINISHED` success |
| `run.cancelled` | `RUN_FINISHED` cancelled |
| `run.failed`, `run.interrupted` | `RUN_ERROR` (`code` = status) |
| `run.steered`, `run.stopping`, `run.waiting_for_approval`, `replay.truncated` | nothing |
| anything else | `custom_event_mapper`, else `CUSTOM` `hermes.<event>` if `passthrough_unknown`, else dropped |

Ids are derived from the Hermes run id and the event `seq` (`<run>:msg:<seq>`,
`<run>:tool:<seq>`, `<run>:tool:<seq>:result`, `<run>:reasoning:<seq>`), so replaying a run from
seq 0 reproduces them exactly. Text after a tool call starts a new assistant message; tool calls
attach to the latest assistant message (`parentMessageId`). A rejoin (`connect`, or a retried run)
replays the backlog from seq 0; if the client already holds messages of that Hermes run, a
`MESSAGES_SNAPSHOT` first drops them so the replay rebuilds them under the same ids
(see [docs/connect.md](docs/connect.md)).

**Approvals.** The interrupt id is the Hermes `request_id`. Hermes does not say which tool call
an approval belongs to, so only when exactly one visible tool call is open (and matches the
approval's `tool`, if it names one) and the backlog was not truncated does the interrupt get `reason: "tool_call"` and `toolCallId`,
which assistant-ui renders as a tool approval; otherwise it is `reason: "confirmation"`. `message`
holds the description and command; `metadata` carries `command`, `description`, `pattern_key` and
`choices`. Resuming with `{"approved": true}` means `once`; `{"approved": false}` or a cancelled
entry always means `deny`; `{"choice": "session"}` or a bare choice string asks for that choice.
Only choices in `allowed_choices` (default `once`, `deny`) are accepted: any other choice is an
input error (HTTP 400, see Errors) and nothing is sent. Each pending interrupt is answered at most
once; entries for interrupts the thread is not waiting on are ignored. The resumed AG-UI run
continues the same Hermes run; if Hermes still waits on an approval the resume left unanswered,
the run ends again with that interrupt.

**Canonical output.** On `run.completed` the final `output` is compared with the text streamed
after the last tool call. A missing suffix is appended, unstreamed output becomes a
`<run>:final` message, and anything else is corrected with a `MESSAGES_SNAPSHOT`.

## Run input

- The last user message becomes the Hermes `input`: its text, or `text` and `image_url` parts when
  it carries image parts (inline data or an `http(s)` URL). An image-only message works. Audio,
  video and document parts, and file-handle sources, are refused with `invalid_input` (HTTP 400).
- Hermes `instructions` (its per-run `ephemeral_system_prompt`, never stored in the session) are
  built on every run from the `instructions` a `run_body` hook returns, the system and developer
  messages, `context` (as a "Runtime context" list) and `state` (as JSON with sorted keys). Hermes
  puts them in the system prompt, so a change from one run to the next, such as a new state,
  costs a prompt-cache miss.
- `forwarded_props={"model", "provider", "model_options"}` (any subset; default none) lets a
  client choose those `/v1/runs` fields through `forwardedProps`; a wrongly typed value is
  `invalid_input`, and `run_body` fields win. Hermes refuses a model that conflicts with a
  session's model lock (`hermes_rejected`).
- The `Idempotency-Key` is derived from the thread and run ids and the body is built
  deterministically, so a retried start of the same AG-UI run is recognized by Hermes.

## Hooks

Every hook gets a `HookContext` first: `action` (`"run"`, `"connect"` or `"stop"`),
`thread_id`, `run_id`, `input` (the `RunAgentInput`, `None` for `stop`) and `request` (the
Starlette `Request` from the FastAPI endpoint, or what you pass as `request=` to `run`,
`connect`, `open_run`, `open_connect` and `stop`). Type aliases for each hook live in
`ag_ui_hermes.agent`.

```python
HermesAgent(
    client,
    store=MyThreadStore(),                                   # ThreadStore protocol, see Limitations
    authorize=check_access,                                  # async ctx -> bool, see Authorization
    tool_filter=lambda ctx, tool, preview, event: None if tool == "secrets" else (tool, preview),
    custom_event_mapper=lambda ctx, event: None,             # host-specific events -> AG-UI events
    state_provider=load_state,                               # async ctx -> state, see below
    state_refresh_tools={"ha_call_service"},                 # read state again after these tools complete
    run_body=lambda ctx: {"instructions": "..."},            # extra /v1/runs body fields
    on_error=lambda ctx, exc: sentry_sdk.capture_exception(exc),
    allowed_choices={"once", "session", "deny"},             # approval choices clients may send
    approval_timeout=300,                                    # Hermes approval timeout, for expiresAt
    forwarded_props={"model"},                               # forwardedProps clients may set, see Run input
    poll_timeout=1800,                                       # give up polling a run whose stream is gone
)
```

Each stream (run, resume or connect) opens with a `STATE_SNAPSHOT` from `state_provider`. A
refresh after a tool in `state_refresh_tools` sends a `STATE_DELTA` (RFC 6902 JSON Patch against
the last state sent in that stream) when both states are JSON objects, nothing when the state did
not change, and a new snapshot otherwise.

## Authorization

`threadId` is chosen by the client, so anyone who knows a thread id could rejoin, stop or answer
approvals on it. Pass `authorize`: it runs before `run` (including resumes), `connect` and `stop`
touch the thread, and returning `False` refuses the request. The endpoint then answers HTTP 403;
`HermesAgent.run`/`connect` used directly end with `RUN_ERROR` code `forbidden`, and `stop` raises
`AgentForbiddenError`. Without `authorize` every request is allowed, which only suits a
single-user host or one that checks the thread in a route dependency.

## Errors and logging

A `RUN_ERROR` the adapter raises carries a fixed message and a stable `code`, never exception
text, Hermes URLs or Hermes's error bodies. The cause goes to the `ag_ui_hermes` logger and to
`on_error`.

| `code` | Meaning |
| --- | --- |
| `invalid_input` | Nothing to send or resume, a disallowed or conflicting approval choice |
| `forbidden` | `authorize` refused the request |
| `hermes_unreachable` | Transport failure talking to Hermes (an idempotent start is retried first) |
| `hermes_rejected` | Hermes answered with an HTTP error status |
| `hermes_bad_response` | Hermes answered 2xx with a body that is not a JSON object |
| `hermes_timeout` | The run did not finish within `poll_timeout` while its status was polled |
| `store_conflict` | The thread store kept refusing compare-and-set writes |
| `internal_error` | Any other exception (a store or hook failure, a bug) |
| `failed`, `interrupted` | The Hermes run itself ended that way (the message is generic; the cause stays in Hermes) |

Input and authorization errors are found before anything is started or stopped. The endpoint
answers them with HTTP 400 or 403 and `{"error": {"code", "message"}}` instead of a stream;
`HermesAgent.open_run`/`open_connect` raise them as `AgentInputError`/`AgentForbiddenError` for
hosts with their own routes. Every later failure ends the stream with `RUN_ERROR` (unless the run
already finished). All errors derive from `HermesError`.

## Limitations

- Hermes tool events carry no tool-call id or arguments. Completions are paired with starts in
  FIFO order per tool name, which is wrong if two calls of the same tool finish out of order; the
  call's arguments are not available (the `preview` summary is in `metadata`).
- The default `InMemoryThreadStore` lives in one process and keeps `max_threads` (10 000)
  records, evicting the least recently used idle thread first; use a shared store for several
  workers. `ThreadRecord.to_dict()`/`from_dict()` serialize a record with a `schema_version`.
  A store implements `get` and `put_if_run`. `put_if_run` must be an atomic compare-and-set:
  it writes only if the stored record still owns that Hermes run and still has the `version` the
  caller read, and bumps `version` (a missing record counts as an empty one with `run_id=None`,
  `version=0`). That keeps a finished stream from releasing a newer run, and two workers starting
  or attaching on the same thread from losing each other's writes (the agent re-reads and retries).
- Only the last user message is sent. History lives in the Hermes session; when the store knows
  no session for a thread, prior text messages go as `conversation_history` (without images).
- A new run on a thread stops that thread's unfinished Hermes run. A retry of the same AG-UI run
  (same `runId`) rejoins that run's own Hermes run instead, even after a newer run replaced it;
  the record keeps the last 32 started runs per thread.
- Hermes keeps 1000 events per run and drops a run's stream about five minutes after it started if
  no client is subscribed. Rejoining after that falls back to polling the run status and shows only
  the final output. If the backlog has dropped the start of the text being streamed, that text is
  not shown again until `run.completed` replaces it with the final output; text before a tool call
  whose start was dropped is not recovered.
- Approval `choices` are exposed as Hermes sends them. assistant-ui's built-in approval UI sends
  only approve/deny; `session`/`always` need a custom UI and `allowed_choices`. Hermes approvals time out on their own;
  answering an expired one is ignored and the run continues.

## Development

```bash
uv run pytest && uv run mypy
cd js && npm install && npm run build && npm test
uv run python -m tests.connect_cases  # regenerate the streams js/src/connect.test.ts replays
```

MIT licensed.
