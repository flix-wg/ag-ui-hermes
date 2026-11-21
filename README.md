# ag-ui-hermes

Exposes a [Hermes Agent](https://github.com/NousResearch/hermes-agent) API server as an
[AG-UI](https://docs.ag-ui.com) 1.0 agent. It runs outside the Hermes process and talks to the
existing `/v1/runs` HTTP API (start, status, SSE events with `Last-Event-ID` replay, stop,
approval), so any AG-UI frontend, such as `@assistant-ui/react-ag-ui`, can drive Hermes.

- `HermesTranslator`: pure, I/O-free mapping from Hermes run events to AG-UI events. You can feed
  it any Hermes-shaped event stream.
- `HermesRunsClient`: async httpx client for `/v1/runs`.
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
from ag_ui_hermes import HermesAgent, HermesRunsClient
from ag_ui_hermes.endpoint import add_hermes_fastapi_endpoint

client = HermesRunsClient("http://127.0.0.1:8642", api_key=os.environ["API_SERVER_KEY"])
app = FastAPI()
add_hermes_fastapi_endpoint(app, HermesAgent(client), "/agent")
# POST /agent (run), POST /agent/connect (rejoin), POST /agent/stop {"threadId": ...}
```

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
`abortRun()`, which posts to `/agent/stop`.

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
attach to the latest assistant message (`parentMessageId`).

**Approvals.** The interrupt id is the Hermes `request_id`. If a tool call is open, the interrupt
has `reason: "tool_call"` and `toolCallId`, which assistant-ui renders as a tool approval;
otherwise `reason: "confirmation"`. `metadata` carries `command`, `description`, `pattern_key`
and `choices`. Resuming with `{"approved": true|false}` maps to `once`/`deny`, `{"choice": "session"}`
or a bare choice string is passed through, and a cancelled resume entry denies. The resumed AG-UI
run continues the same Hermes run.

**Canonical output.** On `run.completed` the final `output` is compared with the text streamed
after the last tool call. A missing suffix is appended, unstreamed output becomes a
`<run>:final` message, and anything else is corrected with a `MESSAGES_SNAPSHOT`.

## Hooks

```python
HermesAgent(
    client,
    store=MyThreadStore(),                          # ThreadStore protocol: get/put ThreadRecord
    tool_filter=lambda tool, preview, event: None if tool == "secrets" else (tool, preview),
    custom_event_mapper=lambda event: None,         # host-specific events -> AG-UI events
    state_provider=load_state,                      # async thread_id -> STATE_SNAPSHOT
    state_refresh_tools={"ha_call_service"},        # snapshot again after these tools complete
    run_body=lambda input: {"instructions": "..."}, # extra /v1/runs body fields
)
```

## Limitations

- Hermes tool events carry no tool-call id or arguments. Completions are paired with starts in
  FIFO order per tool name, which is wrong if two calls of the same tool finish out of order; the
  call's arguments are not available (the `preview` summary is in `metadata`).
- The default `InMemoryThreadStore` lives in one process; use a shared store for several workers.
- Only the text of the last user message is sent. History lives in the Hermes session; when the
  store knows no session for a thread, prior text messages go as `conversation_history`.
- A new run on a thread stops that thread's unfinished Hermes run.
- Hermes keeps 1000 events per run and drops a run's stream about five minutes after it started if
  no client is subscribed. Rejoining after that falls back to polling the run status and shows only
  the final output.
- Approval `choices` are exposed as Hermes sends them. assistant-ui's built-in approval UI sends
  only approve/deny; `session`/`always` need a custom UI. Hermes approvals time out on their own;
  answering an expired one is ignored and the run continues.

## Development

```bash
uv run pytest && uv run mypy
cd js && npm install && npm run build && npm test
```

MIT licensed.
