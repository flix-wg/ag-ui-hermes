# Configuration

## `HermesRunsClient`

```python
HermesRunsClient(base_url, api_key=None, *, headers=None, http=None, timeout=30.0,
                 stream_read_timeout=120.0, max_reconnects=5, start_retries=2)
```

| Argument | |
| --- | --- |
| `base_url` | API root without `/v1`, for example `http://127.0.0.1:8642` or a profile prefix such as `http://host:8642/p/work`. |
| `api_key` | Sent as `Authorization: Bearer <key>` (the Hermes `API_SERVER_KEY`). |
| `headers` | Extra headers on every request. |
| `http` | Your own `httpx.AsyncClient`. The client does not close one it did not create. |
| `timeout` | Seconds for every request. |
| `stream_read_timeout` | Seconds without data before the event stream counts as dropped. |
| `max_reconnects` | Reconnects of a dropped event stream, with `Last-Event-ID`, before giving up. |
| `start_retries` | Retries of `POST /v1/runs` on transport errors. Only a start with an idempotency key is retried, and the agent always sends one. |

Use it as an async context manager or call `aclose()`, for example in a FastAPI lifespan.

`HermesAgent` only needs the `RunsClient` protocol (`start_run`, `get_run`, `stop_run`,
`respond_approval`, `events`, and optionally `session_messages` and `get_capabilities`), so a
host can back it with its own bridge. The protocol's docstring in
[`client.py`](../src/ag_ui_hermes/client.py) gives the error contract.

## `HermesAgent`

```python
HermesAgent(client, *, store=None, authorize=None, tool_filter=None, custom_event_mapper=None,
            passthrough_unknown=False, raw_unknown=False, state_provider=None,
            state_refresh_tools=(), run_body=None, on_error=None, poll_interval=1.0,
            poll_timeout=1800.0, allowed_choices=("once", "deny"), approval_timeout=None,
            forwarded_props=())
```

| Option | Default | |
| --- | --- | --- |
| `store` | `InMemoryThreadStore()` | See [Thread store](#thread-store). |
| `authorize` | none | `async (ctx) -> bool`. See [security.md](security.md). |
| `tool_filter` | none | `(ctx, tool, preview, event) -> (tool, preview) \| None`. Called on `tool.started` (`None` hides the whole call) and on `tool.completed` (`None` hides only the result). |
| `custom_event_mapper` | none | `(ctx, event) -> list[BaseEvent] \| None` for Hermes events the translator does not know. `None` falls through. |
| `passthrough_unknown` | `False` | Forward unknown Hermes events as `CUSTOM` named `hermes.<event>`. |
| `raw_unknown` | `False` | Forward unknown Hermes events as `RAW` with `source="hermes"`. |
| `state_provider` | none | `async (ctx) -> state`. `None` sends nothing. |
| `state_refresh_tools` | `()` | Tool names after whose completion the state is read again. |
| `run_body` | none | `(ctx) -> dict` of extra `POST /v1/runs` fields. |
| `on_error` | none | `(ctx, exc) -> None`. Sees every error the agent reports. Its own exceptions are logged and ignored. |
| `poll_interval` | `1.0` | Seconds between status polls when the event stream is gone. |
| `poll_timeout` | `1800.0` | Seconds of polling before failing with `hermes_timeout`. `None` polls forever. |
| `allowed_choices` | `("once", "deny")` | Approval choices a client may send. `deny` is always allowed. |
| `approval_timeout` | `None` | Seconds Hermes waits on an approval. When set, interrupts carry `expiresAt`. |
| `forwarded_props` | `()` | `forwardedProps` keys a client may set: any of `model`, `provider`, `model_options`. |

Type aliases for every hook (`Authorize`, `ToolFilter`, `StateProvider`, ...) live in
`ag_ui_hermes.agent`.

### `HookContext`

| Field | |
| --- | --- |
| `action` | `"run"`, `"connect"`, `"stop"` or `"capabilities"`. |
| `thread_id`, `run_id` | From the request. Both empty for `"capabilities"`. |
| `input` | The `RunAgentInput`. `None` for `"stop"` and `"capabilities"`. |
| `request` | The Starlette `Request` from the endpoint, or what you pass as `request=` to `run`, `connect`, `open_run`, `open_connect`, `stop` and `capabilities`. |

### Calling the agent directly

- `run(input)` / `connect(input)` return an async generator of AG-UI events. Every failure,
  including a refused request, ends it with `RUN_ERROR`.
- `open_run(input)` / `open_connect(input)` authorize and validate first and raise
  `AgentForbiddenError` or `AgentInputError` before anything is started, so your route can
  answer 403 or 400. They then return the event generator.
- `stop(thread_id, run_id)` returns whether the AG-UI run owned the thread's Hermes run.
- `capabilities()` returns `AgentCapabilities`.

## Run input

How a `RunAgentInput` becomes a `POST /v1/runs` body:

- **User turn.** The last user message becomes the Hermes `input`. It is text, or `text` and
  `image_url` parts when it carries images. An image-only message works. Audio, video and
  document parts are refused with `invalid_input`.
  <!-- TODO(fix-lane): option names and defaults for image sources. -->
  Images are accepted as inline data (`data:image/...`) up to `max_image_bytes` (default 5 MiB).
  `http(s)` image URLs are refused unless `allow_image_urls=True`, because Hermes would fetch
  them. Anything else is `invalid_input` (HTTP 400).
- **History.** Only the last user message is sent; the Hermes session holds the rest. When the
  store knows no session for the thread, earlier user and assistant text goes as
  `conversation_history`.
- **Instructions.** Hermes `instructions` (its per-run system prompt addition, not stored in the
  session) are built on every run from:
  - the `instructions` a `run_body` hook returns;
  <!-- TODO(fix-lane): option names for client system messages, context and state. -->
  - system and developer messages from the client, only with `client_system_messages=True`
    (off by default);
  - `context` and `state`, in a delimited block labelled as client-supplied data. Each can be
    switched off.

  A change from one run to the next, such as a new state, costs a prompt-cache miss.
- **Forwarded props.** `forwarded_props={"model"}` (any subset of `model`, `provider`,
  `model_options`) lets a client set those fields through `forwardedProps`. A wrongly typed
  value is `invalid_input`, and `run_body` fields win. Hermes refuses a model that conflicts
  with a session's model lock (`hermes_rejected`).
- **Idempotency.** The `Idempotency-Key` is derived from the thread and run ids, so a retried
  start of the same AG-UI run is recognized by Hermes. A retry with the same `runId` rejoins
  that run's Hermes run instead of starting a new one.
- **Frontend tools.** `RunAgentInput.tools` is ignored.

## Approvals

A Hermes `approval.request` ends the AG-UI run with `RUN_FINISHED`, outcome `interrupt`:

| Interrupt field | |
| --- | --- |
| `id` | The Hermes `request_id`. |
| `reason` | `"tool_call"` with `toolCallId` when exactly one visible tool call is open (and matches the approval's `tool`, if named) and the backlog was not truncated. Otherwise `"confirmation"`. |
| `message` | Description and command. |
| `responseSchema` | `{"choice": enum}` of the choices Hermes offers and `allowed_choices` permits. |
| `expiresAt` | Only when `approval_timeout` is set. |
| `metadata` | `command`, `description`, `pattern_key`, `pattern_keys`, `choices`, `smart_denied` as Hermes sends them, and `hermesRunId`. |

Resume with a new run on the same thread whose `resume` answers it:

| Resume entry | Hermes choice |
| --- | --- |
| `payload: {"approved": true}` | `once` |
| `payload: {"approved": false}`, or `status: "cancelled"` | `deny` |
| `payload: {"choice": "session"}` or `payload: "session"` | that choice |

A choice outside `allowed_choices` is `invalid_input` (HTTP 400) and nothing is sent, so add
`session` or `always` to offer them. assistant-ui's built-in approval UI sends only
approve/deny. Each pending interrupt is answered at most once; entries for other interrupts are
ignored. The resumed AG-UI run continues the same Hermes run. If Hermes still waits on an
approval the resume left unanswered, the run ends again with that interrupt.

Hermes approvals time out on their own. Answering an expired one is ignored and the run
continues.

## State

With a `state_provider`, each stream (run, resume or connect) opens with a `STATE_SNAPSHOT`.
After a tool in `state_refresh_tools` completes, the state is read again: a `STATE_DELTA`
(RFC 6902 JSON Patch against the last state sent in that stream) when both states are JSON
objects, nothing when it did not change, and a new snapshot otherwise. Capabilities then report
state snapshots and deltas.

## Thread store

A store maps an AG-UI thread to its Hermes session and its current Hermes run. It implements
`get(thread_id)` and `put_if_run(thread_id, record, run_id)`.

`put_if_run` must be an atomic compare-and-set: write only if the stored record still owns
Hermes run `run_id` and still has the `version` the caller read, and store it with `version + 1`.
A missing record counts as `ThreadRecord()` (`run_id=None`, `version=0`). This keeps a finished
stream from releasing a newer run, and two workers on the same thread from losing each other's
writes; the agent re-reads and retries, and fails with `store_conflict` after 50 attempts.

`ThreadRecord.to_dict()` / `from_dict()` serialize a record with a `schema_version`.

<!-- TODO(fix-lane): confirm the eviction rule. -->
`InMemoryThreadStore(max_threads=10_000)` lives in one process and evicts the least recently
used thread without a live Hermes run when full; threads with a live run are never evicted. Use
a shared store (Redis, a database) when you run several workers.

The record keeps the last 32 started AG-UI runs per thread, so a late transport retry still
finds its Hermes run.
