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
`respond_approval`, `events`), so a host can back it with its own bridge. A bridge that also
implements `SupportsSessionMessages` (`session_messages`) lets `connect` restore history, and
one that implements `SupportsCapabilities` (`get_capabilities`) adds what Hermes reports to
the capabilities; the agent looks both methods up with `getattr`, so a proxy's `__getattr__` counts. The protocols' docstrings in
[`client.py`](../src/ag_ui_hermes/client.py) give the error contract.

## `HermesAgent`

```python
HermesAgent(client, *, store=None, authorize=None, tool_filter=None, custom_event_mapper=None,
            unknown_events="drop", state_provider=None, state_refresh_tools=(), run_body=None,
            on_error=None, poll_interval=1.0, poll_timeout=1800.0,
            inputs=InputPolicy(), approvals=ApprovalPolicy())
```

Every hook may be a plain function or a coroutine function; the agent awaits a result that is
awaitable. The hook types (`Authorize`, `ToolFilter`, `StateProvider`, `RunBody`,
`CustomEventMapper`, `OnError`) are exported from `ag_ui_hermes`.

| Option | Default | |
| --- | --- | --- |
| `store` | `InMemoryThreadStore()` | See [Thread store](#thread-store). |
| `authorize` | none | `(ctx) -> bool`. See [security.md](security.md). |
| `tool_filter` | none | `(ctx, tool, preview, event) -> (tool, preview) \| None`. Called on every `tool.started` (`None` hides the whole call) and `tool.completed` (`None` hides only the result), so keep it cheap. Restored session history passes calls with an empty preview, and results (only with `restore_tool_results`) as a `tool.completed` with the stored content as preview. |
| `custom_event_mapper` | none | `(ctx, event) -> list[BaseEvent] \| None` for Hermes events the translator does not know. `None` falls through to `unknown_events`. |
| `unknown_events` | `"drop"` | Hermes events nothing maps: `"drop"`, `"custom"` (`CUSTOM` named `hermes.<event>`) or `"raw"` (`RAW` with `source="hermes"`). |
| `state_provider` | none | `(ctx) -> state`. `None` sends nothing. |
| `state_refresh_tools` | `()` | Tool names after whose completion the state is read again. |
| `run_body` | none | `(ctx) -> dict` of extra `POST /v1/runs` fields. |
| `on_error` | none | `(ctx, exc) -> None`. Sees every error the agent reports. Its own exceptions are logged and ignored. |
| `poll_interval` | `1.0` | Seconds between status polls when the event stream is gone. |
| `poll_timeout` | `1800.0` | Seconds of polling before failing with `hermes_timeout`. Also bounds a single status call that stalls. `None` polls and waits forever. |
| `inputs` | `InputPolicy()` | What a client's request may put into a run, below. |
| `approvals` | `ApprovalPolicy()` | Which approval choices a client may send, below. |

### `InputPolicy`

A frozen dataclass; change one field with `dataclasses.replace`.

| Field | Default | |
| --- | --- | --- |
| `client_system_messages` | `False` | Add the client's system and developer messages to the Hermes instructions. Ignored otherwise. |
| `client_context` | `True` | Pass the client's `context` to the instructions as data. |
| `client_state` | `True` | Pass the client's `state` to the instructions as data. |
| `allow_image_urls` | `False` | Accept `http(s)` image URLs, which Hermes fetches. Only inline `data:` images otherwise. |
| `max_image_bytes` | 5 MiB | Largest decoded inline image. `None` disables the limit. |
| `max_message_image_bytes` | 20 MiB | Largest total of decoded inline images in one message. `None` disables the limit. |
| `forwarded_props` | none | `forwardedProps` keys a client may set: any of `model`, `provider`, `model_options`. Any other key raises `ValueError`. |
| `restore_tool_results` | `False` | Restore stored tool results when `connect` restores a session's history. Stored results are whole and unredacted, unlike the live previews (at most 500 characters, with secrets redacted by Hermes), so they stay out by default. |

### `ApprovalPolicy`

| Field | Default | |
| --- | --- | --- |
| `allowed_choices` | `once`, `deny` | Approval choices a client may send. `deny` is always allowed. |
| `timeout` | `None` | Seconds Hermes waits on an approval. When set, interrupts carry `expiresAt`. |

### `HookContext`

| Field | |
| --- | --- |
| `action` | `"run"`, `"connect"`, `"stop"` or `"capabilities"`. |
| `thread_id`, `run_id` | From the request. Both `None` for `"capabilities"`. |
| `input` | The `RunAgentInput`. `None` for `"stop"` and `"capabilities"`. |
| `request` | The Starlette `Request` from the endpoint, or what you pass as `request=` to `run`, `connect`, `open_run`, `open_connect`, `stop` and `capabilities`. Typed as `RequestLike` (`headers`, `client`, `url`), so FastAPI stays optional. |

### Calling the agent directly

- `run(input)` / `connect(input)` return an async generator of AG-UI events. Every failure,
  including a refused request, ends it with `RUN_ERROR`.
- `open_run(input)` / `open_connect(input)` authorize and validate first and raise
  `AgentForbiddenError` or `AgentInputError` before anything is started, so your route can
  answer 403 or 400. They then return the event generator.
- `stop(thread_id, run_id)` returns whether the AG-UI run owned the thread's Hermes run.
- `capabilities()` returns `AgentCapabilities`.

### Errors

The FastAPI endpoint returns HTTP 400 for invalid input, 403 for denied access, and 502 for
upstream errors on stop and capabilities requests. Its error bodies have the shape
`{"error": {"code": "...", "message": "..."}}`. Once streaming begins, the adapter reports
failures with `RUN_ERROR`. These are the standard error codes.

| Code | Meaning |
| --- | --- |
| `invalid_input` | The request failed validation. See the reasons below. |
| `forbidden` | The `authorize` hook denied the request. |
| `hermes_unreachable` | A transport failure prevented a request to Hermes. |
| `hermes_rejected` | Hermes returned an error status. |
| `hermes_bad_response` | Hermes returned JSON or data in an unexpected format. |
| `hermes_timeout` | Polling for the run exceeded `poll_timeout`. |
| `store_conflict` | The thread store kept refusing compare-and-set writes. |
| `internal_error` | An unexpected exception occurred in the adapter, store, or a hook. |
| `failed`, `interrupted` | The Hermes run ended with that status. |

See [security and logs](security.md#errors-and-logs) for server-side diagnostics.

`invalid_input` (`AgentInputError`) always comes with a fixed `reason` that never echoes
client data:

| `reason` | |
| --- | --- |
| `malformed_body` | The endpoint could not parse the body as JSON of the right shape (`RunAgentInput`, or `{threadId, runId}` for `/stop`), or it came without an `application/json` (or `+json`) content type. |
| `no_user_message` | No user message with content to send. |
| `unsupported_media` | An audio, video or document part, or an image source that is not allowed (an `http(s)` URL without `allow_image_urls`, a non-image or non-base64 data URL). |
| `invalid_image` | Inline image data that is not base64. |
| `image_too_large` | An image over `max_image_bytes`, or a message's images over `max_message_image_bytes`. |
| `bad_forwarded_props` | An allowed `forwardedProps` value of the wrong type. |
| `bad_choice` | An approval choice outside `allowed_choices`, or two answers for one interrupt. |
| `unknown_interrupt` | A resume naming an interrupt the thread neither waits on nor has answered, or a resume on a thread without an interrupted run. |
| `stale_resume` | The thread moved on to a newer run between validating the resume and streaming it. |

The endpoint answers it before streaming as HTTP 400 with
`{"error": {"code": "invalid_input", "message", "reason"}}`, the `message` being fixed per
reason. `stale_resume` only happens once the stream has started, and `run()` and `connect()`
never answer with HTTP: there it ends the stream as `RUN_ERROR` with `code: "invalid_input"`
and the same message, and the reason is on the exception passed to `on_error`
(`exc.reason`) and in the log.

## Run input

How a `RunAgentInput` becomes a `POST /v1/runs` body:

- **User turn.** The last user message becomes the Hermes `input`. It is text, or `text` and
  `image_url` parts when it carries images. An image-only message works. Audio, video and
  document parts are refused with `invalid_input`.
  Images are accepted as inline base64 data (`data:image/...;base64,`). Each may be up to `max_image_bytes`
  (default 5 MiB) and one message's images up to `max_message_image_bytes` (default 20 MiB);
  `None` disables a limit. `http(s)` image URLs are refused unless `InputPolicy(allow_image_urls=True)`,
  because Hermes would fetch them. Anything else, inline data that is not base64, or an image over a limit, is `invalid_input`
  (HTTP 400).
- **History.** Only the last user message is sent; the Hermes session holds the rest. When the
  store knows no session for the thread, earlier user and assistant text goes as
  `conversation_history`.
- **Instructions.** Hermes `instructions` (its per-run system prompt addition, not stored in the
  session) are built on every run from:
  - the `instructions` a `run_body` hook returns;
  - system and developer messages from the client, only with `client_system_messages=True`
    (off by default);
  - `context` and `state`, as one JSON object inside a `<client_data>` block, preceded by a
    sentence saying it is untrusted data and not instructions. `<`, `>` and `&` in the JSON are
    escaped as `\u003c`, `\u003e` and `\u0026`, so the data cannot close the block. Each is
    included by default and can be switched off with `client_context=False` or
    `client_state=False`. Empty `context` and `state` add nothing.

  A change from one run to the next, such as a new state, costs a prompt-cache miss.
- **Forwarded props.** `InputPolicy(forwarded_props={"model"})` (any subset of `model`, `provider`,
  `model_options`) lets a client set those fields through `forwardedProps`. A wrongly typed
  value is `invalid_input`, and `run_body` fields win. Hermes refuses a model that conflicts
  with a session's model lock (`hermes_rejected`).
- **Idempotency.** The `Idempotency-Key` is a SHA-256 hash of the thread and run ids, so a retried
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
| `responseSchema` | `{"choice": enum}` of the choices Hermes offers and `ApprovalPolicy.allowed_choices` permits. |
| `expiresAt` | Only when `ApprovalPolicy.timeout` is set. |
| `metadata` | `command`, `description`, `pattern_key`, `pattern_keys`, `choices`, `smart_denied` as Hermes sends them, and `hermesRunId`. |

Resume with a new run on the same thread whose `resume` answers it:

| Resume entry | Hermes choice |
| --- | --- |
| `payload: {"approved": true}` | `once` |
| `payload: {"approved": false}`, or `status: "cancelled"` | `deny` |
| `payload: {"choice": "session"}` or `payload: "session"` | that choice |

A choice outside `allowed_choices` is `invalid_input` (HTTP 400) and nothing is sent, so add
`session` or `always` to offer them. assistant-ui's built-in approval UI sends only
approve/deny.

A resume must answer interrupts the thread knows: pending ones, or ones a resume already
answered. Any other interrupt id is refused with `invalid_input` (HTTP 400), as is a resume
when the thread has no interrupted run. Each pending interrupt is answered at most once. A late
repeat of an answered id sends nothing to Hermes; if Hermes still waits on another approval the
resume left open, the run ends again with that interrupt. The agent checks the run status for
that approval's `request_id`, and uses the status's own approval when the event backlog no
longer holds it. The resumed AG-UI run continues the same Hermes run.

Hermes approvals time out on their own. Answering an expired one is ignored and the run
continues.

## State

With a `state_provider`, each stream (run, resume or connect) opens with a `STATE_SNAPSHOT`.
After a tool in `state_refresh_tools` completes, and once more just before the run's
`RUN_FINISHED` or `RUN_ERROR`, the state is read again: a `STATE_DELTA` (RFC 6902 JSON Patch
against the last state sent in that stream) when both states are JSON objects, nothing when it
did not change, and a new snapshot otherwise. A run that ends in an [adapter-side error](#errors)
gets no final state. Capabilities then report
state snapshots and deltas.

## Thread store

A store maps an AG-UI thread to its Hermes session and its current Hermes run. It implements
`get(thread_id)` and `compare_and_set(thread_id, record, run_id)`.

`compare_and_set` must be atomic: write only if the stored record still owns
Hermes run `run_id` and still has the `version` the caller read, and store it with `version + 1`.
A missing record counts as `ThreadRecord()` (`run_id=None`, `version=0`). This keeps a finished
stream from releasing a newer run, and two workers on the same thread from losing each other's
writes; the agent re-reads and retries, and fails with `store_conflict` after 50 attempts.

`ThreadRecord.to_dict()` / `from_dict()` serialize a record with a `schema_version`.

`InMemoryThreadStore(max_threads=10_000)` lives in one process and evicts the least recently
used threads without a Hermes run when full. A record with a live run is never evicted, since
its run would lose its stop and approval mapping. When every record is live, the store grows
past `max_threads` and logs a warning. As runs finish, idle records are evicted until the store
is back at capacity. Use a shared store (Redis, a database) when you run several workers.

The record keeps the last 32 started AG-UI runs per thread, so a late transport retry still
finds its Hermes run.
