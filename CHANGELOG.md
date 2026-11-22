# Changelog

All notable changes to this project are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project adheres to [Semantic Versioning](https://semver.org/).

## [0.1.0] - Unreleased

First release.

### Added

- `HermesAgent`: AG-UI 1.0 runs backed by the Hermes Agent `/v1/runs` API, with text, server-side tool calls and results, reasoning, subagents, token usage and `protocolVersion` on `RUN_STARTED`.
- `HermesTranslator`: pure mapping of Hermes run events to AG-UI events, with ids derived from the run id and event `seq` so a replay reproduces them.
- Approvals as AG-UI interrupts, resumed by the next run on the thread; a resume that leaves an approval open ends with that interrupt again.
- Rejoining a run with `connect`: the backlog is replayed after a `MESSAGES_SNAPSHOT` that drops the client's copy, then live events follow. Without a live run, `connect` restores the Hermes session's messages.
- Explicit stop that only cancels the Hermes run the calling AG-UI run owns; a client disconnect only detaches.
- Run input: image parts as `image_url` parts, `forwarded_props` for `model`, `provider` and `model_options`, idempotent run starts. Tool completions pair with their start by `tool_call_id` or `id` once a run carries them, else by tool name. `poll_timeout` also bounds stalled status calls.
- Host state through `state_provider`: `STATE_SNAPSHOT`, then `STATE_DELTA` (RFC 6902) after `state_refresh_tools` and before the run ends.
- `GET <path>/capabilities` and `HermesAgent.capabilities()`.
- Options grouped in the frozen `InputPolicy` (what a client may send) and `ApprovalPolicy` (`allowed_choices`, `timeout`); hooks (`authorize`, `tool_filter`, `state_provider`, `run_body`, `custom_event_mapper`, `on_error`) may be sync or async, and their types are exported.
- `unknown_events` (`"drop"`, `"custom"` or `"raw"`) for Hermes events nothing maps.
- `SupportsSessionMessages` and `SupportsCapabilities` protocols for the optional `RunsClient` methods.
- Thread store protocol with an atomic `compare_and_set`, and a bounded `InMemoryThreadStore`.
- FastAPI endpoint (`ag-ui-hermes[fastapi]`): run, connect, stop and capabilities routes, with request bodies in the OpenAPI schema. Malformed bodies answer HTTP 400 `invalid_input`, and every `invalid_input` carries a fixed `reason`.
- `HermesTranslator` is public but provisional.
- npm package `ag-ui-hermes`: `HermesAgent` (an `HttpAgent`) with `connect()`, server-side `abortRun()`, async `getHeaders` and `getCapabilities()`.
- Example: FastAPI server and assistant-ui chat, runnable against a fake Hermes.

### Fixed

- `InMemoryThreadStore` evicts a newly idle thread when active runs have pushed the store past `max_threads` and no older idle thread can be evicted to restore the cap.

### Security

- `authorize` hook before every run, resume, connect, stop and capabilities request; refusals answer HTTP 403.
- `RUN_ERROR` and error responses carry a fixed message and a stable `code`, never exception text or Hermes error bodies.
- Clients can only send approval choices in `ApprovalPolicy.allowed_choices` (default `once` and `deny`).
- Client system and developer messages are ignored unless `InputPolicy(client_system_messages=True)`.
- `context` and `state` reach the Hermes instructions only as JSON in a `<client_data>` block (`<`, `>` and `&` escaped) after an untrusted-data notice. `client_context=False` and `client_state=False` switch them off.
- Images are accepted as inline base64 data only, up to `max_image_bytes` (5 MiB) each and `max_message_image_bytes` (20 MiB) per message; `None` disables a limit. `http(s)` image URLs need `allow_image_urls=True`. Violations answer `invalid_input` (HTTP 400).
- A resume that answers an interrupt the thread neither waits on nor has answered is refused with `invalid_input`.
- `tool_filter` also applies to the session history `connect` restores; restored tool arguments are always `"{}"`.
- Restored history carries no stored tool results, which are whole and unredacted unlike live previews, unless `InputPolicy(restore_tool_results=True)`.
- After a truncated replay, a tool completion attaches to its call by tool-call id only and is dropped without one, as its start may have been evicted.
- Restored history keys a reused tool-call id to the latest call, so a hidden call's stored result never passes as the earlier visible call's.
- The endpoint refuses POST bodies without an `application/json` (or `+json`) content type (`malformed_body`), so cross-origin requests always need a CORS preflight.
- `InMemoryThreadStore` never evicts a thread with a live run; at the cap it grows and logs a warning.
- The adapter never logs request bodies, headers or tokens. Exceptions from stores and hooks are logged with their traceback, verbatim; the host owns its logging.
