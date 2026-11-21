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
- Thread store protocol with compare-and-set writes, and a bounded `InMemoryThreadStore`.
- FastAPI endpoint (`ag-ui-hermes[fastapi]`): run, connect, stop and capabilities routes.
- npm package `ag-ui-hermes`: `HermesAgent` (an `HttpAgent`) with `connect()`, server-side `abortRun()`, async `getHeaders` and `getCapabilities()`.
- Example: FastAPI server and assistant-ui chat, runnable against a fake Hermes.

### Security

- `authorize` hook before every run, resume, connect, stop and capabilities request; refusals answer HTTP 403.
- `RUN_ERROR` and error responses carry a fixed message and a stable `code`, never exception text or Hermes error bodies.
- Clients can only send approval choices in `allowed_choices` (default `once` and `deny`).
- Client system and developer messages are ignored unless `client_system_messages=True`.
- `context` and `state` reach the Hermes instructions only as JSON in a `<client_data>` block (`<`, `>` and `&` escaped) after an untrusted-data notice. `client_context=False` and `client_state=False` switch them off.
- Images are accepted as inline base64 data only, up to `max_image_bytes` (5 MiB) each and `max_message_image_bytes` (20 MiB) per message; `None` disables a limit. `http(s)` image URLs need `allow_image_urls=True`. Violations answer `invalid_input` (HTTP 400).
- A resume that answers an interrupt the thread neither waits on nor has answered is refused with `invalid_input`.
- `tool_filter` also applies to the session history `connect` restores; restored tool arguments are always `"{}"`.
- `InMemoryThreadStore` never evicts a thread with a live run; at the cap it grows and logs a warning.
- The adapter never logs request bodies, headers or tokens. Exceptions from stores and hooks are logged with their traceback, verbatim; the host owns its logging.
- `tool_filter` hides tool results in restored history, since stored rows lack the preview a filter may hide by.
