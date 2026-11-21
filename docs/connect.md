# Rejoining a run (`connect`)

## What the frontend does

`@assistant-ui/react-ag-ui` 0.1.11 rejoins an in-flight run only through
`agent.connectAgent()`, which calls the agent's protected `connect(input)`. It checks that the
agent's class overrides `connect()`; the stock `HttpAgent` does not, so it would start a new run
instead. It connects when the history adapter reports `unstable_resume` on load and after a thread
switch (`resumeInFlightRun`). Leaving a thread or unmounting calls `detachActiveRun()`; only an
explicit cancel calls `abortRun()`.

The TypeScript `HermesAgent` therefore overrides `connect()` to post the `RunAgentInput` to
`<url>/connect`, and `abortRun()` to also post `{threadId, runId}` to `<url>/stop`, where `runId` is
the agent's last run or connect. The server stops the Hermes run only if that AG-UI run is attached
to it (it started, resumed or rejoined it), so a delayed stop cannot cancel a newer run. Detaching
just drops the HTTP stream, and the server never stops a Hermes run because its client went away.

## What the server sends

`POST /connect` looks up the thread's Hermes run. Without one it answers `RUN_STARTED`,
`RUN_FINISHED`. Otherwise it opens `GET /v1/runs/{id}/events` from the start of the backlog and
streams every event through a fresh translator, then follows the live stream.

Replaying everything, rather than only a suffix, was chosen because:

- The translator's ids depend only on the Hermes run id and event `seq`, so the replay names the
  same messages and tool calls the client already shows.
- `@ag-ui/client`'s reducer applies a `TEXT_MESSAGE_START` for an id it holds to that message and
  appends every delta, so replaying "Hello" onto a held "Hel" would give "HelHello". The connect
  request's `messages` (what the client holds) therefore steer the replay: messages, tool calls,
  results and reasoning the client already has are skipped, a text message is opened only when
  there is text beyond the client's copy, and only that suffix is sent. If the client's copy is
  not a prefix of the run's text, a `MESSAGES_SNAPSHOT` replaces it.
- `@ag-ui/client` verifies each run on its own: `TEXT_MESSAGE_CONTENT` needs a `TEXT_MESSAGE_START`
  and `SUBAGENT_FINISHED` a `SUBAGENT_STARTED` in the same run, so a connect stream that started
  mid-message would be rejected.
- Hermes SSE does not mark where the backlog ends and live events begin, which a
  "snapshot, then continue" approach would need.

A replayed `approval.request` may already be answered. Before turning one into an interrupt the
agent checks `GET /v1/runs/{id}`: only a run still `waiting_for_approval` with the same
`request_id` interrupts.

Resuming after an interrupt is different: the client already has everything up to the approval, so
the agent posts the approval, replays silently up to the interrupt's `seq` to rebuild translator
state, and emits only newer events. Open subagents are announced again in the new AG-UI run.

If the backlog no longer starts at seq 0 (Hermes keeps 1000 events, and says `replay.truncated`),
the text segment whose first delta was evicted has an unknown id. Its text is not streamed under
an invented id; the next tool call, reasoning or interim message restores normal ids, and
`run.completed` reconciles the final answer from the run's canonical `output` (a
`MESSAGES_SNAPSHOT` when the client holds a stale partial). Text before a tool call whose start
was evicted stays as the client last saw it.

If the stream is gone (Hermes sweeps an unsubscribed run's stream after about five minutes), the
agent polls the run status until it is terminal and emits the final output.

A run request that repeats an AG-UI `runId` the thread's Hermes run is attached to (a transport
retry) rejoins that run the same way instead of stopping it and starting a new one.

Python clients use `HermesAgent.connect()` the same way.
