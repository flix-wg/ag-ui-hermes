# Rejoining a run (`connect`)

## What the frontend does

`@assistant-ui/react-ag-ui` 0.1.11 rejoins an in-flight run only through
`agent.connectAgent()`, which calls the agent's protected `connect(input)`. It checks that the
agent's class overrides `connect()`; the stock `HttpAgent` does not, so it would start a new run
instead. It connects when the history adapter reports `unstable_resume` on load and after a thread
switch (`resumeInFlightRun`). Leaving a thread or unmounting calls `detachActiveRun()`; only an
explicit cancel calls `abortRun()`.

The TypeScript `HermesAgent` therefore overrides `connect()` to post the `RunAgentInput` to
`<url>/connect`, and `abortRun()` to also post `{threadId}` to `<url>/stop`. Detaching just drops
the HTTP stream, and the server never stops a Hermes run because its client went away.

## What the server sends

`POST /connect` looks up the thread's Hermes run. Without one it answers `RUN_STARTED`,
`RUN_FINISHED`. Otherwise it opens `GET /v1/runs/{id}/events` from the start of the backlog and
streams every event through a fresh translator, then follows the live stream.

Replaying everything, rather than only a suffix, was chosen because:

- The translator's ids depend only on the Hermes run id and event `seq`, so the replay names the
  same messages and tool calls the client already shows.
- assistant-ui adopts assistant ids it already holds and replaces their content with the replayed
  frames (`AgUiThreadController.replay.test.ts`: "adopts contiguous assistant IDs ...", "keeps
  longer partial text when replay starts with its strict prefix"), so the same text is not appended
  twice.
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

If the stream is gone (Hermes sweeps an unsubscribed run's stream after about five minutes), the
agent polls the run status until it is terminal and emits the final output.

Python clients use `HermesAgent.connect()` the same way.
