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
the agent's last run or connect (without one it posts nothing; the server requires `runId`). The
server stops the Hermes run only if that AG-UI run is attached
to it (it started, resumed or rejoined it), so a delayed stop cannot cancel a newer run. Detaching
just drops the HTTP stream, and the server never stops a Hermes run because its client went away.

## What the server sends

`POST /connect` looks up the thread's Hermes run. Without one it answers `RUN_STARTED`,
`RUN_FINISHED`. Otherwise it opens `GET /v1/runs/{id}/events` from the start of the backlog and
streams every event through a fresh translator, then follows the live stream.

If the client holds messages of this Hermes run (their ids start with `<run>:`), the first
replayed event is preceded by a `MESSAGES_SNAPSHOT` of the client's other messages. That drops the
client's copy of the run, and the replay rebuilds it under the same ids. A probe
(`js/src/connect.test.ts`) that feeds this agent's connect streams to `@ag-ui/client` 1.0.3 and to
`@assistant-ui/react-ag-ui` 0.1.11's thread controller chose this design:

- The two apply events for a message they already hold differently. `@ag-ui/client`'s reducer
  appends to it (a full replay of "Hello" onto a held "Hel" gives "HelHello"), while assistant-ui
  treats a `TEXT_MESSAGE_START` for a held id as a collision and drops the stream (the thread stays
  at "Hel"). Sending only the missing suffix therefore fixes one and not the other.
- Both replace their messages on a `MESSAGES_SNAPSHOT` and then build the replayed messages from
  `TEXT_MESSAGE_START`, so drop-then-replay gives "Hello" in both: for a partial message, for a
  finished segment followed by a live one, for held tool calls and results, and when an early held
  message diverged from the run.
- It needs no boundary between backlog and live events, which Hermes does not expose: neither
  `GET /v1/runs/{id}` nor the SSE stream says which seq was the last one already produced.
- `@ag-ui/client` verifies each run on its own (`TEXT_MESSAGE_CONTENT` needs a `TEXT_MESSAGE_START`
  in the same run), which a full replay satisfies.

The cost is sending the whole backlog again, at most Hermes' 1000 events.

A replayed `approval.request` may already be answered. Before turning one into an interrupt the
agent checks `GET /v1/runs/{id}`: only a run still `waiting_for_approval` with the same
`request_id` interrupts.

Resuming after an interrupt is different: the client already has everything up to the approval, so
the agent posts the approval, replays silently up to the interrupt's `seq` to rebuild translator
state, and emits only newer events. Open subagents are announced again in the new AG-UI run.

If the backlog no longer starts at seq 0 (Hermes keeps 1000 events, and says `replay.truncated`),
the replay cannot rebuild what the client holds, so there is no snapshot: the client keeps its
copy, and replayed messages, tool calls and results it holds are skipped (held text stays as the
client last saw it). The text segment whose first delta was evicted has an unknown id and is not
streamed under an invented one; the next tool call, reasoning or interim message restores normal
ids. `run.completed` then sends a `MESSAGES_SNAPSHOT` that puts the canonical `output` in place of
the stale text and corrects held messages whose text changed, keeping held tool calls and results
the replay did not rebuild. A tool completion whose start was evicted attaches to the oldest
unfinished call of that tool the client holds, or is dropped. An approval after truncation is never
bound to a tool call.

If the stream is gone (Hermes sweeps an unsubscribed run's stream after about five minutes), the
agent polls the run status until it is terminal and emits the final output.

A run request that repeats the `runId` of one of the thread's last 32 started AG-UI runs (a
transport retry) rejoins that run's Hermes run the same way instead of starting a new one. A retry
that arrives after a newer run replaced its run neither stops the newer run nor takes over the
thread: it replays its own, now cancelled, run.

Python clients use `HermesAgent.connect()` the same way.
