# FastAPI + assistant-ui example

A minimal chat in front of a Hermes Agent: streaming text, tool calls, approval interrupts
(approve once or deny) and rejoining a run after a page reload.

- `server/app.py`: FastAPI app with `HermesAgent` at `/agent`, an `authorize` hook with a toy
  per-user thread owner map, and `GET /health`.
- `server/fake_hermes_server.py`: the test suite's fake Hermes (`tests/fake_hermes.py`) served
  over HTTP, for a demo without Hermes.
- `web/`: Vite + React + `@assistant-ui/react` + `@assistant-ui/react-ag-ui`, using the local
  `js/` package.

Use Python 3.11+ with [uv](https://docs.astral.sh/uv/) and Node.js 22.18+ on Node 22,
24.11+ on Node 24, or 26+ with npm. Run `make install` from this directory before
starting either mode.

## Demo without Hermes

```bash
make demo
```

Open http://localhost:5173 and send any message. Every run plays the same script: a sentence,
a `terminal` call to `ls` that asks for approval, then the result and an answer, or a refusal
if you deny. The fake Hermes listens on 8642 and the server on 8000; override them with
`make demo HERMES_PORT=18642 SERVER_PORT=18000`.

## Against a real Hermes

Start the Hermes API server (see the
[Hermes API server docs](https://hermes-agent.nousresearch.com/docs/user-guide/features/api-server)).
Set `API_SERVER_KEY` in your shell to the key configured in Hermes, then run these
commands in separate terminals.

```bash
HERMES_URL=http://127.0.0.1:8642 HERMES_API_KEY="${API_SERVER_KEY:?Set the Hermes API key}" make server
make web
```

## What to try

- **Approvals.** In demo mode, send any message to trigger an approval. With real Hermes, the
  card appears when a command requires approval. It sends `{"approved": true}` (Hermes choice
  `once`) or `{"approved": false}` (`deny`).
- **Reconnect.** Reload the page while a run streams or waits for approval. The thread id is
  kept in `localStorage`, and the history adapter in `web/src/App.tsx` asks the runtime to
  resume, so it calls `connect()` and the server replays the run. A finished thread is restored
  from the Hermes session instead (the fake Hermes has no sessions, so there it comes back
  empty). After a rejoin your own prompt is not shown, because it is not part of the run's
  events.
- **Stop.** The Stop button calls `abortRun()`, which posts to `/agent/stop` and cancels the
  Hermes run.
- **Authorization.** Each browser gets a random id, sent as `X-Demo-User` through `getHeaders`.
  The first user to touch a thread owns it; anyone else gets HTTP 403. Replace this with your
  real authentication.

The owner map and the default thread store live in memory, so restarting the server forgets
all threads.
