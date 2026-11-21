# ag-ui-hermes

[AG-UI](https://docs.ag-ui.com) agent for a [Hermes Agent](https://github.com/NousResearch/hermes-agent)
endpoint served by the Python package `ag-ui-hermes`. `HermesAgent` extends `HttpAgent` from
`@ag-ui/client` and adds `connect()` (rejoin a run in flight), `abortRun()` (stop the run on the
server), `getHeaders` and `getCapabilities()`.

## Install

```bash
npm install ag-ui-hermes @ag-ui/client rxjs
```

Peer dependencies: `@ag-ui/client` `~1.0.3` (tested with 1.0.3 and 1.0.4; the agent uses protected
`HttpAgent` internals, so minor versions are not allowed) and `rxjs` `^7.8.0`. Node >= 20. ESM and CJS.

## Usage with assistant-ui

```tsx
import { HermesAgent } from "ag-ui-hermes";
import { useAgUiRuntime } from "@assistant-ui/react-ag-ui";
import { AssistantRuntimeProvider } from "@assistant-ui/react";

const agent = new HermesAgent({
  url: "/agent",
  getHeaders: () => ({ "X-CSRF-Token": readCsrfToken() }),
});

export function Chat() {
  const runtime = useAgUiRuntime({ agent });
  return <AssistantRuntimeProvider runtime={runtime}>{/* thread UI */}</AssistantRuntimeProvider>;
}
```

## Options

| Option | Default | |
| --- | --- | --- |
| `url` | required | Run endpoint (`POST`). |
| `connectUrl` | `<url>/connect` | Rejoin endpoint. |
| `stopUrl` | `<url>/stop` | Stop endpoint. |
| `headers` | `{}` | Static headers (from `HttpAgent`). |
| `getHeaders` | none | Called before every request; its result overrides `headers`. |

### `getHeaders` and CSRF

`getHeaders` may be sync or async (`() => Record<string, string> | Promise<Record<string, string>>`),
so you can refresh a token first. It is applied to every request the agent makes: run, connect,
stop and capabilities. It is called once per request, so keep it cheap and cache the token.

## Connect, reconnect and stop

- Leaving a thread or closing the tab only detaches. The Hermes run keeps going on the server.
- `connect()` (called by `useAgUiRuntime` when a thread loads) rejoins the run by replaying it
  from the start. Messages the client already holds are sent along, and the server replaces
  them with the replay under the same ids, so nothing is duplicated.
- `abortRun()` is an explicit cancel. It posts `{ threadId, runId }` to `stopUrl`, naming the last
  run or connect of this agent so a late stop cannot cancel a newer run, then aborts the local
  request. Before the first run or connect nothing is posted. The stop is best effort: failures
  are swallowed.

## Capabilities

```ts
const capabilities = await agent.getCapabilities(); // GET <url>/capabilities
```

Returns the AG-UI `AgentCapabilities` object served by the endpoint. It rejects on a non-2xx
response. This requires a server version that serves the route.

## License

MIT
