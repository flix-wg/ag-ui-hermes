# ag-ui-hermes

An [AG-UI](https://docs.ag-ui.com) `HttpAgent` for a
[Hermes Agent](https://github.com/NousResearch/hermes-agent) endpoint served by the
[Python adapter](../README.md). It supports reconnecting to runs, stopping them
server-side, sending dynamic headers, and fetching capabilities.

*AI-driven project built with Claude, Codex, and Gemini.*

## Install

```sh
npm install ag-ui-hermes  # once published
```

The package ships ESM and CommonJS for Node.js 22+. It has two peer dependencies,
`@ag-ui/client` (`~1.0.3`, tested with 1.0.3 and 1.0.4) and `rxjs` (`^7.8.0`).
To build from this directory, run `npm ci && npm run build` with Node.js 22.18+ on
Node 22, 24.11+ on Node 24, or 26+.

## Usage with assistant-ui

```tsx
import type { PropsWithChildren } from "react";
import { AssistantRuntimeProvider } from "@assistant-ui/react";
import { useAgUiRuntime } from "@assistant-ui/react-ag-ui";
import { HermesAgent } from "ag-ui-hermes";

const agent = new HermesAgent({ url: "/agent" });

export function Chat({ children }: PropsWithChildren) {
  const runtime = useAgUiRuntime({ agent });
  return <AssistantRuntimeProvider runtime={runtime}>{children}</AssistantRuntimeProvider>;
}
```

The [FastAPI + assistant-ui example](../examples/fastapi-assistant-ui/README.md)
adds a thread UI with approvals, reconnect, and demo authentication.

## Options

| Option | Default | Purpose |
| --- | --- | --- |
| `url` | Required | Run endpoint. |
| `connectUrl` | `<url>/connect` | Rejoin endpoint. |
| `stopUrl` | `<url>/stop` | Stop endpoint. |
| `headers` | `{}` | Static request headers. |
| `getHeaders` | Omitted | Sync or async callback called once per request. Returned headers override static headers with the same keys. |

The runtime can call `connectAgent()` to rejoin a live run. Leaving a thread detaches
the stream and lets the run continue. `abortRun()` also sends a best-effort stop
request for the current run. See [reconnect and stop](../docs/connect.md).

`getCapabilities()` fetches `GET <url>/capabilities` and returns `AgentCapabilities`.
It rejects on a non-2xx response.
