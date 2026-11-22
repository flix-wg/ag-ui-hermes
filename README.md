# ag-ui-hermes

Connect a [Hermes Agent](https://github.com/NousResearch/hermes-agent) server to an
[AG-UI](https://docs.ag-ui.com) 1.0 frontend. The Python adapter runs alongside Hermes
and uses its `/v1/runs` HTTP API. The [JavaScript client](js/README.md) works with assistant-ui
and other `@ag-ui/client` consumers.

*AI-driven project built with Claude, Codex, and Gemini.*

## Quickstart

Try the [FastAPI + assistant-ui example](examples/fastapi-assistant-ui/README.md) with
a fake Hermes and no API key. With Python 3.11+, uv, and a supported Node version, run
these commands from the repository root.

```sh
cd examples/fastapi-assistant-ui
make install
make demo
```

Open `http://localhost:5173` to try streaming chat, approvals, and reconnecting.

## Install

Once the packages are published, install the FastAPI adapter and the JavaScript client.

```sh
pip install "ag-ui-hermes[fastapi]"
npm install ag-ui-hermes
```

From a checkout, run `uv sync --all-extras` and `(cd js && npm ci && npm run build)`.
The Python package can also be installed from the checkout with
`pip install ".[fastapi]"`. The `fastapi` extra is optional for custom HTTP routes.

## Connect your app

On your server, create a `HermesRunsClient` with the Hermes API URL and its
`API_SERVER_KEY`. Pass it to `HermesAgent` with an `authorize` hook that checks the
caller and thread ownership. Keep the key server-side and close the client on shutdown.
See the [example server](examples/fastapi-assistant-ui/server/app.py) and the
[security guide](docs/security.md).

Register `add_hermes_fastapi_endpoint(app, agent, "/agent")` from
`ag_ui_hermes.endpoint`. Point the [JavaScript agent](js/README.md#usage-with-assistant-ui)
at `/agent`.

| Route | Behavior |
| --- | --- |
| `POST /agent` | Stream a new run or resume after an approval. |
| `POST /agent/connect` | Rejoin a live run or restore session history. |
| `POST /agent/stop` | Stop the run owned by the supplied `threadId` and `runId`. |
| `GET /agent/capabilities` | Return AG-UI `AgentCapabilities`. |

The adapter streams text, server-side tool calls, reasoning blocks, subagent events,
and run usage. It accepts image input and can expose host state. Tool execution stays
on Hermes. Closing the page detaches the stream, and an explicit stop requests cancellation.

## Compatibility

| Component | Requirement |
| --- | --- |
| Python | 3.11+ (CI covers 3.11–3.13) |
| `ag-ui-protocol` | 1.x |
| Node.js | 22+ for the package. Source builds require 22.18+ on Node 22, 24.11+ on Node 24, or 26+. |
| `@ag-ui/client` | `~1.0.3` (CI checks 1.0.3 and 1.0.4) |
| `@assistant-ui/react-ag-ui` | Tested with 0.1.11 |
| Hermes Agent | Tested with v0.22.2. Requires `/v1/runs` with SSE and the run approval endpoint. |

## Documentation

- [Configuration, inputs, hooks, approvals, and errors](docs/configuration.md)
- [Event mapping and limitations](docs/events.md)
- [Reconnect, resume, and stop](docs/connect.md)
- [Security guide](docs/security.md) and [private vulnerability reporting](SECURITY.md)
- [Contributing](CONTRIBUTING.md), [changelog](CHANGELOG.md), and [MIT license](LICENSE)
