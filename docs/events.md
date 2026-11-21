# Event mapping

`HermesTranslator` turns one Hermes run's events into AG-UI events without I/O, so you can feed
it any Hermes-shaped stream. It is provisional: `HermesAgent` is the stable entry point, and
the translator's API may change in a minor release.

| Hermes event | AG-UI events |
| --- | --- |
| `message.delta` | `TEXT_MESSAGE_START` (first delta of a segment), `TEXT_MESSAGE_CONTENT` |
| `message.interim` (`already_streamed: false`) | its own `TEXT_MESSAGE_START/CONTENT/END` |
| `tool.started` | `TOOL_CALL_START` (`metadata.preview`), `TOOL_CALL_END`; closes the open text segment |
| `tool.completed` | `TOOL_CALL_RESULT` (`content` = preview, `metadata.error`, `metadata.duration`) |
| `reasoning.available` | `REASONING_START`, `REASONING_MESSAGE_START/CONTENT/END`, `REASONING_END` |
| `subagent.start` | `SUBAGENT_STARTED` (`description` = goal, `parentToolCallId` = open `delegate_task` call) |
| `subagent.complete` | `SUBAGENT_FINISHED` (`result` = summary) if completed, else `SUBAGENT_ERROR` |
| `approval.request` | `RUN_FINISHED` with outcome `interrupt` (see [configuration.md](configuration.md#approvals)) |
| `approval.responded` | clears the pending interrupt |
| `run.completed` | output fix-up (below), `RUN_FINISHED` success |
| `run.cancelled` | `RUN_FINISHED` cancelled |
| `run.failed`, `run.interrupted` | `RUN_ERROR` (`code` = status) |
| `run.steered`, `run.stopping`, `run.waiting_for_approval`, `replay.truncated` | nothing |
| anything else | `custom_event_mapper`, else by `unknown_events`: dropped (`"drop"`, the default), `CUSTOM` `hermes.<event>` (`"custom"`) or `RAW` (`"raw"`) |

Every stream starts with `RUN_STARTED` carrying `protocolVersion`. A terminal run also carries
`usage` (input, output, total and cache tokens, provider and model) when Hermes reports it.

## Ids

Ids derive from the Hermes run id and the event `seq`, so replaying a run from seq 0 gives the
same ids:

| | |
| --- | --- |
| Text message | `<run>:msg:<seq>` |
| Tool call / its result | `<run>:tool:<seq>`, plus `:id:<id>` when `tool.started` carries a `tool_call_id` (or `id`); the result adds `:result` |
| Reasoning | `<run>:reasoning:<seq>` |
| Subagent | `<run>:subagent:<key>`, the key being Hermes `subagent_id`, else `<delegation_id>:<task_index>`, else the seq |
| Final output not streamed | `<run>:final` |
| Restored session history | `hermes:msg:<row id>` |

Once a run's tool events have carried a `tool_call_id` (or `id`), a `tool.completed` pairs with
its start by that id only; one without an id gets a call of its own. In a run without ids it takes
the earliest open call of the same tool name. Once the backlog was truncated, a completion is
matched by id only and dropped without one, since a lost start could own it (see [connect](connect.md)).

With a `state_provider`, a final `STATE_DELTA` or `STATE_SNAPSHOT` precedes `RUN_FINISHED` and
`RUN_ERROR` from a Hermes run end. Errors raised by the adapter itself get none.

Text after a tool call starts a new assistant message. Tool calls attach to the latest assistant
message (`parentMessageId`).

## Canonical output

On `run.completed` the final `output` is compared with the text streamed after the last tool
call. A missing suffix is appended, unstreamed output becomes a `<run>:final` message, and
anything else is corrected with a `MESSAGES_SNAPSHOT`.
