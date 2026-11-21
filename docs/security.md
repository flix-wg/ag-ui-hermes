# Security

ag-ui-hermes authenticates no one. It sits between a browser you do not trust and a Hermes
agent that can run commands, so the host application has to decide who may do what. This page
lists what a client controls and what to configure.

## Threat model

| Asset | Threat |
| --- | --- |
| Hermes API key | Leaks to the browser and gives direct access to every Hermes run and session. |
| A user's thread | Another user rejoins it, reads its history, stops its run or answers its approvals. |
| Command approvals | A client approves a choice the host never meant to offer (`always`). |
| The system prompt | A client injects instructions through messages, `context` or `state`. |
| Hermes and its network | Hermes is made to fetch client-chosen URLs. |
| Logs and errors | Hermes URLs, error bodies or user data reach the client or logs. |

## Keep the key server-side

`HermesRunsClient` holds the Hermes `API_SERVER_KEY`. Only your server talks to Hermes; the
browser talks to your endpoint. Do not expose the Hermes API server itself.

## Thread ids are client-chosen

AG-UI lets the client pick `threadId`. Anyone who knows or guesses a thread id can, without
`authorize`:

- `connect` to it and receive the replayed run and the Hermes session's stored messages;
- `stop` its run;
- answer its pending approvals through `resume`.

Pass `authorize`. It runs before `run` (including resumes), `connect` and `stop` touch the
thread, and before `capabilities` (with `thread_id=None`, so check only the caller). Check
that the authenticated user owns `ctx.thread_id`; `ctx.request` is the incoming request. A
refusal answers HTTP 403 before anything happens. Without `authorize` every request is allowed,
which only suits a single-user host or one that checks the thread in a route dependency.

Record ownership when a thread is first used, and decide what happens to a thread id nobody
owns yet: the [example](../examples/fastapi-assistant-ui/server/app.py) lets the first user
claim it.

CSRF: if you authenticate with cookies, also require a header a cross-site form cannot set.
`HermesAgent` in JS sends `getHeaders()` on every request (run, connect, stop, capabilities).

## Approvals

Hermes asks before risky commands. Its choices are `once`, `session`, `always` and `deny`;
`HermesAgent` accepts only `ApprovalPolicy.allowed_choices` (default `once` and `deny`, and `deny` is always
allowed). A client cannot widen that: any other choice is refused with HTTP 400 and nothing is
sent to Hermes. Add `session` or `always` only if you want users to grant standing permission.

Each pending interrupt is answered at most once, and only interrupts the thread is actually
waiting on are forwarded. A resume naming an interrupt id the thread neither waits on nor has
already answered is refused with `invalid_input` (HTTP 400).

## What reaches the system prompt

Hermes `instructions` become part of the system prompt for that run. They are built from:

- the `instructions` your `run_body` hook returns: trusted, yours;
- client system and developer messages: ignored unless you set
  `InputPolicy(client_system_messages=True)`,
  since any client can send them;
- `context` and `state`: placed as JSON in a `<client_data>` block after a sentence that calls
  it untrusted data, so the model can tell it from your instructions. `<`, `>` and `&` are
  escaped, so the data cannot close the block. Both are on by default; switch them off with
  `InputPolicy(client_context=False, client_state=False)`. Labelling reduces but does not prevent prompt
  injection; treat both as user input.

`forwardedProps` can only set `model`, `provider` or `model_options`, and only the keys you list
in `InputPolicy(forwarded_props=...)` (none by default).

## Images

By default only inline base64 `data:image/...;base64,` images are accepted: up to `max_image_bytes` (5 MiB)
each and `max_message_image_bytes` (20 MiB) per message. `None` disables a limit. `http(s)`
image URLs would make Hermes or its model provider fetch a client-chosen URL, so they are
refused unless you set `InputPolicy(allow_image_urls=True)`. Everything else, and anything over a limit, is
refused with `invalid_input` (HTTP 400).

## Tool output and history

Tool previews and results go to the client as Hermes sends them on the live stream: Hermes
truncates each preview (to 500 characters) and redacts secrets it recognises. Use `tool_filter`
to hide or rewrite tools whose output the user should not see. It applies to live and replayed
runs and to the session history `connect` restores. Stored rows keep no preview, so restored
calls pass the filter with an empty one, and restored tool calls always carry `"{}"` as
arguments.

Hermes stores tool results whole and unredacted, so the history `connect` restores carries no
tool results by default. `InputPolicy(restore_tool_results=True)` restores them; each then passes
`tool_filter` as a `tool.completed` with the full stored content as its preview, so a filter
that hides a call by its live preview should also check the result.

After a truncated replay, a tool completion is dropped unless a tool-call id ties it to its
call: its start may have been evicted, and a hidden call's result must never land on a visible
call of the same tool.

## Errors and logs

`RUN_ERROR` and the endpoint's error bodies carry a fixed message and a stable `code` (and, for
HTTP 400, a fixed `reason`), never exception text, client data, Hermes URLs or Hermes error
bodies. The cause goes to the `ag_ui_hermes` logger
and to `on_error`. Exceptions are logged with `exc_info`, so tracebacks and exception
messages reach the log verbatim: Hermes error messages, and whatever your store, hooks or
`state_provider` put into theirs. The adapter itself logs thread ids and Hermes run ids, never
request bodies, headers or tokens, but it cannot scrub a secret out of another component's
exception. Keep credentials out of exception messages (a database URL with a password, say),
or add a `logging.Filter` to the `ag_ui_hermes` logger. Treat these logs like application logs
with user data.

## Resource use

`InMemoryThreadStore` evicts idle threads beyond `max_threads` records but never a thread with
a live run. If every record is live it grows past `max_threads` and logs a warning. A run
that ends frees its thread even when the client leaves on the last event or the final state
refresh fails. A run whose last client left before it ended keeps its record until the thread
is used again, so a rejoin can still replay it; under churn, use a store that expires records. A rejoin replays at most Hermes' 1000 events per run. Status polling of a run whose stream
is gone, and a status call that stalls, stop after `poll_timeout`. Rate limiting and request size limits are up to your server.

## Reporting

See [SECURITY.md](../SECURITY.md).
