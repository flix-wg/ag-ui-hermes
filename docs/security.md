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
thread, and before `capabilities` (with an empty `thread_id`, so check only the caller). Check
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
`HermesAgent` accepts only `allowed_choices` (default `once` and `deny`, and `deny` is always
allowed). A client cannot widen that: any other choice is refused with HTTP 400 and nothing is
sent to Hermes. Add `session` or `always` only if you want users to grant standing permission.

Each pending interrupt is answered at most once, and only interrupts the thread is actually
waiting on are forwarded.

## What reaches the system prompt

Hermes `instructions` become part of the system prompt for that run. They are built from:

<!-- TODO(fix-lane): option names and the exact delimiter/label. -->
- the `instructions` your `run_body` hook returns: trusted, yours;
- client system and developer messages: ignored unless you set `client_system_messages=True`,
  since any client can send them;
- `context` and `state`: placed in a delimited block labelled as client-supplied data, so the
  model can tell them apart from your instructions. Each can be switched off. Labelling reduces
  but does not prevent prompt injection; treat both as user input.

`forwardedProps` can only set `model`, `provider` or `model_options`, and only the keys you list
in `forwarded_props` (none by default).

## Images

<!-- TODO(fix-lane): option names and defaults. -->
By default only inline `data:image/...` images up to `max_image_bytes` (5 MiB) are accepted.
`http(s)` image URLs would make Hermes or its model provider fetch a client-chosen URL, so they
are refused unless you set `allow_image_urls=True`. Everything else is refused with HTTP 400.

## Tool output and history

Tool previews and results go to the client as Hermes sends them. Use `tool_filter` to hide or
rewrite tools whose output the user should not see. It applies to live and replayed runs and
<!-- TODO(fix-lane): confirm once merged. -->
to the session history `connect` restores.

## Errors and logs

`RUN_ERROR` and the endpoint's error bodies carry a fixed message and a stable `code`, never
exception text, Hermes URLs or Hermes error bodies. The cause goes to the `ag_ui_hermes` logger
and to `on_error`. Those logs can contain thread ids, Hermes run ids and Hermes error messages,
so treat them like application logs with user data. The adapter does not log message content.

## Resource use

<!-- TODO(fix-lane): confirm the eviction rule. -->
`InMemoryThreadStore` keeps at most `max_threads` records and never evicts a thread with a live
run. A rejoin replays at most Hermes' 1000 events per run. Status polling of a run whose stream
is gone stops after `poll_timeout`. Rate limiting and request size limits are up to your server.

## Reporting

See [SECURITY.md](../SECURITY.md).
