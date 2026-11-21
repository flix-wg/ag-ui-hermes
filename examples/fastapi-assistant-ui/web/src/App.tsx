import {
  AssistantRuntimeProvider,
  ComposerPrimitive,
  MessagePrimitive,
  ThreadPrimitive,
  type ThreadHistoryAdapter,
  type ToolCallMessagePartComponent,
} from "@assistant-ui/react";
import {
  useAgUiInterrupts,
  useAgUiRuntime,
  useAgUiSubmitInterruptResponses,
} from "@assistant-ui/react-ag-ui";
import { HermesAgent } from "ag-ui-hermes";

// A random id kept in localStorage. The demo uses one for the user and one for the thread.
function storedId(key: string): string {
  try {
    const existing = localStorage.getItem(key);
    if (existing) return existing;
    const id = crypto.randomUUID();
    localStorage.setItem(key, id);
    return id;
  } catch {
    return crypto.randomUUID();
  }
}

const user = storedId("demo-user");

const agent = new HermesAgent({
  url: "/agent",
  threadId: storedId("demo-thread"),
  // Stands in for real auth. The server's authorize hook reads it.
  getHeaders: () => ({ "X-Demo-User": user }),
});

// No stored history: on every page load the runtime calls agent.connect(), which replays a
// run that is still going or restores the Hermes session's messages.
const history: ThreadHistoryAdapter = {
  async load() {
    return { messages: [], unstable_resume: true };
  },
  async append() {},
};

function newThread() {
  try {
    localStorage.removeItem("demo-thread");
  } catch {}
  location.reload();
}

export function App() {
  const runtime = useAgUiRuntime({ agent, adapters: { history } });
  return (
    <AssistantRuntimeProvider runtime={runtime}>
      <main>
        <header>
          <h1>Hermes chat</h1>
          <button type="button" onClick={newThread}>
            New thread
          </button>
        </header>
        <Thread />
      </main>
    </AssistantRuntimeProvider>
  );
}

function Thread() {
  return (
    <ThreadPrimitive.Root className="thread">
      <ThreadPrimitive.Viewport className="viewport">
        <ThreadPrimitive.Messages components={{ UserMessage, AssistantMessage }} />
        <Approvals />
      </ThreadPrimitive.Viewport>
      <ComposerPrimitive.Root className="composer">
        <ComposerPrimitive.Input placeholder="Message" rows={1} autoFocus />
        <ThreadPrimitive.If running={false}>
          <ComposerPrimitive.Send>Send</ComposerPrimitive.Send>
        </ThreadPrimitive.If>
        <ThreadPrimitive.If running>
          <ComposerPrimitive.Cancel>Stop</ComposerPrimitive.Cancel>
        </ThreadPrimitive.If>
      </ComposerPrimitive.Root>
    </ThreadPrimitive.Root>
  );
}

function UserMessage() {
  return (
    <MessagePrimitive.Root className="message user">
      <MessagePrimitive.Parts />
    </MessagePrimitive.Root>
  );
}

function AssistantMessage() {
  return (
    <MessagePrimitive.Root className="message assistant">
      <MessagePrimitive.Parts components={{ tools: { Fallback: ToolCall } }} />
    </MessagePrimitive.Root>
  );
}

const ToolCall: ToolCallMessagePartComponent = ({ toolName, result, status }) => (
  <div className="tool">
    <div className="tool-name">
      {toolName}
      {status.type === "running" ? " (running)" : ""}
    </div>
    {result !== undefined && <pre>{typeof result === "string" ? result : JSON.stringify(result)}</pre>}
  </div>
);

// Hermes approvals arrive as AG-UI interrupts. The adapter accepts {approved: true}
// as "once" and {approved: false} as "deny".
function Approvals() {
  const interrupts = useAgUiInterrupts();
  const submit = useAgUiSubmitInterruptResponses();
  return interrupts.map((interrupt) => (
    <div key={interrupt.id} className="approval">
      <p>{interrupt.message ?? "Approval required"}</p>
      <button
        type="button"
        onClick={() => submit([{ interruptId: interrupt.id, status: "resolved", payload: { approved: true } }])}
      >
        Approve once
      </button>
      <button
        type="button"
        onClick={() => submit([{ interruptId: interrupt.id, status: "resolved", payload: { approved: false } }])}
      >
        Deny
      </button>
    </div>
  ));
}
