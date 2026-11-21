import { describe, expect, it } from "vitest";
import type { Message } from "@ag-ui/client";
import { fromAgUiMessages } from "@assistant-ui/react-ag-ui";
// @ts-expect-error internal module: the controller behind useAgUiRuntime, driven without React
import { AgUiThreadController } from "../node_modules/@assistant-ui/react-ag-ui/dist/runtime/AgUiThreadController.js";
import { HermesAgent } from "./index.js";
import cases from "./connect-cases.json";

// Streams the Python agent's connect() emits for a client that held part of the run
// (regenerate with `uv run python -m tests.connect_cases`).
type Case = { held: Message[]; events: object[] };

const sse = (events: object[]) =>
  new Response(events.map((e) => `data: ${JSON.stringify(e)}\n\n`).join(""), {
    headers: { "Content-Type": "text/event-stream" },
  });

// What a history adapter hands the runtime on load.
const threadMessages = (messages: Message[]) =>
  fromAgUiMessages(messages).map((m: any) => ({
    createdAt: new Date(0),
    attachments: [],
    ...(m.role === "assistant" ? { status: { type: "complete", reason: "unknown" } } : {}),
    ...m,
    content: typeof m.content === "string" ? [{ type: "text", text: m.content }] : m.content,
    metadata: { unstable_state: null, unstable_annotations: [], unstable_data: [], steps: [], custom: {}, ...m.metadata },
  }));

async function rejoin({ held, events }: Case) {
  // Serve the recorded stream only to a connect request that carries what the client holds.
  const fetch = async (url: string, init: RequestInit) => {
    const body = JSON.parse(String(init.body));
    expect(url).toBe("https://app.test/agent/connect");
    expect(body.threadId).toBe("t1");
    expect(body.runId).toEqual(expect.any(String));
    expect(body.messages.map((m: Message) => [m.id, m.role, m.content || undefined])).toEqual(
      held.map((m) => [m.id, m.role, m.content || undefined]),
    );
    return sse(events);
  };
  const agent = new HermesAgent({ url: "https://app.test/agent", threadId: "t1", fetch });
  const errors: unknown[] = [];
  const controller = new AgUiThreadController({
    agent,
    logger: { error: (...args: unknown[]) => errors.push(args) },
    showThinking: true,
    onError: (error: unknown) => errors.push(error),
  });
  controller.applyExternalMessages(threadMessages(held));
  await controller.resumeInFlightRun(controller.getMessages());
  expect(errors).toEqual([]);

  const ui = controller.getMessages().filter((m: any) => m.role === "assistant");
  return {
    agentText: agent.messages.filter((m) => m.role === "assistant" && m.content).map((m) => m.content),
    agentResults: agent.messages.filter((m) => m.role === "tool").map((m) => m.content),
    uiText: ui.flatMap((m: any) => m.content.filter((p: any) => p.type === "text").map((p: any) => p.text)),
    uiResults: ui.flatMap((m: any) =>
      m.content.filter((p: any) => p.type === "tool-call").map((p: any) => p.result),
    ),
    uiIds: controller.getMessages().map((m: any) => m.id),
  };
}

describe("connect replay in @ag-ui/client and assistant-ui", () => {
  const all = cases as unknown as Record<string, Case>;

  it.each([
    ["partial", ["Hello"], []],
    ["segment_then_live", ["Let me check.", "Hello"], ["on"]],
    ["held_tools", ["Hello"], ["done a", "done b"]],
    ["early_diverged", ["Let me check.", "Hello"], ["on"]],
  ])("%s", async (name, text, results) => {
    const out = await rejoin(all[name]!);
    expect(out.agentText).toEqual(text);
    expect(out.uiText).toEqual(text);
    expect(out.agentResults).toEqual(results);
    expect(out.uiResults).toEqual(results);
    expect(new Set(out.uiIds).size).toBe(out.uiIds.length);
  });
});
