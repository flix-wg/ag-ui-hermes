import { describe, expect, it, vi } from "vitest";
import { HermesAgent } from "./index.js";

const sse = (events: object[]) =>
  new Response(events.map((e) => `data: ${JSON.stringify(e)}\n\n`).join(""), {
    headers: { "Content-Type": "text/event-stream" },
  });

const replay = [
  { type: "RUN_STARTED", threadId: "t1", runId: "r1" },
  { type: "TEXT_MESSAGE_START", messageId: "run_1:msg:0", role: "assistant" },
  { type: "TEXT_MESSAGE_CONTENT", messageId: "run_1:msg:0", delta: "Hello again" },
  { type: "TEXT_MESSAGE_END", messageId: "run_1:msg:0" },
  { type: "RUN_FINISHED", threadId: "t1", runId: "r1" },
];

const agentWith = (fetch: ReturnType<typeof vi.fn>) =>
  new HermesAgent({
    url: "https://app.test/agent/",
    threadId: "t1",
    headers: { "X-Static": "1" },
    getHeaders: () => ({ "X-CSRF-Token": "csrf" }),
    fetch,
  });

describe("HermesAgent", () => {
  it("rejoins through the connect endpoint with injected headers", async () => {
    const fetch = vi.fn(async () => sse(replay));
    const agent = agentWith(fetch);

    const { newMessages } = await agent.connectAgent();

    const [url, init] = fetch.mock.calls[0] as unknown as [string, RequestInit];
    expect(url).toBe("https://app.test/agent/connect");
    expect(init.headers).toMatchObject({ "X-Static": "1", "X-CSRF-Token": "csrf" });
    expect(JSON.parse(init.body as string)).toMatchObject({ threadId: "t1" });
    expect(newMessages).toMatchObject([{ id: "run_1:msg:0", content: "Hello again" }]);
  });

  it("stops the server run on an explicit abort", () => {
    const fetch = vi.fn(async () => new Response("{}"));
    agentWith(fetch).abortRun();

    const [url, init] = fetch.mock.calls[0] as unknown as [string, RequestInit];
    expect(url).toBe("https://app.test/agent/stop");
    expect(init.headers).toMatchObject({ "X-Static": "1", "X-CSRF-Token": "csrf" });
    expect(JSON.parse(init.body as string)).toEqual({ threadId: "t1" });
  });
});
