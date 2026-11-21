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

const agentWithAsync = (fetch: ReturnType<typeof vi.fn>) =>
  new HermesAgent({
    url: "https://app.test/agent",
    getHeaders: async () => ({ "X-CSRF-Token": "late" }),
    fetch,
  });

describe("HermesAgent", () => {
  it("awaits an async getHeaders on run and sends GET capabilities", async () => {
    const fetch = vi.fn(async (url: string) =>
      url.endsWith("/capabilities") ? Response.json({ identity: { name: "hermes" } }) : sse(replay),
    );
    const agent = agentWithAsync(fetch);

    await agent.runAgent();
    expect((fetch.mock.calls[0] as unknown as [string, RequestInit])[1].headers).toMatchObject({ "X-CSRF-Token": "late", "Content-Type": "application/json" });

    expect(await agent.getCapabilities()).toEqual({ identity: { name: "hermes" } });
    const [url, init] = fetch.mock.calls[1] as unknown as [string, RequestInit];
    expect(url).toBe("https://app.test/agent/capabilities");
    expect(init).toMatchObject({ method: "GET", headers: { "X-CSRF-Token": "late" } });
  });

  it("rejects getCapabilities on a non-2xx response", async () => {
    const agent = agentWithAsync(vi.fn(async () => new Response("no", { status: 404 })));
    await expect(agent.getCapabilities()).rejects.toThrow("HTTP 404");
  });

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

  it("stops the server run it joined on an explicit abort", async () => {
    const fetch = vi.fn(async (url: string) => (url.endsWith("/stop") ? new Response("{}") : sse(replay)));
    const agent = agentWith(fetch);
    agent.abortRun(); // no run yet: nothing to stop
    expect(fetch).not.toHaveBeenCalled();
    await agent.connectAgent({ runId: "r9" });
    agent.abortRun();
    await vi.waitFor(() => expect(fetch).toHaveBeenCalledTimes(2));

    const [url, init] = fetch.mock.calls[1] as unknown as [string, RequestInit];
    expect(url).toBe("https://app.test/agent/stop");
    expect(init.headers).toMatchObject({ "X-Static": "1", "X-CSRF-Token": "csrf" });
    expect(JSON.parse(init.body as string)).toEqual({ threadId: "t1", runId: "r9" });
  });
});
