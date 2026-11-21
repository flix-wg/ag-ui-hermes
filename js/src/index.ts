import {
  HttpAgent,
  runHttpRequest,
  transformHttpEventStream,
  type AgentCapabilities,
  type BaseEvent,
  type HttpAgentConfig,
  type RunAgentInput,
} from "@ag-ui/client";
import type { Observable } from "rxjs";

export interface HermesAgentConfig extends HttpAgentConfig {
  /** Rejoin endpoint. Defaults to `${url}/connect`. */
  connectUrl?: string;
  /** Stop endpoint. Defaults to `${url}/stop`. */
  stopUrl?: string;
  /** Headers resolved before every request (run, connect, stop, capabilities), e.g. a rotating CSRF token. May be async. */
  getHeaders?: () => Record<string, string> | Promise<Record<string, string>>;
}

export type { AgentCapabilities };

/**
 * HttpAgent for an ag-ui-hermes endpoint. It implements `connect()`, so
 * runtimes such as `@assistant-ui/react-ag-ui` rejoin a run that is still in
 * flight instead of starting a new one. Leaving a thread only detaches;
 * `abortRun()` (an explicit cancel) also stops the Hermes run on the server,
 * naming the last run or connect so a late stop cannot cancel a newer run.
 */
export class HermesAgent extends HttpAgent {
  connectUrl: string;
  stopUrl: string;
  getHeaders?: HermesAgentConfig["getHeaders"];
  private baseUrl: string;
  private rawFetch: HttpAgent["fetch"];
  private lastRunId?: string;

  constructor(config: HermesAgentConfig) {
    super(config);
    const base = config.url.replace(/\/+$/, "");
    this.connectUrl = config.connectUrl ?? `${base}/connect`;
    this.stopUrl = config.stopUrl ?? `${base}/stop`;
    this.getHeaders = config.getHeaders;
    this.baseUrl = base;
    this.rawFetch = this.fetch;
    this.fetch = (url, init) => this.withDynamicHeaders(url, init);
  }

  // HttpAgent builds request options synchronously, so dynamic headers are merged at the fetch layer.
  // A method, not an instance field: clone() uses Object.create and would skip field initializers.
  private async withDynamicHeaders(url: Parameters<HttpAgent["fetch"]>[0], init: Parameters<HttpAgent["fetch"]>[1]) {
    const dynamic = await this.getHeaders?.();
    return this.rawFetch(url, dynamic ? { ...init, headers: { ...(init.headers as Record<string, string>), ...dynamic } } : init);
  }

  /** Fetches `GET <url>/capabilities`. */
  async getCapabilities(): Promise<AgentCapabilities> {
    const res = await this.fetch(`${this.baseUrl}/capabilities`, {
      method: "GET",
      headers: { ...this.headers, Accept: "application/json" },
    });
    if (!res.ok) throw new Error(`capabilities request failed: HTTP ${res.status}`);
    return (await res.json()) as AgentCapabilities;
  }

  protected override requestInit(input: RunAgentInput): RequestInit {
    this.lastRunId = input.runId;
    return super.requestInit(input);
  }

  protected override connect(input: RunAgentInput): Observable<BaseEvent> {
    if (this.abortController.signal.aborted) {
      this.abortController = new AbortController();
    }
    return transformHttpEventStream(
      runHttpRequest(() => this.fetch(this.connectUrl, this.requestInit(input))),
    );
  }

  override abortRun(): void {
    // Without a run of ours there is nothing the server would let us stop.
    if (this.lastRunId !== undefined) {
      void this.fetch(this.stopUrl, {
        method: "POST",
        headers: { ...this.headers, "Content-Type": "application/json" },
        body: JSON.stringify({ threadId: this.threadId, runId: this.lastRunId }),
        keepalive: true,
      }).catch(() => undefined);
    }
    super.abortRun();
  }

  override clone(): HermesAgent {
    const cloned = super.clone() as HermesAgent;
    cloned.connectUrl = this.connectUrl;
    cloned.stopUrl = this.stopUrl;
    cloned.getHeaders = this.getHeaders;
    cloned.baseUrl = this.baseUrl;
    cloned.rawFetch = this.rawFetch;
    cloned.fetch = (url, init) => cloned.withDynamicHeaders(url, init);
    return cloned;
  }
}
