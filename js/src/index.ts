import {
  HttpAgent,
  runHttpRequest,
  transformHttpEventStream,
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
  /** Headers resolved for every request, e.g. a rotating CSRF token. */
  getHeaders?: () => Record<string, string>;
}

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
  getHeaders?: () => Record<string, string>;
  private lastRunId?: string;

  constructor(config: HermesAgentConfig) {
    super(config);
    const base = config.url.replace(/\/+$/, "");
    this.connectUrl = config.connectUrl ?? `${base}/connect`;
    this.stopUrl = config.stopUrl ?? `${base}/stop`;
    this.getHeaders = config.getHeaders;
  }

  private requestHeaders(): Record<string, string> {
    return { ...this.headers, ...this.getHeaders?.() };
  }

  protected override requestInit(input: RunAgentInput): RequestInit {
    this.lastRunId = input.runId;
    const init = super.requestInit(input);
    return {
      ...init,
      headers: { ...(init.headers as Record<string, string>), ...this.getHeaders?.() },
    };
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
        headers: { ...this.requestHeaders(), "Content-Type": "application/json" },
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
    return cloned;
  }
}
