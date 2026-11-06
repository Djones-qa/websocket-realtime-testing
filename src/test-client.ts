import { WebSocket, type RawData } from "ws";

export interface CloseInfo {
  code: number;
  reason: string;
}

/**
 * Promise-friendly WebSocket client for tests. Records every received frame so
 * assertions never race with delivery, and exposes waitFor/expectNone helpers.
 */
export class TestClient {
  readonly received: any[] = [];
  closed?: CloseInfo;
  private ws!: WebSocket;
  private waiters: Array<() => void> = [];
  private consumed = 0;

  static async connect(url: string, opts: { token?: string } = {}): Promise<TestClient> {
    const c = new TestClient();
    await c.open(url);
    if (opts.token) {
      c.send({ type: "auth", token: opts.token });
      await c.waitFor((m) => m.type === "auth.ok");
    }
    return c;
  }

  open(url: string): Promise<void> {
    this.ws = new WebSocket(url);
    this.ws.on("message", (raw: RawData) => {
      let parsed: any;
      try {
        parsed = JSON.parse(raw.toString());
      } catch {
        parsed = { type: "__raw", raw: raw.toString() };
      }
      this.received.push(parsed);
      this.notify();
    });
    this.ws.on("close", (code, reason) => {
      this.closed = { code, reason: reason.toString() };
      this.notify();
    });
    return new Promise((res, rej) => {
      this.ws.once("open", () => res());
      this.ws.once("error", rej);
    });
  }

  send(msg: unknown) {
    this.ws.send(typeof msg === "string" ? msg : JSON.stringify(msg));
  }

  sendRaw(data: string | Buffer) {
    this.ws.send(data);
  }

  private notify() {
    const w = this.waiters;
    this.waiters = [];
    w.forEach((f) => f());
  }

  private waitChange(ms: number): Promise<void> {
    return new Promise((res) => {
      const t = setTimeout(res, ms);
      this.waiters.push(() => {
        clearTimeout(t);
        res();
      });
    });
  }

  /** Resolve with the next not-yet-consumed frame matching the predicate. */
  async waitFor(pred: (m: any) => boolean, timeoutMs = 2000): Promise<any> {
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      while (this.consumed < this.received.length) {
        const m = this.received[this.consumed++];
        if (pred(m)) return m;
      }
      const left = deadline - Date.now();
      if (left <= 0) throw new Error(`timed out waiting for frame; got ${JSON.stringify(this.received)}`);
      await this.waitChange(left);
    }
  }

  async waitForClose(timeoutMs = 3000): Promise<CloseInfo> {
    const deadline = Date.now() + timeoutMs;
    while (!this.closed) {
      const left = deadline - Date.now();
      if (left <= 0) throw new Error("timed out waiting for close");
      await this.waitChange(left);
    }
    return this.closed;
  }

  /** Assert no matching frame arrives within the window. */
  async expectNone(pred: (m: any) => boolean, windowMs = 200) {
    await new Promise((r) => setTimeout(r, windowMs));
    const hit = this.received.slice(this.consumed).find(pred);
    if (hit) throw new Error(`unexpected frame: ${JSON.stringify(hit)}`);
  }

  close() {
    this.ws.close();
  }

  terminate() {
    this.ws.terminate();
  }

  /** Answer protocol pings or not (for heartbeat tests). */
  get raw(): WebSocket {
    return this.ws;
  }
}

/** Reconnect with exponential backoff; returns the connected client and attempts used. */
export async function connectWithRetry(
  url: string,
  token: string,
  { retries = 5, baseMs = 25 } = {},
): Promise<{ client: TestClient; attempts: number }> {
  let last: unknown;
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      return { client: await TestClient.connect(url, { token }), attempts: attempt };
    } catch (e) {
      last = e;
      await new Promise((r) => setTimeout(r, baseMs * 2 ** (attempt - 1)));
    }
  }
  throw new Error(`failed after ${retries} attempts: ${String(last)}`);
}
