import { createServer, type Server } from "node:http";
import { WebSocketServer, WebSocket } from "ws";

/**
 * Reference real-time server under test.
 *
 * Protocol (JSON text frames):
 *   -> {type:"auth", token}              <- {type:"auth.ok", user} | close(4401)
 *   -> {type:"subscribe", channel}       <- {type:"subscribed", channel}
 *   -> {type:"unsubscribe", channel}     <- {type:"unsubscribed", channel}
 *   -> {type:"publish", channel, data}   <- fan-out {type:"message", channel, seq, data, from}
 *   -> {type:"ping"}                     <- {type:"pong"}
 * Errors: {type:"error", code, message}. Close codes: 4401 auth, 4408 auth timeout, 4429 rate limit.
 */
export interface ServerOptions {
  /** token -> username */
  tokens?: Record<string, string>;
  authTimeoutMs?: number;
  /** max messages per rolling window before the socket is closed with 4429 */
  rateLimit?: { max: number; windowMs: number };
  /** drop clients that do not answer protocol-level pings */
  heartbeatMs?: number;
  maxPayloadBytes?: number;
  /** fixed port (default 0 = ephemeral); used to simulate a server coming back */
  port?: number;
}

export interface RunningServer {
  port: number;
  url: string;
  wss: WebSocketServer;
  close(): Promise<void>;
  /** forcibly drop every client (simulates a network failure / deploy) */
  dropAll(): void;
}

interface ClientState {
  user?: string;
  channels: Set<string>;
  hits: number[];
  alive: boolean;
}

export async function startServer(opts: ServerOptions = {}): Promise<RunningServer> {
  const tokens = opts.tokens ?? { "good-token": "alice", "other-token": "bob" };
  const authTimeoutMs = opts.authTimeoutMs ?? 1000;
  const http: Server = createServer();
  const wss = new WebSocketServer({ server: http, maxPayload: opts.maxPayloadBytes ?? 64 * 1024 });
  const state = new Map<WebSocket, ClientState>();
  const seqByChannel = new Map<string, number>();

  const send = (ws: WebSocket, msg: unknown) => {
    if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(msg));
  };
  const err = (ws: WebSocket, code: string, message: string) => send(ws, { type: "error", code, message });

  wss.on("connection", (ws) => {
    const s: ClientState = { channels: new Set(), hits: [], alive: true };
    state.set(ws, s);

    const authTimer = setTimeout(() => {
      if (!s.user) ws.close(4408, "auth timeout");
    }, authTimeoutMs);

    // protocol violations (e.g. oversized frame) surface as socket errors; ws closes with the right code
    ws.on("error", () => undefined);
    ws.on("pong", () => (s.alive = true));
    ws.on("close", () => {
      clearTimeout(authTimer);
      state.delete(ws);
    });

    ws.on("message", (raw) => {
      if (opts.rateLimit) {
        const now = Date.now();
        s.hits = s.hits.filter((t) => now - t < opts.rateLimit!.windowMs);
        s.hits.push(now);
        if (s.hits.length > opts.rateLimit.max) return ws.close(4429, "rate limited");
      }

      let msg: any;
      try {
        msg = JSON.parse(raw.toString());
      } catch {
        return err(ws, "bad_json", "frame is not valid JSON");
      }
      if (typeof msg !== "object" || msg === null || typeof msg.type !== "string") {
        return err(ws, "bad_shape", "missing string field 'type'");
      }

      if (msg.type === "auth") {
        const user = tokens[msg.token];
        if (!user) return ws.close(4401, "unauthorized");
        s.user = user;
        clearTimeout(authTimer);
        return send(ws, { type: "auth.ok", user });
      }
      if (!s.user) return ws.close(4401, "unauthorized");

      switch (msg.type) {
        case "ping":
          return send(ws, { type: "pong" });
        case "subscribe":
          if (typeof msg.channel !== "string") return err(ws, "bad_channel", "channel required");
          s.channels.add(msg.channel);
          return send(ws, { type: "subscribed", channel: msg.channel });
        case "unsubscribe":
          s.channels.delete(msg.channel);
          return send(ws, { type: "unsubscribed", channel: msg.channel });
        case "publish": {
          if (typeof msg.channel !== "string") return err(ws, "bad_channel", "channel required");
          if (!s.channels.has(msg.channel)) return err(ws, "not_subscribed", "subscribe before publishing");
          const seq = (seqByChannel.get(msg.channel) ?? 0) + 1;
          seqByChannel.set(msg.channel, seq);
          for (const [peer, ps] of state) {
            if (ps.channels.has(msg.channel)) {
              send(peer, { type: "message", channel: msg.channel, seq, data: msg.data, from: s.user });
            }
          }
          return;
        }
        default:
          return err(ws, "unknown_type", `unknown type '${msg.type}'`);
      }
    });
  });

  let hb: NodeJS.Timeout | undefined;
  if (opts.heartbeatMs) {
    hb = setInterval(() => {
      for (const [ws, s] of state) {
        if (!s.alive) {
          ws.terminate();
          continue;
        }
        s.alive = false;
        ws.ping();
      }
    }, opts.heartbeatMs);
  }

  await new Promise<void>((res) => http.listen(opts.port ?? 0, "127.0.0.1", res));
  const port = (http.address() as { port: number }).port;

  return {
    port,
    url: `ws://127.0.0.1:${port}`,
    wss,
    dropAll: () => state.forEach((_, ws) => ws.terminate()),
    close: () =>
      new Promise<void>((res) => {
        if (hb) clearInterval(hb);
        wss.clients.forEach((c) => c.terminate());
        wss.close(() => http.close(() => res()));
      }),
  };
}
