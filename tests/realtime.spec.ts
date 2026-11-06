import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { WebSocket } from "ws";
import { startServer, type RunningServer } from "../src/server";
import { TestClient, connectWithRetry } from "../src/test-client";

let server: RunningServer;
const clients: TestClient[] = [];
const track = (c: TestClient) => (clients.push(c), c);

beforeEach(async () => {
  server = await startServer();
});
afterEach(async () => {
  clients.splice(0).forEach((c) => c.terminate());
  await server.close();
});

const auth = async (token = "good-token") => track(await TestClient.connect(server.url, { token }));

describe("authentication", () => {
  it("accepts a valid token", async () => {
    const c = track(await TestClient.connect(server.url));
    c.send({ type: "auth", token: "good-token" });
    expect(await c.waitFor((m) => m.type === "auth.ok")).toEqual({ type: "auth.ok", user: "alice" });
  });

  it("closes with 4401 on an invalid token", async () => {
    const c = track(await TestClient.connect(server.url));
    c.send({ type: "auth", token: "nope" });
    expect((await c.waitForClose()).code).toBe(4401);
  });

  it("closes with 4401 when sending before authenticating", async () => {
    const c = track(await TestClient.connect(server.url));
    c.send({ type: "subscribe", channel: "a" });
    expect((await c.waitForClose()).code).toBe(4401);
  });

  it("closes with 4408 when the client never authenticates", async () => {
    await server.close();
    server = await startServer({ authTimeoutMs: 100 });
    const c = track(await TestClient.connect(server.url));
    expect((await c.waitForClose()).code).toBe(4408);
  });
});

describe("pub/sub", () => {
  it("fans out to every subscriber including the sender", async () => {
    const [a, b] = [await auth("good-token"), await auth("other-token")];
    for (const c of [a, b]) {
      c.send({ type: "subscribe", channel: "room" });
      await c.waitFor((m) => m.type === "subscribed");
    }
    a.send({ type: "publish", channel: "room", data: "hi" });
    const [ma, mb] = await Promise.all([
      a.waitFor((m) => m.type === "message"),
      b.waitFor((m) => m.type === "message"),
    ]);
    expect(ma).toMatchObject({ channel: "room", data: "hi", from: "alice" });
    expect(mb).toEqual(ma);
  });

  it("isolates channels", async () => {
    const [a, b] = [await auth("good-token"), await auth("other-token")];
    a.send({ type: "subscribe", channel: "one" });
    b.send({ type: "subscribe", channel: "two" });
    await Promise.all([a.waitFor((m) => m.type === "subscribed"), b.waitFor((m) => m.type === "subscribed")]);
    a.send({ type: "publish", channel: "one", data: 1 });
    await a.waitFor((m) => m.type === "message");
    await b.expectNone((m) => m.type === "message");
  });

  it("stops delivery after unsubscribe", async () => {
    const [a, b] = [await auth("good-token"), await auth("other-token")];
    for (const c of [a, b]) c.send({ type: "subscribe", channel: "x" });
    await Promise.all([a.waitFor((m) => m.type === "subscribed"), b.waitFor((m) => m.type === "subscribed")]);
    b.send({ type: "unsubscribe", channel: "x" });
    await b.waitFor((m) => m.type === "unsubscribed");
    a.send({ type: "publish", channel: "x", data: "later" });
    await a.waitFor((m) => m.type === "message");
    await b.expectNone((m) => m.type === "message");
  });

  it("rejects publishing to a channel you have not joined", async () => {
    const a = await auth();
    a.send({ type: "publish", channel: "private", data: 1 });
    expect(await a.waitFor((m) => m.type === "error")).toMatchObject({ code: "not_subscribed" });
  });
});

describe("ordering and delivery", () => {
  it("delivers 200 messages in order with gap-free sequence numbers", async () => {
    const [pub, sub] = [await auth("good-token"), await auth("other-token")];
    for (const c of [pub, sub]) c.send({ type: "subscribe", channel: "stream" });
    await Promise.all([pub.waitFor((m) => m.type === "subscribed"), sub.waitFor((m) => m.type === "subscribed")]);

    const N = 200;
    for (let i = 0; i < N; i++) pub.send({ type: "publish", channel: "stream", data: i });
    const got: any[] = [];
    while (got.length < N) got.push(await sub.waitFor((m) => m.type === "message"));

    expect(got.map((m) => m.data)).toEqual(Array.from({ length: N }, (_, i) => i));
    expect(got.map((m) => m.seq)).toEqual(Array.from({ length: N }, (_, i) => i + 1));
  });

  it("keeps independent sequences per channel under concurrent publishers", async () => {
    const [a, b] = [await auth("good-token"), await auth("other-token")];
    for (const c of [a, b]) for (const ch of ["c1", "c2"]) c.send({ type: "subscribe", channel: ch });
    await a.waitFor((m) => m.type === "subscribed");
    await b.waitFor((m) => m.type === "subscribed");
    for (let i = 0; i < 20; i++) {
      a.send({ type: "publish", channel: "c1", data: i });
      b.send({ type: "publish", channel: "c2", data: i });
    }
    const seen: Record<string, number[]> = { c1: [], c2: [] };
    while (seen.c1.length < 20 || seen.c2.length < 20) {
      const m = await a.waitFor((x) => x.type === "message");
      seen[m.channel].push(m.seq);
    }
    for (const ch of ["c1", "c2"]) expect(seen[ch]).toEqual(Array.from({ length: 20 }, (_, i) => i + 1));
  });
});

describe("malformed input", () => {
  it("returns bad_json for non-JSON frames and keeps the socket open", async () => {
    const a = await auth();
    a.sendRaw("{not json");
    expect(await a.waitFor((m) => m.type === "error")).toMatchObject({ code: "bad_json" });
    a.send({ type: "ping" });
    expect(await a.waitFor((m) => m.type === "pong")).toBeTruthy();
  });

  it.each([[`[]`], [`42`], [`{"no":"type"}`], [`{"type":7}`]])("returns bad_shape for %s", async (frame) => {
    const a = await auth();
    a.sendRaw(frame);
    expect(await a.waitFor((m) => m.type === "error")).toMatchObject({ code: "bad_shape" });
  });

  it("returns unknown_type for unsupported message types", async () => {
    const a = await auth();
    a.send({ type: "teleport" });
    expect(await a.waitFor((m) => m.type === "error")).toMatchObject({ code: "unknown_type" });
  });

  it("closes with 1009 when a frame exceeds the max payload", async () => {
    const a = await auth();
    a.sendRaw(JSON.stringify({ type: "publish", channel: "x", data: "z".repeat(70 * 1024) }));
    expect((await a.waitForClose()).code).toBe(1009);
  });
});

describe("rate limiting", () => {
  it("closes with 4429 once the window budget is exceeded", async () => {
    await server.close();
    server = await startServer({ rateLimit: { max: 5, windowMs: 1000 } });
    const a = await auth(); // 1 message used
    for (let i = 0; i < 10; i++) a.send({ type: "ping" });
    expect((await a.waitForClose()).code).toBe(4429);
  });
});

describe("heartbeat", () => {
  it("keeps clients that answer protocol pings", async () => {
    await server.close();
    server = await startServer({ heartbeatMs: 80 });
    const a = await auth();
    await new Promise((r) => setTimeout(r, 400));
    expect(a.closed).toBeUndefined();
  });

  it("terminates clients that stop answering pings", async () => {
    await server.close();
    server = await startServer({ heartbeatMs: 80 });
    const dead = new WebSocket(server.url, { autoPong: false } as object); // autoPong: ws >= 8.17 (not yet in @types/ws)
    const closed = new Promise<void>((res) => dead.once("close", () => res()));
    await new Promise((res) => dead.once("open", res));
    await closed; // server must reap it after ~2 missed beats
    expect(dead.readyState).toBe(WebSocket.CLOSED);
  });
});

describe("reconnect", () => {
  it("recovers after the server drops all connections and resubscribes", async () => {
    const a = await auth();
    a.send({ type: "subscribe", channel: "news" });
    await a.waitFor((m) => m.type === "subscribed");

    server.dropAll();
    expect((await a.waitForClose()).code).toBe(1006);

    const { client, attempts } = await connectWithRetry(server.url, "good-token");
    track(client);
    expect(attempts).toBe(1);
    client.send({ type: "subscribe", channel: "news" });
    await client.waitFor((m) => m.type === "subscribed");
    client.send({ type: "publish", channel: "news", data: "back" });
    expect(await client.waitFor((m) => m.type === "message")).toMatchObject({ data: "back", seq: 1 });
  });

  it("retries with backoff until the server comes back on the same port", async () => {
    const port = server.port;
    await server.close();
    const pending = connectWithRetry(`ws://127.0.0.1:${port}`, "good-token", { retries: 8, baseMs: 30 });
    await new Promise((r) => setTimeout(r, 120)); // let a few attempts fail
    server = await startServer({ port });
    const { client, attempts } = await pending;
    track(client);
    expect(attempts).toBeGreaterThan(1);
  });

  it("gives up after the retry budget is exhausted", async () => {
    const port = server.port;
    await server.close();
    await expect(connectWithRetry(`ws://127.0.0.1:${port}`, "good-token", { retries: 2, baseMs: 5 })).rejects.toThrow(
      /failed after 2 attempts/,
    );
    server = await startServer();
  });
});
