# websocket-realtime-testing

A TypeScript + Vitest framework for testing WebSocket / real-time systems, shipped with a small reference server so every pattern runs offline with no Docker and no external services.

![CI](https://github.com/Djones-qa/websocket-realtime-testing/actions/workflows/ci.yml/badge.svg)
![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)

## Why

REST suites do not tell you whether a chat, dashboard or trading feed survives dropped sockets, slow consumers or out-of-order frames. This repo shows how to test those behaviours deterministically, without `sleep()`-driven flakiness.

## What is covered

| Area | Checks |
| --- | --- |
| Authentication | valid token, bad token (close 4401), unauthenticated send, auth timeout (4408) |
| Pub/Sub | fan-out, channel isolation, unsubscribe, publish-without-join rejection |
| Ordering | 200-message burst in order, gap-free sequence numbers, per-channel sequences with concurrent publishers |
| Malformed input | invalid JSON, wrong shape, unknown type, oversized frame (close 1009) |
| Rate limiting | window budget exceeded (close 4429) |
| Heartbeat | live clients kept, unresponsive clients reaped |
| Reconnect | server drop (1006), resubscribe, exponential backoff, retry exhaustion |

## Key design: `TestClient`

`src/test-client.ts` records every frame and exposes event-driven helpers instead of fixed sleeps:

- `waitFor(predicate)` resolves on the next matching frame, with a timeout and a dump of what was received
- `waitForClose()` returns the close code and reason
- `expectNone(predicate, windowMs)` asserts a frame does not arrive
- `connectWithRetry()` models client reconnect with exponential backoff

## Quick start

```bash
npm install
npm run typecheck
npm test
```

Requires Node 20+.

## Project structure

```
src/
  server.ts        reference real-time server (auth, pub/sub, heartbeat, rate limit)
  test-client.ts   promise-based WebSocket test client
tests/
  realtime.spec.ts 23 tests across the areas above
.github/workflows/ci.yml
```

## Pointing it at your own service

Replace `startServer()` in the spec with your service URL and adapt the message shapes in the predicates. The `TestClient` is protocol-agnostic apart from JSON parsing.

## Author

**Darrius Jones**, QA Automation Engineer

- GitHub: [Djones-qa](https://github.com/Djones-qa)

## License

Released under the [MIT License](LICENSE).

## Topics

`websocket` `realtime-testing` `qa-automation` `vitest` `typescript` `test-automation` `pubsub` `reconnect` `heartbeat` `quality-engineering`
