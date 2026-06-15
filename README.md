# ratatoskr-acorn

TypeScript SDK for the [Ratatoskr](https://github.com/SrRobert0/Ratatoskr) queue API.

[![npm version](https://img.shields.io/npm/v/ratatoskr-acorn)](https://www.npmjs.com/package/ratatoskr-acorn)
[![npm downloads](https://img.shields.io/npm/dm/ratatoskr-acorn)](https://www.npmjs.com/package/ratatoskr-acorn)
[![license](https://img.shields.io/npm/l/ratatoskr-acorn)](./LICENSE)
![node >=18](https://img.shields.io/node/v/ratatoskr-acorn)

---

## Features

- **Continuous consumption** with long polling and configurable concurrency
- **Auto ack/nack** — messages are deleted on success, retried on failure
- **Graceful shutdown** — drains in-flight handlers before stopping
- **Manual controls** — `receive`, `ack`, `peek`, `changeVisibility` for custom flows
- **Zero dependencies** — uses native `fetch` (Node ≥ 18)
- **Dual ESM + CJS** — works in both module systems with full TypeScript types

---

## Installation

```bash
npm install ratatoskr-acorn
```

> Requires **Node.js 18+**.

---

## Quick start

```ts
import { RatatoskrAcorn } from 'ratatoskr-acorn';

const acorn = new RatatoskrAcorn({
  baseUrl: 'https://your-ratatoskr-server.com',
  apiKey: process.env.RATATOSKR_API_KEY!,
});

const worker = acorn.subscribe('your-queue-id', async (msg) => {
  console.log('Received:', msg.body);
});

worker.start();
```

---

## API

### `new RatatoskrAcorn(options)`

| Option | Type | Default | Description |
|---|---|---|---|
| `baseUrl` | `string` | — | Base URL of your Ratatoskr server |
| `apiKey` | `string` | — | API key sent as `x-api-key` header |
| `timeout` | `number` | `35000` | Per-request timeout in ms |

---

### `acorn.subscribe(queueId, handler, options?)` → `Worker`

Starts a continuous polling loop. Each received message is passed to `handler`. On success, the message is automatically acknowledged (deleted). On failure, it returns to the queue after the visibility timeout.

```ts
const worker = acorn.subscribe('queue-id', async (msg) => {
  const payload = JSON.parse(msg.body);
  await processOrder(payload);
}, {
  concurrency: 5,       // up to 5 handlers running in parallel
  waitTimeSeconds: 10,  // long polling window per cycle (0–20)
  onError: (err, msg) => {
    console.error('Handler failed:', err.message, msg?.id);
  },
});

worker.start();

// Graceful shutdown
process.on('SIGTERM', async () => {
  await worker.stop(); // waits for in-flight handlers to finish
  process.exit(0);
});
```

**Subscribe options:**

| Option | Type | Default | Description |
|---|---|---|---|
| `concurrency` | `number` | `1` | Max parallel handlers |
| `waitTimeSeconds` | `number` | `10` | Long polling seconds per cycle (0–20) |
| `maxNumberOfMessages` | `number` | `concurrency` | Messages fetched per poll (1–10) |
| `onError` | `(err, msg \| null) => void` | `console.error` | Called on handler errors or poll failures |

**Worker methods:**

| Method | Description |
|---|---|
| `worker.start()` | Starts the polling loop (idempotent) |
| `worker.stop()` | Stops gracefully, awaiting all in-flight handlers |

---

### `acorn.send(queueId, input)` → `Promise<Message>`

Sends a message to a queue.

```ts
await acorn.send('queue-id', {
  body: JSON.stringify({ orderId: 123 }),
  attributes: { source: 'api' },
  delaySeconds: 5, // optional delivery delay (0–900)
});
```

**Input fields:**

| Field | Type | Description |
|---|---|---|
| `body` | `string` | Message body |
| `attributes` | `Record<string, string>` | Optional metadata |
| `delaySeconds` | `number` | Delivery delay in seconds (0–900) |
| `messageGroupId` | `string` | FIFO queues only |
| `messageDeduplicationId` | `string` | FIFO queues (when content-based dedup is off) |

---

### `acorn.receive(queueId, options?)` → `Promise<ReceiveResult>`

Manually polls messages. Prefer `subscribe()` for continuous consumption.

```ts
const { messages } = await acorn.receive('queue-id', {
  maxNumberOfMessages: 5,
  waitTimeSeconds: 10,
  visibilityTimeout: 30,
});
```

---

### `acorn.ack(queueId, receiptHandle)` → `Promise<void>`

Manually acknowledges (deletes) a message after processing.

```ts
await acorn.ack('queue-id', msg.receiptHandle!);
```

---

### `acorn.peek(queueId, options?)` → `Promise<ReceiveResult>`

Inspects messages **without** consuming them. Does not change status, `receiptHandle`, or `receiveCount`.

```ts
const { messages } = await acorn.peek('queue-id', { limit: 10 });
```

---

### `acorn.changeVisibility(queueId, receiptHandle, visibilityTimeout)` → `Promise<void>`

Extends the visibility timeout of an in-flight message. Useful for long-running handlers that need more time.

```ts
await acorn.changeVisibility('queue-id', msg.receiptHandle!, 120);
```

---

### `acorn.health()` → `Promise<HealthResult>`

Checks if the Ratatoskr server is reachable.

```ts
const { status } = await acorn.health();
console.log(status); // 'ok'
```

---

## Error handling

All API errors throw a `RatatoskrError`:

```ts
import { RatatoskrError } from 'ratatoskr-acorn';

try {
  await acorn.send('queue-id', { body: 'hello' });
} catch (err) {
  if (err instanceof RatatoskrError) {
    console.error(err.message);    // human-readable message
    console.error(err.statusCode); // HTTP status code
    console.error(err.code);       // error code string
    console.error(err.details);    // optional extra details
  }
}
```

---

## Message shape

```ts
interface Message {
  id: string;
  body: string;
  attributes: Record<string, string>;
  messageGroupId: string | null;
  messageDeduplicationId: string | null;
  receiptHandle: string | null;  // required for ack/changeVisibility
  receiveCount: number;
  status: 'delayed' | 'available' | 'in_flight' | 'archived';
  createdAt: string;
  availableAt: string;
  archivedAt: string | null;
}
```

---

## License

MIT
