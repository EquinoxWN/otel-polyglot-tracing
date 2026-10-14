// Worker: the CONSUMER span continues the producer's trace from the message headers, and the
// shipment call to the gateway carries it on over HTTP.
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { createServer, type IncomingHttpHeaders, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { after, afterEach, before, beforeEach, describe, it } from "node:test";
import { SpanKind, SpanStatusCode, trace } from "@opentelemetry/api";
import { registerInstrumentations } from "@opentelemetry/instrumentation";
import { UndiciInstrumentation } from "@opentelemetry/instrumentation-undici";
import { InMemorySpanExporter, NodeTracerProvider, SimpleSpanProcessor, type ReadableSpan } from "@opentelemetry/sdk-trace-node";
import { SqliteQueue } from "../src/queue.js";
import { processNext, runWorker, type WorkerOptions } from "../src/worker.js";

const exporter = new InMemorySpanExporter();
const provider = new NodeTracerProvider({ spanProcessors: [new SimpleSpanProcessor(exporter)] });
provider.register();
registerInstrumentations({ tracerProvider: provider, instrumentations: [new UndiciInstrumentation()] });

const TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736";
const PRODUCER_ID = "00f067aa0ba902b7";
const ORDER = { order_id: "0123456789abcdef", sku: "SKU-1", quantity: 2 };

/** One request the fake gateway received. */
interface Received {
  url: string;
  headers: IncomingHttpHeaders;
  body: string;
}

let gateway: Server;
let gatewayStatus = 204;
let received: Received[] = [];
let dir: string;
let dbPath: string;
let queue: SqliteQueue;
let opts: WorkerOptions;

before(async () => {
  gateway = createServer((req, res) => {
    let body = "";
    req.on("data", (c: Buffer) => (body += c.toString()));
    req.on("end", () => {
      received.push({ url: req.url ?? "", headers: req.headers, body });
      res.statusCode = gatewayStatus;
      res.end();
    });
  });
  await new Promise<void>((resolve) => gateway.listen(0, "127.0.0.1", resolve));
});

after(async () => {
  await new Promise((resolve) => gateway.close(resolve));
  await provider.shutdown();
});

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "worker-test-"));
  dbPath = join(dir, "queue.db");
  queue = new SqliteQueue(dbPath);
  const port = (gateway.address() as AddressInfo).port;
  opts = { queue, tracer: trace.getTracer("test"), gatewayUrl: new URL(`http://127.0.0.1:${port}`), maxAttempts: 2 };
  exporter.reset();
  received = [];
  gatewayStatus = 204;
});

afterEach(() => {
  queue.close();
  rmSync(dir, { recursive: true, force: true });
});

/** Insert a message the way the Python producer does. */
function publish(headers: string, body: string): void {
  const db = new DatabaseSync(dbPath);
  db.prepare("INSERT INTO messages (topic, headers, body, enqueued_at) VALUES ('orders', ?, ?, ?)").run(headers, body, Date.now() / 1000);
  db.close();
}

/** State and attempts of every message. */
function states(): { state: string; attempts: number }[] {
  const db = new DatabaseSync(dbPath);
  const rows = db.prepare("SELECT state, attempts FROM messages ORDER BY id").all();
  db.close();
  return rows.map((r) => ({ state: String(r["state"]), attempts: Number(r["attempts"]) }));
}

/** The single finished span matching pred. */
function only(pred: (s: ReadableSpan) => boolean, what: string): ReadableSpan {
  const found = exporter.getFinishedSpans().filter(pred);
  assert.equal(found.length, 1, `want one ${what}, got ${found.length}`);
  return found[0]!;
}

const traceparent = (trace: string, span: string): string => JSON.stringify({ traceparent: `00-${trace}-${span}-01` });

describe("processNext", () => {
  it("continues the producer's trace and carries it to the gateway", async () => {
    publish(traceparent(TRACE_ID, PRODUCER_ID), JSON.stringify(ORDER));
    assert.equal(await processNext(opts), "done");

    const consumer = only((s) => s.kind === SpanKind.CONSUMER, "consumer span");
    const ship = only((s) => s.name === "fulfillment.ship", "ship span");
    const client = only((s) => s.kind === SpanKind.CLIENT, "fetch span");
    assert.equal(consumer.name, "process orders");
    assert.equal(consumer.spanContext().traceId, TRACE_ID);
    assert.equal(consumer.parentSpanContext?.spanId, PRODUCER_ID);
    assert.equal(consumer.parentSpanContext?.isRemote, true);
    assert.equal(ship.parentSpanContext?.spanId, consumer.spanContext().spanId);
    assert.equal(client.parentSpanContext?.spanId, ship.spanContext().spanId);
    assert.equal(consumer.attributes["order.id"], ORDER.order_id);
    assert.equal(consumer.attributes["messaging.operation.type"], "process");

    assert.equal(received.length, 1);
    assert.equal(received[0]!.url, `/orders/${ORDER.order_id}/shipment`);
    assert.equal(received[0]!.headers["traceparent"], `00-${TRACE_ID}-${client.spanContext().spanId}-01`);
    const { tracking_number } = JSON.parse(received[0]!.body) as { tracking_number: string };
    assert.match(tracking_number, /^TRK[0-9A-F]{10}$/);
    assert.equal(ship.attributes["shipment.tracking_number"], tracking_number);
    assert.deepEqual(states(), [{ state: "done", attempts: 1 }]);
  });

  it("keeps the producer's tracestate", async () => {
    publish(JSON.stringify({ traceparent: `00-${TRACE_ID}-${PRODUCER_ID}-01`, tracestate: "shop=a1" }), JSON.stringify(ORDER));
    assert.equal(await processNext(opts), "done");
    const consumer = only((s) => s.kind === SpanKind.CONSUMER, "consumer span");
    assert.equal(consumer.spanContext().traceState?.get("shop"), "a1");
    assert.match(String(received[0]!.headers["tracestate"]), /shop=a1/);
  });

  for (const [name, headers] of [
    ["no headers", "{}"],
    ["not JSON", "not json"],
    ["an array", "[]"],
    ["a non-string value", '{"traceparent": 5}'],
    ["a malformed traceparent", '{"traceparent": "00-zz-yy-01"}'],
    ["an all-zero trace id", traceparent("0".repeat(32), PRODUCER_ID)],
    ["an oversized value", JSON.stringify({ traceparent: "a".repeat(2000) })],
  ] as const) {
    it(`starts a new trace when headers contain ${name}`, async () => {
      publish(headers, JSON.stringify(ORDER));
      assert.equal(await processNext(opts), "done");
      const consumer = only((s) => s.kind === SpanKind.CONSUMER, "consumer span");
      assert.equal(consumer.parentSpanContext, undefined);
      assert.match(String(received[0]!.headers["traceparent"]), new RegExp(`^00-${consumer.spanContext().traceId}-`));
    });
  }

  it("sends a poison body straight to dead without calling the gateway", async () => {
    publish(traceparent(TRACE_ID, PRODUCER_ID), JSON.stringify({ ...ORDER, quantity: 0 }));
    assert.equal(await processNext(opts), "dead");
    const consumer = only((s) => s.kind === SpanKind.CONSUMER, "consumer span");
    assert.equal(consumer.status.code, SpanStatusCode.ERROR);
    assert.equal(consumer.spanContext().traceId, TRACE_ID);
    assert.equal(received.length, 0);
    assert.deepEqual(states(), [{ state: "dead", attempts: 1 }]);
  });

  it("retries when the gateway fails, then gives up after maxAttempts", async () => {
    gatewayStatus = 500;
    publish(traceparent(TRACE_ID, PRODUCER_ID), JSON.stringify(ORDER));
    assert.equal(await processNext(opts), "retry");
    assert.deepEqual(states(), [{ state: "ready", attempts: 1 }]);
    assert.equal(await processNext(opts), "dead");
    assert.deepEqual(states(), [{ state: "dead", attempts: 2 }]);
    const ships = exporter.getFinishedSpans().filter((s) => s.name === "fulfillment.ship");
    assert.equal(ships.length, 2);
    assert.ok(ships.every((s) => s.status.code === SpanStatusCode.ERROR && s.spanContext().traceId === TRACE_ID));
  });

  it("treats an order unknown to the gateway as poison", async () => {
    gatewayStatus = 404;
    publish(traceparent(TRACE_ID, PRODUCER_ID), JSON.stringify(ORDER));
    assert.equal(await processNext(opts), "dead");
  });

  it("never leaks one message's trace into the next", async () => {
    const other = "a".repeat(31) + "1";
    publish(traceparent(TRACE_ID, PRODUCER_ID), JSON.stringify(ORDER));
    publish("{}", JSON.stringify({ ...ORDER, order_id: "fedcba9876543210" }));
    publish(traceparent(other, "b".repeat(16)), JSON.stringify({ ...ORDER, order_id: "00000000000000ff" }));
    for (let i = 0; i < 3; i++) {
      assert.equal(await processNext(opts), "done");
    }
    const consumers = exporter.getFinishedSpans().filter((s) => s.kind === SpanKind.CONSUMER);
    assert.equal(consumers[0]!.spanContext().traceId, TRACE_ID);
    assert.ok(![TRACE_ID, other].includes(consumers[1]!.spanContext().traceId));
    assert.equal(consumers[1]!.parentSpanContext, undefined);
    assert.equal(consumers[2]!.spanContext().traceId, other);
    assert.equal(consumers[2]!.parentSpanContext?.spanId, "b".repeat(16));
  });

  it("returns empty and creates no span when nothing is queued", async () => {
    assert.equal(await processNext(opts), "empty");
    assert.equal(exporter.getFinishedSpans().length, 0);
  });
});

describe("runWorker", () => {
  it("drains the queue and stops when aborted", async () => {
    for (let i = 0; i < 3; i++) {
      publish(traceparent(TRACE_ID, PRODUCER_ID), JSON.stringify({ ...ORDER, order_id: `${i}`.padStart(16, "0") }));
    }
    const stop = new AbortController();
    const running = runWorker(opts, stop.signal, 10);
    while (received.length < 3) {
      await new Promise((r) => setTimeout(r, 10));
    }
    stop.abort();
    await running;
    assert.deepEqual(
      states().map((s) => s.state),
      ["done", "done", "done"],
    );
  });
});
