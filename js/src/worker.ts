// Fulfillment worker: consumes reserved orders, continues their trace and reports the shipment.
import { randomBytes } from "node:crypto";
import { setTimeout as sleep } from "node:timers/promises";
import { ROOT_CONTEXT, SpanKind, SpanStatusCode, propagation, type Span, type Tracer } from "@opentelemetry/api";
import { PoisonMessageError, parseHeaders, parseOrder, type ReservedOrder } from "./message.js";
import type { SqliteQueue } from "./queue.js";

export const TOPIC = "orders";

/** Everything the worker needs, injected so tests can use their own queue, tracer and gateway. */
export interface WorkerOptions {
  readonly queue: SqliteQueue;
  readonly tracer: Tracer;
  readonly gatewayUrl: URL;
  readonly visibilityMs?: number;
  readonly maxAttempts?: number;
  readonly requestTimeoutMs?: number;
}

/** Result of one processNext call. */
export type Outcome = "empty" | "done" | "retry" | "dead";

/** Claim one message and process it inside a CONSUMER span that continues the producer's trace. */
export async function processNext(opts: WorkerOptions): Promise<Outcome> {
  const msg = opts.queue.claim(TOPIC, opts.visibilityMs ?? 30_000);
  if (msg === undefined) {
    return "empty";
  }
  // ROOT_CONTEXT, not the active context: each message's trace comes only from its own headers.
  const parent = propagation.extract(ROOT_CONTEXT, parseHeaders(msg.headers));
  const attributes = {
    "messaging.system": "sqlite",
    "messaging.destination.name": TOPIC,
    "messaging.operation.type": "process",
    "messaging.operation.name": "process",
    "messaging.message.id": String(msg.id),
    "queue.attempt": msg.attempts,
  };
  return opts.tracer.startActiveSpan(
    `process ${TOPIC}`,
    { kind: SpanKind.CONSUMER, attributes },
    parent,
    async (span): Promise<Outcome> => {
      try {
        const order = parseOrder(msg.body);
        span.setAttribute("order.id", order.order_id);
        await ship(opts, order);
        opts.queue.ack(msg.id);
        return "done";
      } catch (err) {
        const error = markError(span, err);
        const maxAttempts = err instanceof PoisonMessageError ? 0 : (opts.maxAttempts ?? 3);
        return opts.queue.fail(msg.id, error.message, maxAttempts);
      } finally {
        span.end();
      }
    },
  );
}

/** Book the shipment inside the fulfillment.ship business span and tell the gateway. */
async function ship(opts: WorkerOptions, order: ReservedOrder): Promise<void> {
  const attributes = { "order.id": order.order_id, "order.sku": order.sku, "order.quantity": order.quantity };
  await opts.tracer.startActiveSpan("fulfillment.ship", { attributes }, async (span) => {
    try {
      const tracking = `TRK${randomBytes(5).toString("hex").toUpperCase()}`;
      span.setAttribute("shipment.tracking_number", tracking);
      // order_id is validated as 16 hex characters, so it cannot change the path.
      const url = new URL(`/orders/${order.order_id}/shipment`, opts.gatewayUrl);
      const res = await fetch(url, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ tracking_number: tracking }),
        signal: AbortSignal.timeout(opts.requestTimeoutMs ?? 5_000),
      });
      await res.body?.cancel();
      if (res.status === 404) {
        throw new PoisonMessageError("gateway does not know this order");
      }
      if (!res.ok) {
        throw new Error(`gateway answered ${res.status}`);
      }
    } catch (err) {
      throw markError(span, err);
    } finally {
      span.end();
    }
  });
}

/** Record err on span as an exception and an error status; return it as an Error. */
function markError(span: Span, err: unknown): Error {
  const error = err instanceof Error ? err : new Error(String(err));
  span.recordException(error);
  span.setStatus({ code: SpanStatusCode.ERROR, message: error.message });
  return error;
}

/** Poll the queue until signal aborts, sleeping pollMs whenever it is empty. */
export async function runWorker(opts: WorkerOptions, signal: AbortSignal, pollMs: number): Promise<void> {
  while (!signal.aborted) {
    let outcome: Outcome;
    try {
      outcome = await processNext(opts);
    } catch (err) {
      console.error("fulfillment: queue error:", err instanceof Error ? err.message : err);
      outcome = "empty";
    }
    if (outcome === "empty") {
      await sleep(pollMs, undefined, { signal }).catch(() => undefined);
    }
  }
}
