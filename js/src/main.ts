// Entry point of the fulfillment worker: node dist/src/main.js
//
// Environment: QUEUE_DB (path of the SQLite queue shared with the inventory service), GATEWAY_URL
// (default http://127.0.0.1:8080), POLL_MS (10..5000, default 50), plus OTEL_EXPORTER_OTLP_* and OTEL_BSP_*.
import { trace } from "@opentelemetry/api";
import { SqliteQueue } from "./queue.js";
import { setupTelemetry } from "./telemetry.js";
import { runWorker } from "./worker.js";

/** Accept only an http(s) origin: no credentials, path, query or fragment. */
export function parseGatewayUrl(raw: string): URL {
  const url = new URL(raw);
  if (!["http:", "https:"].includes(url.protocol) || url.username || url.password || url.search || url.hash || url.pathname !== "/") {
    throw new Error(`GATEWAY_URL must be an http(s) origin, got ${raw}`);
  }
  return url;
}

/** Read POLL_MS as an integer between 10 and 5000. */
export function parsePollMs(raw: string): number {
  const n = Number(raw);
  if (!Number.isInteger(n) || n < 10 || n > 5000) {
    throw new Error(`POLL_MS must be an integer from 10 to 5000, got ${raw}`);
  }
  return n;
}

/** Run until SIGINT or SIGTERM, then flush spans and close the queue. */
async function main(): Promise<void> {
  const env = process.env;
  const queuePath = env["QUEUE_DB"] ?? "queue.db";
  const gatewayUrl = parseGatewayUrl(env["GATEWAY_URL"] ?? "http://127.0.0.1:8080");
  const pollMs = parsePollMs(env["POLL_MS"] ?? "50");

  const provider = setupTelemetry("fulfillment");
  const queue = new SqliteQueue(queuePath);
  const stop = new AbortController();
  for (const sig of ["SIGINT", "SIGTERM"] as const) {
    process.once(sig, () => stop.abort());
  }
  // The e2e harness waits for this line before sending orders.
  console.log(`fulfillment worker polling ${queuePath}`);
  try {
    await runWorker({ queue, gatewayUrl, tracer: trace.getTracer("fulfillment") }, stop.signal, pollMs);
  } finally {
    queue.close();
    await provider.shutdown();
  }
}

if (import.meta.main) {
  main().catch((err: unknown) => {
    console.error("fulfillment:", err instanceof Error ? err.message : err);
    process.exitCode = 1;
  });
}
