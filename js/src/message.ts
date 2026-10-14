// Parsing of untrusted queue messages: trace headers and the reserved-order body.

/** Body published by the inventory service for every reserved order. */
export interface ReservedOrder {
  readonly order_id: string;
  readonly sku: string;
  readonly quantity: number;
}

/** A message that can never succeed; it goes straight to the dead state. */
export class PoisonMessageError extends Error {
  override readonly name = "PoisonMessageError";
}

const MAX_HEADERS = 16;
const MAX_HEADER_VALUE = 1024;
const HEADER_KEY = /^[a-z0-9_.-]{1,64}$/;
const ORDER_ID = /^[0-9a-f]{16}$/;
const SKU = /^[A-Z0-9][A-Z0-9-]{0,31}$/;
const ORDER_KEYS = ["order_id", "quantity", "sku"];

/** Turn the stored headers JSON into a propagation carrier; anything malformed yields no headers. */
export function parseHeaders(raw: string): Record<string, string> {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return {};
  }
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    return {};
  }
  const entries = Object.entries(parsed);
  if (entries.length > MAX_HEADERS) {
    return {};
  }
  const out: Record<string, string> = Object.create(null) as Record<string, string>;
  for (const [key, value] of entries) {
    if (!HEADER_KEY.test(key) || typeof value !== "string" || value.length > MAX_HEADER_VALUE) {
      return {};
    }
    out[key] = value;
  }
  return out;
}

/** Validate the order body strictly: exactly the three expected fields, in range. */
export function parseOrder(raw: string): ReservedOrder {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new PoisonMessageError("body is not JSON");
  }
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new PoisonMessageError("body is not an object");
  }
  const body = parsed as Record<string, unknown>;
  const keys = Object.keys(body).sort();
  if (keys.length !== ORDER_KEYS.length || keys.some((k, i) => k !== ORDER_KEYS[i])) {
    throw new PoisonMessageError(`body fields must be ${ORDER_KEYS.join(", ")}`);
  }
  const { order_id, sku, quantity } = body;
  if (typeof order_id !== "string" || !ORDER_ID.test(order_id)) {
    throw new PoisonMessageError("invalid order_id");
  }
  if (typeof sku !== "string" || !SKU.test(sku)) {
    throw new PoisonMessageError("invalid sku");
  }
  if (typeof quantity !== "number" || !Number.isInteger(quantity) || quantity < 1 || quantity > 100) {
    throw new PoisonMessageError("invalid quantity");
  }
  return { order_id, sku, quantity };
}
