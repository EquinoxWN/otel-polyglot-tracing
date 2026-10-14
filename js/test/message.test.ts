// Untrusted message parsing: headers degrade to "no context", bad bodies are poison.
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { PoisonMessageError, parseHeaders, parseOrder } from "../src/message.js";
import { parseGatewayUrl, parsePollMs } from "../src/main.js";

const TP = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

describe("parseHeaders", () => {
  it("returns string headers unchanged", () => {
    assert.deepEqual({ ...parseHeaders(JSON.stringify({ traceparent: TP, tracestate: "a=b" })) }, { traceparent: TP, tracestate: "a=b" });
  });

  for (const raw of ["", "null", "1", '"x"', "[]", '{"traceparent":1}', '{"Traceparent":"x"}', JSON.stringify({ k: "v".repeat(1025) })]) {
    it(`ignores ${raw.slice(0, 30) || "an empty string"}`, () => {
      assert.deepEqual({ ...parseHeaders(raw) }, {});
    });
  }

  it("ignores more than 16 headers", () => {
    const many = Object.fromEntries(Array.from({ length: 17 }, (_, i) => [`h${i}`, "v"]));
    assert.deepEqual({ ...parseHeaders(JSON.stringify(many)) }, {});
  });

  it("cannot be used to pollute prototypes", () => {
    const carrier = parseHeaders('{"__proto__": "x"}');
    assert.equal(Object.getPrototypeOf(carrier), null);
    assert.equal(({} as Record<string, unknown>)["x"], undefined);
  });
});

describe("parseOrder", () => {
  const good = { order_id: "0123456789abcdef", sku: "SKU-1", quantity: 2 };

  it("accepts the producer's body", () => {
    assert.deepEqual(parseOrder(JSON.stringify(good)), good);
  });

  for (const [name, body] of [
    ["not JSON", "{"],
    ["an array", "[]"],
    ["an extra field", JSON.stringify({ ...good, x: 1 })],
    ["a missing field", JSON.stringify({ order_id: good.order_id, sku: good.sku })],
    ["an uppercase order id", JSON.stringify({ ...good, order_id: "0123456789ABCDEF" })],
    ["a path in the order id", JSON.stringify({ ...good, order_id: "../../admin/1234" })],
    ["a lowercase sku", JSON.stringify({ ...good, sku: "sku" })],
    ["a fractional quantity", JSON.stringify({ ...good, quantity: 1.5 })],
    ["a quantity of 101", JSON.stringify({ ...good, quantity: 101 })],
    ["a string quantity", JSON.stringify({ ...good, quantity: "2" })],
  ] as const) {
    it(`rejects ${name} as poison`, () => {
      assert.throws(() => parseOrder(body), PoisonMessageError);
    });
  }
});

describe("configuration", () => {
  it("accepts only an http(s) origin as GATEWAY_URL", () => {
    assert.equal(parseGatewayUrl("http://127.0.0.1:8080").port, "8080");
    for (const bad of ["ftp://h", "http://u:p@h", "http://h/path", "http://h/?q", "http://h/#f"]) {
      assert.throws(() => parseGatewayUrl(bad), /GATEWAY_URL/);
    }
  });

  it("bounds POLL_MS", () => {
    assert.equal(parsePollMs("50"), 50);
    for (const bad of ["9", "5001", "1.5", "x", ""]) {
      assert.throws(() => parsePollMs(bad), /POLL_MS/);
    }
  });
});
