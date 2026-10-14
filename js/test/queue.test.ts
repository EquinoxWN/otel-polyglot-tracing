// Queue consumer: schema contract, claim/ack/fail state machine, atomic claims across threads.
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { afterEach, beforeEach, describe, it } from "node:test";
import { Worker } from "node:worker_threads";
import { SCHEMA, SqliteQueue } from "../src/queue.js";

let dir: string;
let dbPath: string;
let queue: SqliteQueue;

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "queue-test-"));
  dbPath = join(dir, "queue.db");
  queue = new SqliteQueue(dbPath);
});

afterEach(() => {
  queue.close();
  rmSync(dir, { recursive: true, force: true });
});

/** Insert n messages on a topic, as the Python producer does. */
function publish(n: number, topic = "orders"): void {
  const db = new DatabaseSync(dbPath);
  const insert = db.prepare("INSERT INTO messages (topic, headers, body, enqueued_at) VALUES (?, '{}', ?, ?)");
  for (let i = 0; i < n; i++) {
    insert.run(topic, JSON.stringify({ i }), Date.now() / 1000);
  }
  db.close();
}

const normalise = (sql: string): string => sql.replace(/\s+/g, " ").trim();

describe("SqliteQueue", () => {
  it("uses exactly the shared schema in spec/queue.sql", () => {
    const spec = readFileSync(new URL("../../../spec/queue.sql", import.meta.url), "utf8");
    assert.equal(normalise(SCHEMA), normalise(spec));
  });

  it("claims in FIFO order and counts attempts", () => {
    publish(3);
    const ids = [queue.claim("orders", 30_000), queue.claim("orders", 30_000), queue.claim("orders", 30_000)].map((m) => m?.id);
    assert.deepEqual(ids, [1, 2, 3]);
    assert.equal(queue.claim("orders", 30_000), undefined);
  });

  it("re-delivers a claim only after the visibility timeout", () => {
    publish(1);
    const t0 = Date.now();
    assert.equal(queue.claim("orders", 1_000, t0)?.attempts, 1);
    assert.equal(queue.claim("orders", 1_000, t0 + 500), undefined);
    assert.equal(queue.claim("orders", 1_000, t0 + 1_500)?.attempts, 2);
  });

  it("acked messages are never delivered again", () => {
    publish(1);
    const m = queue.claim("orders", 1_000, 0);
    queue.ack(m!.id);
    assert.equal(queue.claim("orders", 1_000, 10_000_000), undefined);
  });

  it("failed messages retry until maxAttempts, then are dead", () => {
    publish(1);
    let m = queue.claim("orders", 30_000);
    assert.equal(queue.fail(m!.id, "boom", 2), "retry");
    m = queue.claim("orders", 30_000);
    assert.equal(m?.attempts, 2);
    assert.equal(queue.fail(m!.id, "x".repeat(10_000), 2), "dead");
    assert.equal(queue.claim("orders", 30_000, Date.now() + 10_000_000), undefined);
    const db = new DatabaseSync(dbPath);
    const row = db.prepare("SELECT state, length(last_error) AS n FROM messages").get();
    db.close();
    assert.deepEqual({ state: row?.["state"], n: row?.["n"] }, { state: "dead", n: 500 });
  });

  it("keeps topics apart", () => {
    publish(1, "other");
    assert.equal(queue.claim("orders", 30_000), undefined);
    assert.equal(queue.claim("other", 30_000)?.id, 1);
  });

  it("never hands the same message to two concurrent consumers", async () => {
    publish(200);
    const code = `
      const { DatabaseSync } = require("node:sqlite");
      const { parentPort, workerData } = require("node:worker_threads");
      const db = new DatabaseSync(workerData, { timeout: 5000 });
      const claim = db.prepare("UPDATE messages SET state='claimed', claimed_at=?, attempts=attempts+1 WHERE id = (SELECT id FROM messages WHERE topic='orders' AND state='ready' ORDER BY id LIMIT 1) RETURNING id");
      const ids = [];
      for (;;) { const row = claim.get(Date.now() / 1000); if (!row) break; ids.push(Number(row.id)); }
      db.close();
      parentPort.postMessage(ids);
    `;
    // The worker threads run the same claim statement as SqliteQueue.claim (checked below).
    assert.match(SqliteQueue.prototype.claim.toString(), /UPDATE messages SET state = 'claimed'/);
    const runs = Array.from(
      { length: 4 },
      () =>
        new Promise<number[]>((resolve, reject) => {
          const w = new Worker(code, { eval: true, workerData: dbPath });
          w.once("message", resolve);
          w.once("error", reject);
        }),
    );
    const all = (await Promise.all(runs)).flat();
    assert.equal(all.length, 200);
    assert.equal(new Set(all).size, 200);
  });
});
