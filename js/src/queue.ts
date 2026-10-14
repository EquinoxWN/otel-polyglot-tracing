// Consumer side of the SQLite-backed queue shared with the Python inventory service.
import { DatabaseSync } from "node:sqlite";

// Same table as spec/queue.sql; a test fails if they drift apart.
export const SCHEMA = `
CREATE TABLE IF NOT EXISTS messages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  topic TEXT NOT NULL,
  headers TEXT NOT NULL,
  body TEXT NOT NULL,
  enqueued_at REAL NOT NULL,
  state TEXT NOT NULL DEFAULT 'ready' CHECK (state IN ('ready', 'claimed', 'done', 'dead')),
  attempts INTEGER NOT NULL DEFAULT 0,
  claimed_at REAL,
  last_error TEXT
);
CREATE INDEX IF NOT EXISTS messages_by_state ON messages (topic, state, id);
`;

/** A claimed message, exactly as stored. */
export interface QueueMessage {
  readonly id: number;
  readonly topic: string;
  readonly headers: string;
  readonly body: string;
  readonly attempts: number;
}

/** What happened to a message after a failed attempt. */
export type FailOutcome = "retry" | "dead";

const MAX_ERROR_CHARS = 500;

/** At-least-once queue: claim, then ack or fail; stale claims are re-delivered. */
export class SqliteQueue {
  readonly #db: DatabaseSync;

  /** Open (and if needed create) the queue database at path. */
  constructor(path: string, busyTimeoutMs = 5000) {
    this.#db = new DatabaseSync(path, { timeout: busyTimeoutMs });
    this.#db.exec("PRAGMA journal_mode=WAL");
    this.#db.exec(SCHEMA);
  }

  /** Atomically claim the oldest ready (or stale claimed) message of a topic. */
  claim(topic: string, visibilityMs: number, nowMs: number = Date.now()): QueueMessage | undefined {
    const now = nowMs / 1000;
    const staleBefore = now - visibilityMs / 1000;
    const row = this.#db
      .prepare(
        `UPDATE messages SET state = 'claimed', claimed_at = ?, attempts = attempts + 1
         WHERE id = (SELECT id FROM messages
                     WHERE topic = ? AND (state = 'ready' OR (state = 'claimed' AND claimed_at < ?))
                     ORDER BY id LIMIT 1)
         RETURNING id, topic, headers, body, attempts`,
      )
      .get(now, topic, staleBefore);
    if (row === undefined) {
      return undefined;
    }
    return {
      id: Number(row["id"]),
      topic: String(row["topic"]),
      headers: String(row["headers"]),
      body: String(row["body"]),
      attempts: Number(row["attempts"]),
    };
  }

  /** Mark a message as processed. */
  ack(id: number): void {
    this.#db.prepare("UPDATE messages SET state = 'done', last_error = NULL WHERE id = ?").run(id);
  }

  /** Record a failure; retry until maxAttempts, after which the message is dead. */
  fail(id: number, error: string, maxAttempts: number): FailOutcome {
    const row = this.#db
      .prepare(
        `UPDATE messages SET state = CASE WHEN attempts < ? THEN 'ready' ELSE 'dead' END,
                             last_error = ?, claimed_at = NULL
         WHERE id = ? RETURNING state`,
      )
      .get(maxAttempts, error.slice(0, MAX_ERROR_CHARS), id);
    return row !== undefined && row["state"] === "ready" ? "retry" : "dead";
  }

  /** Close the connection. */
  close(): void {
    this.#db.close();
  }
}
