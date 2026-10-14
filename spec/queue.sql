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
