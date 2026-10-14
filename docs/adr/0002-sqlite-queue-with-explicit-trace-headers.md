# ADR 0002: A SQLite queue with explicit trace headers instead of a broker in M1

- **Status:** Accepted

## Context

M1 must show W3C trace context crossing a queue between Python and Node.js, and prove it with a
test that runs on a laptop without Docker and in CI. Real brokers (Kafka, RabbitMQ, NATS) need a
server process or a container, and their tracing plug-ins inject and extract the context
internally, which hides the step that most often breaks in real systems.

## Decision

The queue is one SQLite table (`spec/queue.sql`) in WAL mode with a `headers` column next to the
body, like Kafka record headers or AMQP application headers. Python's standard `sqlite3` produces;
Node.js 24's built-in `node:sqlite` consumes with one atomic `UPDATE ... RETURNING` claim, a
visibility timeout and retry/dead states. The producer calls `propagate.inject` inside its PRODUCER
span; the consumer calls `propagation.extract(ROOT_CONTEXT, headers)` and starts a CONSUMER span
with that parent. Both sides embed the schema, and a test in each language compares it to
`spec/queue.sql`.

## Consequences

- No extra runtime dependency: both languages ship SQLite, the e2e test needs only a file in a
  temp directory, and nothing listens on a network port for the queue.
- The inject/extract code is visible and unit-tested, including malformed and oversized headers.
- SQLite has one writer at a time: under 20 concurrent orders the publish span reached about
  240 ms in the e2e run. Throughput is not an M1 goal; this would be the first thing to replace.
- The worker polls (20 ms in the test, 50 ms by default) instead of being pushed messages, which
  adds up to one poll interval of latency per message and costs a little idle CPU.
- Moving to a broker later changes only the two queue modules; span names, attributes and the
  test's expected trace stay the same.
