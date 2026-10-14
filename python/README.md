# otel-polyglot-tracing: inventory service (Python)

FastAPI service that reserves stock and publishes each reserved order to the SQLite queue with the
W3C trace context in the message headers. Run it with `python -m otel_polyglot_tracing`; see the
[main README](../README.md) for the whole system and its tests.
