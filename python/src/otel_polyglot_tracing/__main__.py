"""Run the inventory service: ``python -m otel_polyglot_tracing``.

Environment: INVENTORY_ADDR (default 127.0.0.1:8081; port 0 picks a free port), QUEUE_DB (path of
the SQLite queue shared with the worker), QUEUE_TRACE_PROPAGATION (on|off, default on), plus the
standard OTEL_EXPORTER_OTLP_* and OTEL_BSP_* variables.
"""

from __future__ import annotations

import os
import socket
import sys

import uvicorn

from otel_polyglot_tracing import telemetry
from otel_polyglot_tracing.inventory import DEFAULT_STOCK, Stock, create_app
from otel_polyglot_tracing.sqlite_queue import SqliteQueue


def parse_addr(addr: str) -> tuple[str, int]:
    """Split host:port and check the port range."""
    host, sep, port = addr.rpartition(":")
    if not sep or not host or not port.isdigit() or not 0 <= int(port) <= 65535:
        raise ValueError(f"INVENTORY_ADDR must be host:port, got {addr!r}")
    return host, int(port)


def parse_switch(value: str) -> bool:
    """Read an on/off setting."""
    lowered = value.strip().lower()
    if lowered in {"on", "true", "1"}:
        return True
    if lowered in {"off", "false", "0"}:
        return False
    raise ValueError(f"expected on or off, got {value!r}")


def main() -> int:
    """Bind the socket, announce the address and serve until interrupted."""
    host, port = parse_addr(os.environ.get("INVENTORY_ADDR", "127.0.0.1:8081"))
    propagate_ctx = parse_switch(os.environ.get("QUEUE_TRACE_PROPAGATION", "on"))
    provider = telemetry.setup("inventory")
    app = create_app(
        SqliteQueue(os.environ.get("QUEUE_DB", "queue.db")),
        Stock(DEFAULT_STOCK),
        provider,
        propagate_queue_context=propagate_ctx,
    )
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((host, port))
    # The e2e harness reads this line to learn the port when INVENTORY_ADDR ends in :0.
    print(f"inventory listening on http://{host}:{sock.getsockname()[1]}", flush=True)
    config = uvicorn.Config(
        app, log_level="warning", access_log=False, lifespan="off", limit_concurrency=200, timeout_keep_alive=5
    )
    try:
        uvicorn.Server(config).run(sockets=[sock])
    finally:
        provider.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
