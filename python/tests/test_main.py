"""Service entry point: configuration parsing fails fast on bad values."""

from __future__ import annotations

import pytest

from otel_polyglot_tracing.__main__ import parse_addr, parse_switch


def test_parse_addr_accepts_host_and_port() -> None:
    assert parse_addr("127.0.0.1:0") == ("127.0.0.1", 0)
    assert parse_addr("localhost:8081") == ("localhost", 8081)


@pytest.mark.parametrize("addr", ["8081", ":8081", "127.0.0.1:", "127.0.0.1:http", "127.0.0.1:70000"])
def test_parse_addr_rejects_bad_values(addr: str) -> None:
    with pytest.raises(ValueError, match="INVENTORY_ADDR"):
        parse_addr(addr)


@pytest.mark.parametrize(
    ("value", "want"), [("on", True), ("TRUE", True), ("1", True), ("off", False), (" false ", False)]
)
def test_parse_switch(value: str, want: bool) -> None:
    assert parse_switch(value) is want


def test_parse_switch_rejects_typos() -> None:
    with pytest.raises(ValueError, match="on or off"):
        parse_switch("of")
