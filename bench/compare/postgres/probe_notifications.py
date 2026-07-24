#!/usr/bin/env python3
"""Probe raw LISTEN/NOTIFY delivery through DOCKET_BENCH_DATABASE_URL."""

from __future__ import annotations

import os
import secrets

import psycopg


def main() -> None:
    database_url = os.environ["DOCKET_BENCH_DATABASE_URL"]
    channel = "docket_bench_probe"
    payload = secrets.token_hex(12)

    with (
        psycopg.connect(database_url, autocommit=True) as listener,
        psycopg.connect(database_url, autocommit=True) as sender,
    ):
        listener.execute(f"LISTEN {channel}")
        sender.execute("SELECT pg_notify(%s, %s)", (channel, payload))
        received = list(listener.notifies(timeout=5.0, stop_after=1))
        listener.execute(f"UNLISTEN {channel}")

    matched = len(received) == 1 and received[0].payload == payload
    print(f"received={len(received)} payload_matched={matched}")
    if not matched:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
