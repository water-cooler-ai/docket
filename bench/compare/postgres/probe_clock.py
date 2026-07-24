#!/usr/bin/env python3
"""Estimate database clock offset using local request/response midpoints."""

from __future__ import annotations

import os
import statistics
import time

import psycopg


def main() -> None:
    offsets_ms: list[float] = []
    round_trips_ms: list[float] = []

    with psycopg.connect(
        os.environ["DOCKET_BENCH_DATABASE_URL"], autocommit=True
    ) as connection:
        for _sample in range(50):
            started_ns = time.time_ns()
            database_time = connection.execute(
                "SELECT clock_timestamp()"
            ).fetchone()[0]
            finished_ns = time.time_ns()
            midpoint_ms = (started_ns + finished_ns) / 2_000_000
            database_ms = database_time.timestamp() * 1_000
            offsets_ms.append(database_ms - midpoint_ms)
            round_trips_ms.append((finished_ns - started_ns) / 1_000_000)

    print(
        "database_minus_droplet_ms="
        f"{statistics.median(offsets_ms):.3f} "
        f"range={min(offsets_ms):.3f}..{max(offsets_ms):.3f}"
    )
    print(
        "query_round_trip_ms="
        f"{statistics.median(round_trips_ms):.3f} "
        f"range={min(round_trips_ms):.3f}..{max(round_trips_ms):.3f}"
    )


if __name__ == "__main__":
    main()
