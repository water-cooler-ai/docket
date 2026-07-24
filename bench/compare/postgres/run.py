#!/usr/bin/env python3
"""Compare durable Docket and LangGraph execution on one PostgreSQL server."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import platform
import statistics
import subprocess
import time
from datetime import UTC, datetime
from importlib.metadata import version
from pathlib import Path
from typing import Any, TypedDict

import psycopg
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.graph import END, START, StateGraph
from psycopg import sql
from psycopg.conninfo import make_conninfo
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool


ROOT = Path(__file__).resolve().parents[3]
SCENARIOS = (
    ("single_node", ("chain", 1), "single_runs"),
    ("chain_10", ("chain", 10), "chain_runs"),
    ("fanout_8", ("fanout", 8), "fanout_runs"),
)

# The PostgreSQL checkpointer can deserialize extension types. Restrict it to
# known-safe msgpack values for this benchmark's primitive state.
os.environ.setdefault("LANGGRAPH_STRICT_MSGPACK", "true")


class State(TypedDict):
    token: int


def noop(_state: State) -> dict[str, object]:
    return {}


def build_langgraph(shape: tuple[str, int], saver: AsyncPostgresSaver):
    kind, count = shape
    builder = StateGraph(State)
    node_ids = [f"node_{index}" for index in range(1, count + 1)]

    for node_id in node_ids:
        builder.add_node(node_id, noop)

    if kind == "chain":
        builder.add_edge(START, node_ids[0])
        for source, target in zip(node_ids, node_ids[1:], strict=False):
            builder.add_edge(source, target)
        builder.add_edge(node_ids[-1], END)
    elif kind == "fanout":
        for node_id in node_ids:
            builder.add_edge(START, node_id)
            builder.add_edge(node_id, END)
    else:
        raise ValueError(f"unknown graph shape: {kind}")

    return builder.compile(checkpointer=saver)


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    index = max(int(len(ordered) * fraction + 0.999999) - 1, 0)
    return ordered[index]


def median(values: list[float]) -> float:
    return statistics.median(values)


def parse_levels(value: str) -> list[int]:
    try:
        levels = [int(item.strip()) for item in value.split(",") if item.strip()]
    except ValueError as error:
        raise argparse.ArgumentTypeError("levels must be comma-separated integers") from error

    if not levels or any(level <= 0 for level in levels) or len(set(levels)) != len(
        levels
    ):
        raise argparse.ArgumentTypeError("levels must be unique positive integers")
    return levels


def parse_scenarios(value: str) -> list[str]:
    names = [item.strip() for item in value.split(",") if item.strip()]
    available = {scenario for scenario, _shape, _count_key in SCENARIOS}
    if (
        not names
        or len(set(names)) != len(names)
        or any(name not in available for name in names)
    ):
        choices = ",".join(scenario for scenario, _shape, _count_key in SCENARIOS)
        raise argparse.ArgumentTypeError(
            f"scenarios must be unique names from: {choices}"
        )
    return names


def make_schema_name() -> str:
    return f"langgraph_bench_{os.getpid()}_{time.time_ns() % 1_000_000_000}"


def create_schema(database_url: str, schema: str) -> None:
    with psycopg.connect(database_url, autocommit=True) as connection:
        connection.execute(
            sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema))
        )


def drop_schema(database_url: str, schema: str) -> None:
    if not schema.startswith("langgraph_bench_"):
        raise ValueError(f"refusing to drop unsafe schema name {schema!r}")
    with psycopg.connect(database_url, autocommit=True) as connection:
        connection.execute(
            sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                sql.Identifier(schema)
            )
        )


async def checkpoint_tables(
    pool: AsyncConnectionPool[Any], *, include_migrations: bool = False
) -> list[str]:
    async with pool.connection() as connection:
        cursor = await connection.execute(
            """
            SELECT tablename
            FROM pg_tables
            WHERE schemaname = current_schema()
              AND tablename LIKE 'checkpoint%'
            ORDER BY tablename
            """
        )
        names = [row["tablename"] for row in await cursor.fetchall()]
    if include_migrations:
        return names
    return [name for name in names if name != "checkpoint_migrations"]


async def reset_langgraph(pool: AsyncConnectionPool[Any]) -> None:
    tables = await checkpoint_tables(pool)
    if not tables:
        return
    statement = sql.SQL("TRUNCATE {}").format(
        sql.SQL(", ").join(sql.Identifier(table) for table in tables)
    )
    async with pool.connection() as connection:
        await connection.execute(statement)


async def scalar(
    pool: AsyncConnectionPool[Any], statement: str | sql.Composed
) -> int:
    async with pool.connection() as connection:
        cursor = await connection.execute(statement)
        row = await cursor.fetchone()
    assert row is not None
    return int(next(iter(row.values())))


async def logical_bytes(pool: AsyncConnectionPool[Any], tables: list[str]) -> int:
    total = 0
    for table in tables:
        statement = sql.SQL(
            "SELECT COALESCE(sum(pg_column_size(row_data)), 0) AS value "
            "FROM {} AS row_data"
        ).format(sql.Identifier(table))
        total += await scalar(pool, statement)
    return total


async def run_langgraph_batch(
    pool: AsyncConnectionPool[Any],
    graphs: list[Any],
    scenario: str,
    repeat: int,
    count: int,
    durability: str,
) -> dict[str, Any]:
    queue: asyncio.Queue[int] = asyncio.Queue()
    for index in range(count):
        queue.put_nowait(index)

    started = time.perf_counter()
    latencies: list[float] = []
    outputs: list[State] = []

    async def worker(worker_index: int) -> None:
        graph = graphs[worker_index]
        while True:
            try:
                index = queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            try:
                thread_id = f"{scenario}-c{len(graphs)}-r{repeat}-{index}"
                result = await graph.ainvoke(
                    {"token": 0},
                    {"configurable": {"thread_id": thread_id}},
                    durability=durability,
                )
                outputs.append(result)
                latencies.append((time.perf_counter() - started) * 1_000)
            finally:
                queue.task_done()

    await asyncio.gather(*(worker(index) for index in range(len(graphs))))
    elapsed_ms = max(latencies)
    tables = await checkpoint_tables(pool)
    checkpoint_rows = await scalar(pool, "SELECT count(*) AS value FROM checkpoints")
    write_rows = await scalar(
        pool, "SELECT count(*) AS value FROM checkpoint_writes"
    )
    blob_rows = await scalar(pool, "SELECT count(*) AS value FROM checkpoint_blobs")
    thread_rows = await scalar(
        pool, "SELECT count(DISTINCT thread_id) AS value FROM checkpoints"
    )
    bytes_used = await logical_bytes(pool, tables)

    invariants = {
        "completed": len(outputs) == count,
        "valid_output": all(output.get("token") == 0 for output in outputs),
        "one_thread_per_run": thread_rows == count,
    }
    if not all(invariants.values()):
        raise RuntimeError(f"LangGraph invariant failure: {invariants}")

    return {
        "framework": "langgraph_postgres",
        "langgraph_durability": durability,
        "scenario": scenario,
        "concurrency": len(graphs),
        "repeat": repeat,
        "runs": count,
        "elapsed_ms": elapsed_ms,
        "runs_per_second": count / max(elapsed_ms / 1_000, 0.000001),
        "p50_ms": percentile(latencies, 0.50),
        "p95_ms": percentile(latencies, 0.95),
        "p99_ms": percentile(latencies, 0.99),
        "checkpoint_rows": checkpoint_rows,
        "write_rows": write_rows,
        "blob_rows": blob_rows,
        "logical_bytes": bytes_used,
        "invariants": invariants,
    }


async def benchmark_langgraph(
    database_url: str,
    schema: str,
    levels: list[int],
    repeats: int,
    warmup: int,
    counts: dict[str, int],
    scenario_defs: list[tuple[str, tuple[str, int], str]],
    pool_size: int,
    durability: str,
) -> tuple[list[dict[str, Any]], list[str]]:
    conninfo = make_conninfo(database_url, options=f"-csearch_path={schema}")
    max_level = max(levels)
    pool: AsyncConnectionPool[Any] = AsyncConnectionPool(
        conninfo=conninfo,
        min_size=min(4, max_level, pool_size),
        max_size=pool_size,
        kwargs={
            "autocommit": True,
            "prepare_threshold": 0,
            "row_factory": dict_row,
        },
        open=False,
        name="docket-langgraph-benchmark",
    )
    await pool.open()
    await pool.wait()

    try:
        setup_saver = AsyncPostgresSaver(pool)
        await setup_saver.setup()
        tables = await checkpoint_tables(pool, include_migrations=True)
        results: list[dict[str, Any]] = []

        for scenario, shape, count_key in scenario_defs:
            for concurrency in levels:
                savers = [AsyncPostgresSaver(pool) for _ in range(concurrency)]
                graphs = [build_langgraph(shape, saver) for saver in savers]

                if warmup:
                    await reset_langgraph(pool)
                    await run_langgraph_batch(
                        pool,
                        graphs,
                        f"{scenario}-warm",
                        0,
                        warmup,
                        durability,
                    )

                for repeat in range(1, repeats + 1):
                    await reset_langgraph(pool)
                    results.append(
                        await run_langgraph_batch(
                            pool,
                            graphs,
                            scenario,
                            repeat,
                            counts[count_key],
                            durability,
                        )
                    )

        return results, tables
    finally:
        await pool.close()


def benchmark_docket(
    database_url: str,
    levels: list[int],
    repeats: int,
    warmup: int,
    counts: dict[str, int],
    scenario_defs: list[tuple[str, tuple[str, int], str]],
    pool_size: int,
) -> tuple[list[dict[str, Any]], dict[str, str]]:
    command = [
        "mix",
        "run",
        "bench/compare/postgres/docket_runner.exs",
        "--database-url",
        database_url,
        "--levels",
        ",".join(str(level) for level in levels),
        "--pool-size",
        str(pool_size),
        "--scenarios",
        ",".join(scenario for scenario, _shape, _count_key in scenario_defs),
        "--repeats",
        str(repeats),
        "--warmup",
        str(warmup),
        "--single-runs",
        str(counts["single_runs"]),
        "--chain-runs",
        str(counts["chain_runs"]),
        "--fanout-runs",
        str(counts["fanout_runs"]),
    ]
    completed = subprocess.run(
        command,
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        env={**os.environ, "MIX_ENV": "prod"},
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"Docket benchmark exited with {completed.returncode}.\n"
            f"stdout:\n{completed.stdout}\nstderr:\n{completed.stderr}"
        )

    results: list[dict[str, Any]] = []
    metadata: dict[str, str] = {}

    for line in completed.stdout.splitlines():
        if line.startswith("META,"):
            (
                _,
                framework,
                elixir,
                otp,
                postgres_version,
                database,
                _schema,
            ) = line.split(",")
            metadata = {
                "framework": framework,
                "elixir": elixir,
                "otp": otp,
                "postgres_version_num": postgres_version,
                "database": database,
            }
        elif line.startswith("RESULT,"):
            (
                _,
                framework,
                scenario,
                concurrency,
                repeat,
                runs,
                elapsed_ms,
                runs_per_second,
                p50_ms,
                p95_ms,
                p99_ms,
                notifications_received,
                notification_polls,
                scheduled_polls,
                run_rows,
                event_rows,
                logical_bytes_value,
                invariants_pass,
            ) = line.split(",")
            results.append(
                {
                    "framework": framework,
                    "scenario": scenario,
                    "concurrency": int(concurrency),
                    "repeat": int(repeat),
                    "runs": int(runs),
                    "elapsed_ms": float(elapsed_ms),
                    "runs_per_second": float(runs_per_second),
                    "p50_ms": float(p50_ms),
                    "p95_ms": float(p95_ms),
                    "p99_ms": float(p99_ms),
                    "notifications_received": int(notifications_received),
                    "notification_polls": int(notification_polls),
                    "scheduled_polls": int(scheduled_polls),
                    "completion_wait": "local_terminal_telemetry",
                    "run_rows": int(run_rows),
                    "event_rows": int(event_rows),
                    "logical_bytes": int(logical_bytes_value),
                    "invariants": {"all_docket_invariants": invariants_pass == "true"},
                }
            )

    expected = len(scenario_defs) * len(levels) * repeats
    if len(results) != expected:
        raise RuntimeError(
            f"Docket returned {len(results)} results; expected {expected}.\n"
            f"stdout:\n{completed.stdout}\nstderr:\n{completed.stderr}"
        )
    return results, metadata


def aggregate(
    docket_results: list[dict[str, Any]],
    langgraph_results: list[dict[str, Any]],
    counts: dict[str, int],
    scenario_defs: list[tuple[str, tuple[str, int], str]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    all_results = docket_results + langgraph_results

    for scenario, _shape, count_key in scenario_defs:
        levels = sorted(
            {
                result["concurrency"]
                for result in all_results
                if result["scenario"] == scenario
            }
        )
        for concurrency in levels:
            selected = [
                result
                for result in all_results
                if result["scenario"] == scenario
                and result["concurrency"] == concurrency
            ]
            by_framework = {
                framework: [
                    result
                    for result in selected
                    if result["framework"] == framework
                ]
                for framework in ("docket_postgres", "langgraph_postgres")
            }
            docket = by_framework["docket_postgres"]
            langgraph = by_framework["langgraph_postgres"]
            docket_rps = median([result["runs_per_second"] for result in docket])
            langgraph_rps = median(
                [result["runs_per_second"] for result in langgraph]
            )

            rows.append(
                {
                    "scenario": scenario,
                    "concurrency": concurrency,
                    "runs": counts[count_key],
                    "docket_runs_per_second": docket_rps,
                    "langgraph_runs_per_second": langgraph_rps,
                    "docket_over_langgraph_throughput": docket_rps / langgraph_rps,
                    "docket_elapsed_ms": median(
                        [result["elapsed_ms"] for result in docket]
                    ),
                    "langgraph_elapsed_ms": median(
                        [result["elapsed_ms"] for result in langgraph]
                    ),
                    "docket_p50_ms": median([result["p50_ms"] for result in docket]),
                    "langgraph_p50_ms": median(
                        [result["p50_ms"] for result in langgraph]
                    ),
                    "docket_p95_ms": median([result["p95_ms"] for result in docket]),
                    "langgraph_p95_ms": median(
                        [result["p95_ms"] for result in langgraph]
                    ),
                    "docket_p99_ms": median([result["p99_ms"] for result in docket]),
                    "langgraph_p99_ms": median(
                        [result["p99_ms"] for result in langgraph]
                    ),
                    "docket_logical_bytes_per_run": median(
                        [result["logical_bytes"] / result["runs"] for result in docket]
                    ),
                    "langgraph_logical_bytes_per_run": median(
                        [
                            result["logical_bytes"] / result["runs"]
                            for result in langgraph
                        ]
                    ),
                    "docket_event_rows_per_run": median(
                        [result["event_rows"] / result["runs"] for result in docket]
                    ),
                    "langgraph_checkpoint_rows_per_run": median(
                        [
                            result["checkpoint_rows"] / result["runs"]
                            for result in langgraph
                        ]
                    ),
                }
            )

    return rows


def format_rate(value: float) -> str:
    return f"{value:,.0f}"


def format_bytes(value: float) -> str:
    if value >= 1024:
        return f"{value / 1024:.1f} KiB"
    return f"{value:.0f} B"


def render_report(payload: dict[str, Any]) -> str:
    lines = [
        "# Docket vs LangGraph PostgreSQL benchmark",
        "",
        f"Generated: {payload['generated_at']}",
        "",
        "| Scenario | C | Docket r/s | LangGraph r/s | Throughput ratio | "
        "Docket p95 | LangGraph p95 | Docket bytes/run | LangGraph bytes/run |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in payload["summary"]:
        lines.append(
            f"| {row['scenario']} | {row['concurrency']} | "
            f"{format_rate(row['docket_runs_per_second'])} | "
            f"{format_rate(row['langgraph_runs_per_second'])} | "
            f"{row['docket_over_langgraph_throughput']:.2f}× | "
            f"{row['docket_p95_ms']:.1f} ms | {row['langgraph_p95_ms']:.1f} ms | "
            f"{format_bytes(row['docket_logical_bytes_per_run'])} | "
            f"{format_bytes(row['langgraph_logical_bytes_per_run'])} |"
        )

    lines.extend(
        [
            "",
            "## Completion timeline",
            "",
            "| Scenario | C | Docket total | LangGraph total | "
            "Docket p50 / p95 / p99 | LangGraph p50 / p95 / p99 |",
            "| --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in payload["summary"]:
        lines.append(
            f"| {row['scenario']} | {row['concurrency']} | "
            f"{row['docket_elapsed_ms'] / 1_000:.3f} s | "
            f"{row['langgraph_elapsed_ms'] / 1_000:.3f} s | "
            f"{row['docket_p50_ms'] / 1_000:.3f} / "
            f"{row['docket_p95_ms'] / 1_000:.3f} / "
            f"{row['docket_p99_ms'] / 1_000:.3f} s | "
            f"{row['langgraph_p50_ms'] / 1_000:.3f} / "
            f"{row['langgraph_p95_ms'] / 1_000:.3f} / "
            f"{row['langgraph_p99_ms'] / 1_000:.3f} s |"
        )

    environment = payload["environment"]
    config = payload["config"]
    lines.extend(
        [
            "",
            "Throughput ratio is Docket runs/second divided by LangGraph "
            "runs/second; values above 1.0× favor Docket. Each value is the "
            "median of complete queue-drain trials. Per-run latency begins when "
            "the batch is submitted and therefore includes queueing.",
            "",
            "Docket uses its supervised PostgreSQL runtime, durable run queue, "
            "claim fencing, LISTEN/NOTIFY wake-up, and retained events. LangGraph "
            "uses one AsyncPostgresSaver/compiled graph per worker over a shared "
            "Psycopg pool; each invocation has a unique thread.",
            "",
            "Logical bytes/run sums PostgreSQL row payload sizes for Docket runs "
            "and events, or LangGraph checkpoints, writes, and blobs. It excludes "
            "indexes, table-page slack, migrations, and the graph definition.",
            "",
            "## Environment",
            "",
            f"- Docket commit: `{environment['docket_commit']}`"
            f"{' (dirty)' if environment['docket_dirty'] else ''}",
            f"- LangGraph / PostgreSQL saver: `{environment['langgraph']}` / "
            f"`{environment['langgraph_checkpoint_postgres']}`",
            f"- Psycopg / pool: `{environment['psycopg']}` / "
            f"`{environment['psycopg_pool']}`",
            f"- Elixir / OTP: `{environment['elixir']}` / `{environment['otp']}`",
            f"- Python: `{environment['python']}`",
            f"- PostgreSQL server_version_num: `{environment['postgres_version_num']}`",
            f"- Platform: `{environment['platform']}`",
            f"- Scenarios: `{config['scenarios']}`",
            f"- Framework order: `{config['framework_order']}`; database pool cap: "
            f"`{config['pool_size']}`",
            f"- LangGraph checkpoint durability: "
            f"`{config['langgraph_durability']}`",
            f"- Docket completion wait: `{config['docket_completion_wait']}` "
            "(one final durable batch read)",
            f"- Concurrency levels: `{config['levels']}`; repeats: "
            f"`{config['repeats']}`; warmups: `{config['warmup']}`",
            "",
        ]
    )
    return "\n".join(lines)


def git_value(*arguments: str) -> str:
    return subprocess.run(
        ["git", *arguments],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DOCKET_BENCH_DATABASE_URL"),
        help="Dedicated PostgreSQL database URL (or DOCKET_BENCH_DATABASE_URL)",
    )
    parser.add_argument("--levels", type=parse_levels, default=parse_levels("1,8,32"))
    parser.add_argument("--pool-size", type=int)
    parser.add_argument(
        "--scenarios",
        type=parse_scenarios,
        default=parse_scenarios("single_node,chain_10,fanout_8"),
    )
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument(
        "--framework-order",
        choices=("docket-first", "langgraph-first"),
        default="docket-first",
    )
    parser.add_argument(
        "--langgraph-durability",
        choices=("sync", "async", "exit"),
        default="sync",
        help="LangGraph checkpoint durability (default: sync)",
    )
    parser.add_argument("--warmup", type=int, default=5)
    parser.add_argument("--single-runs", type=int, default=300)
    parser.add_argument("--chain-runs", type=int, default=300)
    parser.add_argument("--fanout-runs", type=int, default=300)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    if not args.database_url:
        parser.error("--database-url or DOCKET_BENCH_DATABASE_URL is required")
    if args.repeats < 1 or args.warmup < 0:
        parser.error("--repeats must be at least 1 and --warmup non-negative")
    pool_size = args.pool_size or (2 * max(args.levels) + 8)
    if pool_size < 2:
        parser.error("--pool-size must be at least 2")
    if min(args.single_runs, args.chain_runs, args.fanout_runs) <= 0:
        parser.error("scenario run counts must be positive")

    counts = {
        "single_runs": args.single_runs,
        "chain_runs": args.chain_runs,
        "fanout_runs": args.fanout_runs,
    }
    selected = set(args.scenarios)
    scenario_defs = [
        scenario_def for scenario_def in SCENARIOS if scenario_def[0] in selected
    ]
    docket_results: list[dict[str, Any]] = []
    docket_metadata: dict[str, str] = {}
    langgraph_results: list[dict[str, Any]] = []
    langgraph_tables: list[str] = []

    def run_docket() -> None:
        nonlocal docket_results, docket_metadata
        docket_results, docket_metadata = benchmark_docket(
            args.database_url,
            args.levels,
            args.repeats,
            args.warmup,
            counts,
            scenario_defs,
            pool_size,
        )

    def run_langgraph() -> None:
        nonlocal langgraph_results, langgraph_tables
        schema = make_schema_name()
        create_schema(args.database_url, schema)
        try:
            langgraph_results, langgraph_tables = asyncio.run(
                benchmark_langgraph(
                    args.database_url,
                    schema,
                    args.levels,
                    args.repeats,
                    args.warmup,
                    counts,
                    scenario_defs,
                    pool_size,
                    args.langgraph_durability,
                )
            )
        finally:
            drop_schema(args.database_url, schema)

    if args.framework_order == "docket-first":
        run_docket()
        run_langgraph()
    else:
        run_langgraph()
        run_docket()

    generated_at = datetime.now(UTC).isoformat()
    output = args.output or (
        ROOT
        / "tmp"
        / "bench"
        / "compare"
        / "postgres"
        / datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    )
    output.mkdir(parents=True, exist_ok=True)
    dirty = bool(git_value("status", "--porcelain"))

    payload = {
        "generated_at": generated_at,
        "config": {
            "levels": args.levels,
            "pool_size": pool_size,
            "scenarios": args.scenarios,
            "framework_order": args.framework_order,
            "langgraph_durability": args.langgraph_durability,
            "docket_completion_wait": "local_terminal_telemetry",
            "repeats": args.repeats,
            "warmup": args.warmup,
            **counts,
        },
        "environment": {
            "docket_commit": git_value("rev-parse", "HEAD"),
            "docket_dirty": dirty,
            "langgraph": version("langgraph"),
            "langgraph_checkpoint_postgres": version(
                "langgraph-checkpoint-postgres"
            ),
            "psycopg": version("psycopg"),
            "psycopg_pool": version("psycopg-pool"),
            "elixir": docket_metadata.get("elixir", "unknown"),
            "otp": docket_metadata.get("otp", "unknown"),
            "postgres_version_num": docket_metadata.get(
                "postgres_version_num", "unknown"
            ),
            "database": docket_metadata.get("database", "unknown"),
            "python": platform.python_version(),
            "platform": platform.platform(),
            "langgraph_tables": langgraph_tables,
        },
        "summary": aggregate(
            docket_results, langgraph_results, counts, scenario_defs
        ),
        "raw_results": docket_results + langgraph_results,
    }
    report = render_report(payload)
    (output / "results.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    (output / "report.md").write_text(report, encoding="utf-8")
    print(report)
    print(f"Artifacts: {output}")


if __name__ == "__main__":
    main()
