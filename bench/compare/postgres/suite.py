#!/usr/bin/env python3
"""Run an order-balanced PostgreSQL comparison suite and aggregate raw trials."""

from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import sys
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql


ROOT = Path(__file__).resolve().parents[3]
RUNNER = ROOT / "bench" / "compare" / "postgres" / "run.py"
SCENARIOS = ("single_node", "chain_10", "fanout_8")
OWNED_SCHEMA_PREFIXES = ("docket_bench_", "langgraph_bench_")


def parse_int_list(value: str) -> list[int]:
    try:
        values = [int(item.strip()) for item in value.split(",") if item.strip()]
    except ValueError as error:
        raise argparse.ArgumentTypeError("expected comma-separated integers") from error
    if not values or any(item <= 0 for item in values) or len(set(values)) != len(
        values
    ):
        raise argparse.ArgumentTypeError("values must be unique positive integers")
    return values


def parse_scenarios(value: str) -> list[str]:
    values = [item.strip() for item in value.split(",") if item.strip()]
    if (
        not values
        or len(set(values)) != len(values)
        or any(item not in SCENARIOS for item in values)
    ):
        raise argparse.ArgumentTypeError(
            f"scenarios must be unique names from: {','.join(SCENARIOS)}"
        )
    return values


def median(values: list[float]) -> float:
    return statistics.median(values)


def coefficient_of_variation(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = statistics.mean(values)
    return statistics.stdev(values) / mean if mean else 0.0


def format_optional_ratio(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.3f}×"


def owned_schemas(database_url: str) -> list[str]:
    with psycopg.connect(database_url, autocommit=True) as connection:
        rows = connection.execute(
            """
            SELECT nspname
            FROM pg_namespace
            WHERE nspname LIKE 'docket_bench_%'
               OR nspname LIKE 'langgraph_bench_%'
            ORDER BY nspname
            """
        ).fetchall()
    return [str(row[0]) for row in rows]


def cleanup_owned_schemas(database_url: str) -> list[str]:
    names = owned_schemas(database_url)
    with psycopg.connect(database_url, autocommit=True) as connection:
        for name in names:
            if not name.startswith(OWNED_SCHEMA_PREFIXES):
                raise RuntimeError(f"refusing to drop non-benchmark schema {name!r}")
            connection.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    sql.Identifier(name)
                )
            )
    return names


def trial_order(repeat: int, scenario_index: int, level_index: int) -> str:
    return (
        "docket-first"
        if (repeat + scenario_index + level_index) % 2 == 0
        else "langgraph-first"
    )


def run_trial(
    *,
    output: Path,
    phase: str,
    scenario: str,
    concurrency: int,
    repeat: int,
    order: str,
    runs: int,
    warmup: int,
    pool_size: int,
    langgraph_durability: str,
) -> dict[str, Any]:
    trial_output = (
        output
        / "trials"
        / f"{phase}-{scenario}-c{concurrency}-r{repeat}-{order}"
    )
    command = [
        sys.executable,
        str(RUNNER),
        "--scenarios",
        scenario,
        "--levels",
        str(concurrency),
        "--pool-size",
        str(pool_size),
        "--framework-order",
        order,
        "--langgraph-durability",
        langgraph_durability,
        "--repeats",
        "1",
        "--warmup",
        str(warmup),
        "--single-runs",
        str(runs),
        "--chain-runs",
        str(runs),
        "--fanout-runs",
        str(runs),
        "--output",
        str(trial_output),
    ]
    print(
        f"START phase={phase} scenario={scenario} concurrency={concurrency} "
        f"repeat={repeat} order={order} runs={runs} "
        f"langgraph_durability={langgraph_durability}",
        flush=True,
    )
    completed = subprocess.run(
        command,
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        env=os.environ,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"trial failed with exit {completed.returncode}\n"
            f"stdout:\n{completed.stdout}\nstderr:\n{completed.stderr}"
        )
    payload = json.loads((trial_output / "results.json").read_text())
    for result in payload["raw_results"]:
        result["phase"] = phase
        result["suite_repeat"] = repeat
        result["framework_order"] = order
    print(
        "DONE "
        + " ".join(
            f"{result['framework']}={result['runs_per_second']:.1f}r/s"
            for result in payload["raw_results"]
        ),
        flush=True,
    )
    return payload


def aggregate(raw_results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, int], list[dict[str, Any]]] = defaultdict(list)
    for result in raw_results:
        grouped[
            (result["phase"], result["scenario"], result["concurrency"])
        ].append(result)

    summary: list[dict[str, Any]] = []
    for (phase, scenario, concurrency), results in sorted(grouped.items()):
        docket = [
            result for result in results if result["framework"] == "docket_postgres"
        ]
        langgraph = [
            result
            for result in results
            if result["framework"] == "langgraph_postgres"
        ]
        docket_by_repeat = {result["suite_repeat"]: result for result in docket}
        langgraph_by_repeat = {
            result["suite_repeat"]: result for result in langgraph
        }
        repeats = sorted(set(docket_by_repeat) & set(langgraph_by_repeat))
        paired = [
            {
                "repeat": repeat,
                "order": docket_by_repeat[repeat]["framework_order"],
                "ratio": docket_by_repeat[repeat]["runs_per_second"]
                / langgraph_by_repeat[repeat]["runs_per_second"],
            }
            for repeat in repeats
        ]
        docket_rates = [result["runs_per_second"] for result in docket]
        langgraph_rates = [result["runs_per_second"] for result in langgraph]

        row: dict[str, Any] = {
            "phase": phase,
            "scenario": scenario,
            "concurrency": concurrency,
            "runs_per_trial": results[0]["runs"],
            "repeats": len(repeats),
            "docket_runs_per_second_median": median(docket_rates),
            "docket_runs_per_second_cv": coefficient_of_variation(docket_rates),
            "langgraph_runs_per_second_median": median(langgraph_rates),
            "langgraph_runs_per_second_cv": coefficient_of_variation(
                langgraph_rates
            ),
            "docket_over_langgraph_ratio_of_medians": median(docket_rates)
            / median(langgraph_rates),
            "paired_ratio_median": median([item["ratio"] for item in paired]),
            "docket_elapsed_ms_median": median(
                [result["elapsed_ms"] for result in docket]
            ),
            "langgraph_elapsed_ms_median": median(
                [result["elapsed_ms"] for result in langgraph]
            ),
            "docket_p50_ms_median": median(
                [result["p50_ms"] for result in docket]
            ),
            "langgraph_p50_ms_median": median(
                [result["p50_ms"] for result in langgraph]
            ),
            "docket_p95_ms_median": median(
                [result["p95_ms"] for result in docket]
            ),
            "langgraph_p95_ms_median": median(
                [result["p95_ms"] for result in langgraph]
            ),
            "docket_p99_ms_median": median(
                [result["p99_ms"] for result in docket]
            ),
            "langgraph_p99_ms_median": median(
                [result["p99_ms"] for result in langgraph]
            ),
            "docket_logical_bytes_per_run": median(
                [result["logical_bytes"] / result["runs"] for result in docket]
            ),
            "langgraph_logical_bytes_per_run": median(
                [result["logical_bytes"] / result["runs"] for result in langgraph]
            ),
        }
        for order in ("docket-first", "langgraph-first"):
            ratios = [item["ratio"] for item in paired if item["order"] == order]
            row[f"paired_ratio_median_{order.replace('-', '_')}"] = (
                median(ratios) if ratios else None
            )
        summary.append(row)
    return summary


def render_report(payload: dict[str, Any]) -> str:
    lines = [
        "# Docket vs LangGraph repeated PostgreSQL suite",
        "",
        f"Generated: {payload['generated_at']}",
        "",
        "| Phase | Scenario | C | Runs | N | Docket r/s (CV) | "
        "LangGraph r/s (CV) | Paired ratio |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in payload["summary"]:
        lines.append(
            f"| {row['phase']} | {row['scenario']} | {row['concurrency']} | "
            f"{row['runs_per_trial']:,} | {row['repeats']} | "
            f"{row['docket_runs_per_second_median']:,.1f} "
            f"({row['docket_runs_per_second_cv']:.1%}) | "
            f"{row['langgraph_runs_per_second_median']:,.1f} "
            f"({row['langgraph_runs_per_second_cv']:.1%}) | "
            f"{row['paired_ratio_median']:.3f}× |"
        )

    lines.extend(
        [
            "",
            "## Completion timeline",
            "",
            "| Phase | Scenario | C | Docket total | LangGraph total | "
            "Docket p50 / p95 / p99 | LangGraph p50 / p95 / p99 |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in payload["summary"]:
        lines.append(
            f"| {row['phase']} | {row['scenario']} | {row['concurrency']} | "
            f"{row['docket_elapsed_ms_median'] / 1_000:.3f}s | "
            f"{row['langgraph_elapsed_ms_median'] / 1_000:.3f}s | "
            f"{row['docket_p50_ms_median'] / 1_000:.3f} / "
            f"{row['docket_p95_ms_median'] / 1_000:.3f} / "
            f"{row['docket_p99_ms_median'] / 1_000:.3f}s | "
            f"{row['langgraph_p50_ms_median'] / 1_000:.3f} / "
            f"{row['langgraph_p95_ms_median'] / 1_000:.3f} / "
            f"{row['langgraph_p99_ms_median'] / 1_000:.3f}s |"
        )

    lines.extend(
        [
            "",
            "## Order check",
            "",
            "| Phase | Scenario | C | Docket first | LangGraph first |",
            "| --- | --- | ---: | ---: | ---: |",
        ]
    )
    for row in payload["summary"]:
        docket_first = row["paired_ratio_median_docket_first"]
        langgraph_first = row["paired_ratio_median_langgraph_first"]
        lines.append(
            f"| {row['phase']} | {row['scenario']} | {row['concurrency']} | "
            f"{format_optional_ratio(docket_first)} | "
            f"{format_optional_ratio(langgraph_first)} |"
        )

    config = payload["config"]
    lines.extend(
        [
            "",
            "Ratios are paired Docket throughput divided by LangGraph throughput; "
            "values near 1.0× indicate parity. CV is the sample coefficient of "
            "variation across complete trials. Framework order alternates within "
            "every configuration.",
            "",
            f"Matrix: `{config['matrix_runs']}` runs, "
            f"`{config['matrix_repeats']}` repeats, concurrency "
            f"`{config['matrix_levels']}`. Scale: `{config['scale_runs']}` runs, "
            f"`{config['scale_repeats']}` repeats, concurrency "
            f"`{config['scale_level']}`. Database pool cap: "
            f"`{config['pool_size']}`. LangGraph durability: "
            f"`{config['langgraph_durability']}`. Docket completion wait: "
            f"`{config['docket_completion_wait']}`.",
            "",
            f"Cleanup audit: `{payload['cleanup_audit']}`.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DOCKET_BENCH_DATABASE_URL"),
    )
    parser.add_argument(
        "--profiles",
        choices=("matrix", "scale", "both"),
        default="both",
    )
    parser.add_argument("--scenarios", type=parse_scenarios, default=list(SCENARIOS))
    parser.add_argument("--matrix-levels", type=parse_int_list, default=[1, 4, 8])
    parser.add_argument("--matrix-runs", type=int, default=500)
    parser.add_argument("--matrix-repeats", type=int, default=6)
    parser.add_argument("--scale-level", type=int, default=8)
    parser.add_argument("--scale-runs", type=int, default=5_000)
    parser.add_argument("--scale-repeats", type=int, default=4)
    parser.add_argument("--warmup", type=int, default=20)
    parser.add_argument("--pool-size", type=int, default=16)
    parser.add_argument(
        "--langgraph-durability",
        choices=("sync", "async", "exit"),
        default="sync",
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    if not args.database_url:
        parser.error("--database-url or DOCKET_BENCH_DATABASE_URL is required")
    os.environ["DOCKET_BENCH_DATABASE_URL"] = args.database_url
    positive = (
        args.matrix_runs,
        args.matrix_repeats,
        args.scale_level,
        args.scale_runs,
        args.scale_repeats,
        args.pool_size,
    )
    if any(value <= 0 for value in positive) or args.warmup < 0:
        parser.error("run counts, repeats, levels, and pool size must be positive")

    output = args.output or (
        ROOT
        / "tmp"
        / "bench"
        / "compare"
        / "postgres"
        / f"suite-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}"
    )
    output.mkdir(parents=True, exist_ok=True)

    stale_before = cleanup_owned_schemas(args.database_url)
    if stale_before:
        print(f"CLEANUP before={stale_before}", flush=True)

    payloads: list[dict[str, Any]] = []
    cleanup_after: list[str] = []
    try:
        if args.profiles in ("matrix", "both"):
            for scenario_index, scenario in enumerate(args.scenarios):
                for level_index, concurrency in enumerate(args.matrix_levels):
                    for repeat in range(1, args.matrix_repeats + 1):
                        payloads.append(
                            run_trial(
                                output=output,
                                phase="matrix",
                                scenario=scenario,
                                concurrency=concurrency,
                                repeat=repeat,
                                order=trial_order(
                                    repeat, scenario_index, level_index
                                ),
                                runs=args.matrix_runs,
                                warmup=args.warmup,
                                pool_size=args.pool_size,
                                langgraph_durability=args.langgraph_durability,
                            )
                        )

        if args.profiles in ("scale", "both"):
            for scenario_index, scenario in enumerate(args.scenarios):
                for repeat in range(1, args.scale_repeats + 1):
                    payloads.append(
                        run_trial(
                            output=output,
                            phase="scale",
                            scenario=scenario,
                            concurrency=args.scale_level,
                            repeat=repeat,
                            order=trial_order(repeat, scenario_index, 0),
                            runs=args.scale_runs,
                            warmup=args.warmup,
                            pool_size=args.pool_size,
                            langgraph_durability=args.langgraph_durability,
                        )
                    )
    finally:
        cleanup_after = cleanup_owned_schemas(args.database_url)
        print(f"CLEANUP after={cleanup_after}", flush=True)

    remaining = owned_schemas(args.database_url)
    if remaining:
        raise RuntimeError(f"benchmark schemas remain after cleanup: {remaining}")

    raw_results = [
        result
        for payload in payloads
        for result in payload["raw_results"]
    ]
    invariant_failures = [
        result
        for result in raw_results
        if not all(result["invariants"].values())
    ]
    if invariant_failures:
        raise RuntimeError(f"invariant failures: {invariant_failures}")

    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "config": {
            "profiles": args.profiles,
            "scenarios": args.scenarios,
            "matrix_levels": args.matrix_levels,
            "matrix_runs": args.matrix_runs,
            "matrix_repeats": args.matrix_repeats,
            "scale_level": args.scale_level,
            "scale_runs": args.scale_runs,
            "scale_repeats": args.scale_repeats,
            "warmup": args.warmup,
            "pool_size": args.pool_size,
            "langgraph_durability": args.langgraph_durability,
            "docket_completion_wait": "local_terminal_telemetry",
        },
        "environment": payloads[0]["environment"] if payloads else {},
        "cleanup_audit": {
            "removed_before": stale_before,
            "removed_after": cleanup_after,
            "remaining": remaining,
        },
        "summary": aggregate(raw_results),
        "raw_results": raw_results,
    }
    report = render_report(payload)
    (output / "results.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (output / "report.md").write_text(report, encoding="utf-8")
    print(report, flush=True)
    print(f"Artifacts: {output}", flush=True)


if __name__ == "__main__":
    main()
