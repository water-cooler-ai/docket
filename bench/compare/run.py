#!/usr/bin/env python3
"""Run equivalent in-memory Docket and LangGraph microbenchmarks."""

from __future__ import annotations

import argparse
import gc
import json
import os
import platform
import statistics
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Callable, TypedDict

from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph


ROOT = Path(__file__).resolve().parents[2]
SCENARIOS = (
    ("single_node", ("chain", 1)),
    ("chain_10", ("chain", 10)),
    ("fanout_8", ("fanout", 8)),
)


class State(TypedDict):
    token: int


def noop(_state: State) -> dict[str, object]:
    return {}


@dataclass
class Result:
    framework: str
    scenario: str
    mode: str
    iterations: int
    median_ns: float
    p95_ns: float
    min_ns: float
    max_ns: float
    samples_ns: list[float]

    @property
    def median_ops_per_second(self) -> float:
        return 1_000_000_000 / self.median_ns


def build_langgraph(
    shape: tuple[str, int], *, checkpointer: InMemorySaver | None = None
):
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

    return builder.compile(checkpointer=checkpointer)


def time_ns(call: Callable[[], None]) -> int:
    started = time.perf_counter_ns()
    call()
    return time.perf_counter_ns() - started


def repeat(call: Callable[[int], None], count: int) -> None:
    for index in range(count):
        call(index)


def calibrate(call: Callable[[int], None], target_seconds: float, cap: int) -> int:
    probe_iterations = 10
    elapsed = max(time_ns(lambda: repeat(call, probe_iterations)), 1)
    estimate = round(target_seconds * 1_000_000_000 * probe_iterations / elapsed)
    return max(10, min(estimate, cap))


def nearest_rank(sorted_values: list[float], fraction: float) -> float:
    index = max(int(len(sorted_values) * fraction + 0.999999) - 1, 0)
    return sorted_values[index]


def summarize(
    framework: str,
    scenario: str,
    mode: str,
    iterations: int,
    samples: list[float],
) -> Result:
    ordered = sorted(samples)
    return Result(
        framework=framework,
        scenario=scenario,
        mode=mode,
        iterations=iterations,
        median_ns=statistics.median(ordered),
        p95_ns=nearest_rank(ordered, 0.95),
        min_ns=ordered[0],
        max_ns=ordered[-1],
        samples_ns=samples,
    )


def benchmark_langgraph(
    target_seconds: float, repeats: int, warmup: int
) -> list[Result]:
    results: list[Result] = []

    for scenario, shape in SCENARIOS:
        plain_graph = build_langgraph(shape)
        plain_call = lambda _index: plain_graph.invoke({"token": 0})
        repeat(plain_call, warmup)
        iterations = calibrate(plain_call, target_seconds, cap=5_000)
        samples: list[float] = []

        for _ in range(repeats):
            gc.collect()
            elapsed = time_ns(lambda: repeat(plain_call, iterations))
            samples.append(elapsed / iterations)

        results.append(
            summarize("langgraph", scenario, "no_checkpointer", iterations, samples)
        )

        # Use a fresh saver for each measured batch so retained checkpoints from
        # earlier samples do not distort later samples. Saver construction and
        # graph compilation remain outside the timed region, as they do for
        # Docket's precompiled runtime graph.
        warm_graph = build_langgraph(shape, checkpointer=InMemorySaver())
        repeat(
            lambda index: warm_graph.invoke(
                {"token": 0},
                {"configurable": {"thread_id": f"warm-{scenario}-{index}"}},
            ),
            warmup,
        )

        calibration_graph = build_langgraph(shape, checkpointer=InMemorySaver())
        checkpoint_call = lambda index: calibration_graph.invoke(
            {"token": 0},
            {"configurable": {"thread_id": f"cal-{scenario}-{index}"}},
        )
        checkpoint_iterations = calibrate(
            checkpoint_call, target_seconds, cap=1_000
        )
        checkpoint_samples: list[float] = []

        for sample_index in range(repeats):
            graph = build_langgraph(shape, checkpointer=InMemorySaver())
            gc.collect()
            elapsed = time_ns(
                lambda: repeat(
                    lambda index: graph.invoke(
                        {"token": 0},
                        {
                            "configurable": {
                                "thread_id": f"sample-{sample_index}-{index}"
                            }
                        },
                    ),
                    checkpoint_iterations,
                )
            )
            checkpoint_samples.append(elapsed / checkpoint_iterations)

        results.append(
            summarize(
                "langgraph",
                scenario,
                "memory_checkpointer",
                checkpoint_iterations,
                checkpoint_samples,
            )
        )

    return results


def benchmark_docket(
    target_seconds: float, repeats: int, warmup: int
) -> tuple[list[Result], dict[str, str]]:
    command = [
        "mix",
        "run",
        "bench/compare/docket_runner.exs",
        "--target-seconds",
        str(target_seconds),
        "--repeats",
        str(repeats),
        "--warmup",
        str(warmup),
    ]
    completed = subprocess.run(
        command,
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
        env={**os.environ, "MIX_ENV": "dev"},
    )

    results: list[Result] = []
    metadata: dict[str, str] = {}

    for line in completed.stdout.splitlines():
        if line.startswith("META,"):
            _, framework, elixir, otp, *_ = line.split(",")
            metadata = {
                "framework": framework,
                "elixir": elixir,
                "otp": otp,
            }
        elif line.startswith("RESULT,"):
            (
                _,
                framework,
                scenario,
                mode,
                iterations,
                median_ns,
                p95_ns,
                min_ns,
                max_ns,
                samples_ns,
            ) = line.split(",")
            results.append(
                Result(
                    framework=framework,
                    scenario=scenario,
                    mode=mode,
                    iterations=int(iterations),
                    median_ns=float(median_ns),
                    p95_ns=float(p95_ns),
                    min_ns=float(min_ns),
                    max_ns=float(max_ns),
                    samples_ns=[float(value) for value in samples_ns.split("|")],
                )
            )

    if len(results) != len(SCENARIOS):
        raise RuntimeError(
            "Docket runner returned an unexpected result set.\n"
            f"stdout:\n{completed.stdout}\nstderr:\n{completed.stderr}"
        )

    return results, metadata


def distribution_version(package: str) -> str:
    try:
        from importlib.metadata import version

        return version(package)
    except Exception:
        return "unknown"


def comparison_rows(results: list[Result]) -> list[dict[str, object]]:
    indexed = {(result.scenario, result.framework, result.mode): result for result in results}
    rows: list[dict[str, object]] = []

    for scenario, _shape in SCENARIOS:
        docket = indexed[(scenario, "docket", "inline")]
        for mode in ("no_checkpointer", "memory_checkpointer"):
            other = indexed[(scenario, "langgraph", mode)]
            rows.append(
                {
                    "scenario": scenario,
                    "langgraph_mode": mode,
                    "docket_median_ns": docket.median_ns,
                    "langgraph_median_ns": other.median_ns,
                    "docket_over_langgraph": docket.median_ns / other.median_ns,
                }
            )

    return rows


def format_duration(ns: float) -> str:
    if ns >= 1_000_000:
        return f"{ns / 1_000_000:.2f} ms"
    if ns >= 1_000:
        return f"{ns / 1_000:.1f} µs"
    return f"{ns:.0f} ns"


def render_report(payload: dict[str, object]) -> str:
    indexed = {
        (result["scenario"], result["framework"], result["mode"]): result
        for result in payload["results"]
    }
    lines = [
        "# Docket vs LangGraph in-memory benchmark",
        "",
        f"Generated: {payload['generated_at']}",
        "",
        "| Scenario | Docket inline | LangGraph no checkpointer | Ratio | LangGraph memory checkpointer | Ratio |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]

    for scenario, _shape in SCENARIOS:
        docket = indexed[(scenario, "docket", "inline")]
        plain = indexed[(scenario, "langgraph", "no_checkpointer")]
        memory = indexed[(scenario, "langgraph", "memory_checkpointer")]
        plain_ratio = docket["median_ns"] / plain["median_ns"]
        memory_ratio = docket["median_ns"] / memory["median_ns"]
        lines.append(
            f"| {scenario} | {format_duration(docket['median_ns'])} | "
            f"{format_duration(plain['median_ns'])} | {plain_ratio:.2f}× | "
            f"{format_duration(memory['median_ns'])} | {memory_ratio:.2f}× |"
        )

    lines.extend(
        [
            "",
            "Ratios are Docket time divided by LangGraph time: below 1.0× means "
            "Docket was faster. Values are medians of independently timed batches; "
            "each framework compiled the graph before timing.",
            "",
            "Docket inline always constructs its run document, transition events, "
            "and checkpoint values. LangGraph's no-checkpointer mode is therefore a "
            "lower-overhead bound, while its in-memory checkpointer retains full "
            "per-thread checkpoint history and is an upper-overhead comparison. "
            "Neither mode includes PostgreSQL or network I/O.",
            "",
            "## Environment",
            "",
            f"- Docket commit: `{payload['environment']['docket_commit']}`"
            f"{' (dirty)' if payload['environment']['docket_dirty'] else ''}",
            f"- LangGraph / checkpoint / langchain-core: "
            f"`{payload['environment']['langgraph']}` / "
            f"`{payload['environment']['langgraph_checkpoint']}` / "
            f"`{payload['environment']['langchain_core']}`",
            f"- Elixir / OTP: `{payload['environment']['elixir']}` / "
            f"`{payload['environment']['otp']}`",
            f"- Python: `{payload['environment']['python']}`",
            f"- Platform: `{payload['environment']['platform']}`",
            f"- Target sample duration: `{payload['config']['target_seconds']}s`; "
            f"repeats: `{payload['config']['repeats']}`; warmups: "
            f"`{payload['config']['warmup']}`",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target-seconds", type=float, default=0.5)
    parser.add_argument("--repeats", type=int, default=7)
    parser.add_argument("--warmup", type=int, default=50)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    if args.target_seconds <= 0 or args.repeats < 3 or args.warmup < 0:
        parser.error(
            "--target-seconds must be positive, --repeats at least 3, "
            "and --warmup non-negative"
        )

    docket_results, docket_metadata = benchmark_docket(
        args.target_seconds, args.repeats, args.warmup
    )
    langgraph_results = benchmark_langgraph(
        args.target_seconds, args.repeats, args.warmup
    )
    results = docket_results + langgraph_results
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    dirty = bool(
        subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    )

    generated_at = datetime.now(UTC).isoformat()
    output = args.output or (
        ROOT
        / "tmp"
        / "bench"
        / "compare"
        / datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    )
    output.mkdir(parents=True, exist_ok=True)

    payload: dict[str, object] = {
        "generated_at": generated_at,
        "config": {
            "target_seconds": args.target_seconds,
            "repeats": args.repeats,
            "warmup": args.warmup,
        },
        "environment": {
            "docket_commit": commit,
            "docket_dirty": dirty,
            "langgraph": distribution_version("langgraph"),
            "langgraph_checkpoint": distribution_version("langgraph-checkpoint"),
            "langchain_core": distribution_version("langchain-core"),
            "ormsgpack": distribution_version("ormsgpack"),
            "xxhash": distribution_version("xxhash"),
            "elixir": docket_metadata.get("elixir", "unknown"),
            "otp": docket_metadata.get("otp", "unknown"),
            "python": platform.python_version(),
            "platform": platform.platform(),
        },
        "results": [
            {**asdict(result), "median_ops_per_second": result.median_ops_per_second}
            for result in results
        ],
        "comparisons": comparison_rows(results),
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
