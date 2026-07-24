# Docket vs LangGraph

This microbenchmark compares precompiled, in-memory execution of equivalent
graphs:

- one no-op node;
- a ten-node no-op chain;
- an eight-node no-op fan-out.

It reports Docket's processless inline runner beside two LangGraph modes:
without a checkpointer and with `InMemorySaver`. Docket inline always builds
the run document, transition events, and checkpoint values. The two LangGraph
modes therefore provide lower- and upper-overhead reference points rather
than perfectly identical persistence semantics.

Create an isolated Python environment and run the suite:

```sh
python3 -m venv /tmp/docket-langgraph-bench
/tmp/docket-langgraph-bench/bin/python -m pip install \
  -r bench/compare/requirements.txt
/tmp/docket-langgraph-bench/bin/python bench/compare/run.py
```

The default run uses seven measured batches targeting 0.5 seconds each, after
50 warmup invocations. Override those controls when needed:

```sh
/tmp/docket-langgraph-bench/bin/python bench/compare/run.py \
  --target-seconds 1.0 \
  --repeats 9 \
  --warmup 100
```

Each run prints a Markdown summary and writes `results.json` plus `report.md`
under `tmp/bench/compare/<timestamp>/`. Ratios are Docket median time divided
by LangGraph median time, so values below `1.0×` favor Docket.

The in-memory suite above deliberately excludes PostgreSQL. Use the following
matrix when comparing durable execution.

## PostgreSQL comparison

`postgres/run.py` compares durable execution on the same PostgreSQL server. It
uses isolated scratch schemas and removes them at the end of the run.

Docket runs through its supervised production runtime with LISTEN/NOTIFY,
durable queueing, claim fencing, and retained events. LangGraph runs through
`AsyncPostgresSaver` with one compiled graph/checkpointer per worker over a
shared Psycopg connection pool. Every run or thread uses a unique ID.
LangGraph uses `sync` checkpoint durability by default so each checkpoint is
persisted before the next superstep begins, matching Docket's durable
transition boundary more closely. Use `--langgraph-durability async` or
`--langgraph-durability exit` to measure LangGraph's weaker durability modes
as separate throughput references.

The Docket driver waits on its local post-commit terminal telemetry rather
than polling `inspect_run` over PostgreSQL. After all workflows signal
completion, the harness performs one durable batch read and runs its SQL-backed
row-count and lifecycle invariants. This keeps observer traffic out of the
timed execution path without treating an in-memory signal as the correctness
authority.

Use a dedicated benchmark-capable database:

```sh
createdb docket_langgraph_bench

/tmp/docket-langgraph-bench/bin/python bench/compare/postgres/run.py \
  --database-url postgres://localhost:5432/docket_langgraph_bench
```

The default matrix submits 300 runs per graph shape, measures one, eight, and
32 complete workflows in flight, and repeats every trial three times. It
reports median throughput, p95 queue-plus-service latency, logical PostgreSQL
row bytes per run, and retained event or checkpoint row counts. Raw trials and
a Markdown report are written under
`tmp/bench/compare/postgres/<timestamp>/`.

For a single large queue-drain trial, select one shape and reduce repeats:

```sh
/tmp/docket-langgraph-bench/bin/python bench/compare/postgres/run.py \
  --database-url postgres://localhost:5432/docket_langgraph_bench \
  --scenarios single_node \
  --levels 10 \
  --single-runs 10000 \
  --repeats 1
```

This scale mode also reports total drain time and the p50, p95, and p99
completion points measured from batch submission. Use the default three or
more repeats when comparing medians rather than inspecting one large run.

The benchmark does not run the LangGraph Agent Server. It compares Docket's
open-source production runtime with the open-source LangGraph execution engine
and PostgreSQL checkpointer.
