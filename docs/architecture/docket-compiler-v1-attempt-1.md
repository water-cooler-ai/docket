# Docket Compiler v1 — Attempt 1 Implementation Design

Status: active implementation attempt
Date: 2026-07-02
Parent design: `docs/architecture/docket-compiler-design.md`

This note records the concrete implementation decisions for Attempt 1 of the
compiler slice. Where the reference documents disagree or leave decisions open,
this document is the tiebreaker for this attempt. Tests are written first; the
compiler is implemented to make them pass.

## 1. Decisions Resolving Open Questions

1. **Runtime graph shape** follows `docket-compiler-design.md` section 10, not
   the older sketch in `docket-runtime-design.md` section 8. The struct carries
   `id`, `graph_id`, `graph_hash`, `channels`, `nodes`, `edges`, `outputs`,
   `policies`, `lowering`. Runtime-loop-only concerns (`input_channels`,
   `output_channels` lists) are derivable and omitted in v1.
2. **Runtime ID namespacing** uses the recommended v1 policy:
   `node:<node_id>`, `input:<input_id>`, `state:<field_id>`, `edge:<edge_id>`,
   `output:<output_id>`.
3. **Runtime edge descriptors are plain maps** keyed by public edge ID
   (compiler design 9.11/section 10, open decision 4). No runtime edge struct
   in Attempt 1.
4. **Channel types in v1** are `:last_value` (inputs, state), `:ephemeral`
   (single-source edge activations), and `:barrier` (multi-source edge
   activations, carrying the required source node IDs). Other channel types
   from the runtime design are post-v1.
5. **Barrier sources are embedded in the channel** so the runtime loop can
   track per-source completion without re-deriving it from edges.
6. **Profiles**: `compile/2` and `verify/2` accept `profile: :publish | :run`
   (default `:publish`). Both apply identical rules in v1. No `:preview`
   profile yet (open decision 3).
7. **Cycles**: cycles are allowed. A cycle without a `max_supersteps` limit
   (graph policy `"max_supersteps"` or `opts[:max_supersteps]` runtime
   default) is an `:unbounded_cycle` error. A bounded cycle containing no
   guarded edge gets an `:unguarded_cycle` warning.
8. **Reachability through multi-source edges** requires all sources reachable
   (a barrier that can never fully fire does not make its target reachable).
   Computed as a fixed point.
9. **Schema validation engine**: the compiler needs to validate field defaults
   and node config values, so Attempt 1 adds a minimal public
   `Docket.Schema.validate/2` (type checks for `:string`, `:float`, `:map`,
   `:object`, `:enum`; `required` object fields; enum membership; unknown
   object keys rejected). Constraints beyond these are ignored in v1.
10. **Output schema compatibility** is type equality, plus enum-value subset
    (output enum values must be a superset of the source's so every source
    value projects cleanly). Omitted output schema inherits the source schema.
11. **Node implementation callback exports**: a module that loads but does not
    export `config_schema/0` and `call/3` fails with
    `:invalid_node_implementation` (new code; the design's family table is
    non-exhaustive). `:node_module_not_loaded` stays reserved for load
    failures, `:unsupported_node_implementation` for non-module types or
    functions other than `:call`.
12. **Node config defaults** from `config_schema/0` are applied into the
    runtime node's `config` during lowering and are never written back to the
    public graph.
13. **Determinism**: every map iteration is sorted by public ID; diagnostics
    are emitted in fixed phase order and sorted by path within a phase; the
    runtime graph ID is derived from the graph ID and content hash
    (`<graph_id>@<first 12 hash chars>`), so identical graphs compile to
    identical runtime graphs.
14. **Ingest canonicalizes through the wire format**: graphs are free-form in
    memory (serialization happens at compile/hash/storage time, not per
    edit), so compiler ingest round-trips the document through
    `Serializer.dump/2` + `load!/2` and validates/lowers the canonical form
    (atom keys and values in open content become strings, exactly as storage
    would see them). Canonicalization is never written back to the public
    graph. A graph that cannot cross the boundary falls back to raw
    validation for granular, path-bearing diagnostics next to the ingest
    error (`:non_durable_graph_value` for non-durable content; the
    serializer's own code otherwise). Graphs claiming an unsupported
    `schema_version` are never canonicalized, because the v1 wire format
    stamps version 1 on dump.
15. **verify/compile share the full pipeline** including lowering and runtime
    graph self-validation; `verify/2` throws the runtime graph away and
    returns the graph with fresh diagnostics. Stale diagnostics on the input
    graph are always ignored and never echoed back.

## 2. Module Layout

```text
lib/docket/runtime/graph.ex                Docket.Runtime.Graph
lib/docket/runtime/graph/node.ex           Docket.Runtime.Graph.Node
lib/docket/runtime/graph/channel.ex        Docket.Runtime.Graph.Channel
lib/docket/runtime/graph/lowering.ex       Docket.Runtime.Graph.Lowering

lib/docket/graph/compiler.ex               public verify/compile facade
lib/docket/graph/compiler/diagnostics.ex   diagnostic builders
lib/docket/graph/compiler/validation.ex    phases 9.2 - 9.10
lib/docket/graph/compiler/lowering.ex      phase 9.11
lib/docket/graph/compiler/runtime_validation.ex  phase 9.12
lib/docket/schema.ex                       + validate/2 (minimal engine)
```

Only `Docket.Graph.Compiler`, the runtime graph structs, and
`Docket.Schema.validate/2` are public. Compiler submodules are `@moduledoc
false`. The suggested `Compiler.Context` module turned out unnecessary in
Attempt 1: validation passes are stateless functions over the graph plus
opts, which keeps ordering trivially deterministic.

Diagnostic codes added beyond the design's example families:

- `:invalid_field_default` - field default fails its own schema
- `:invalid_node_implementation` - module loads but misses the
  `Docket.Node` callbacks (decision 11)
- `:duplicate_edge_source` - duplicate node in a multi-source list
- `:unguarded_branch_edge` (warning) - grouped edge without a guard
- `:unguarded_cycle` (warning) - bounded cycle with no guarded edge
  (decision 7)

## 3. Pipeline

```text
verify/compile
  -> Context.new(graph, opts)        ignore stale diagnostics, index IDs
  -> Validation.run(context)         9.2 document, 9.3 fields, 9.4 outputs,
                                     9.5 nodes, 9.6 edges, 9.7 branches,
                                     9.8 guards, 9.9 topology, 9.10 cycles
  -> if blocking errors: {:error, graph + diagnostics}
  -> Lowering.run(context)           9.11
  -> RuntimeValidation.run(context)  9.12
  -> verify:  {:ok|:error, graph + diagnostics}
     compile: {:ok, runtime_graph} | {:error, graph + diagnostics}
```

All validation phases run even when earlier phases produced errors (maximum
diagnostic yield); only lowering is gated on a clean error list. Phases that
need facts from invalid records skip just those records.

## 4. Test Plan (written before the compiler)

```text
test/support/fixtures/test_nodes.ex        executable Docket.Node fixtures
test/support/fixtures/graph_fixtures.ex    graph fixtures from the catalogs
test/support/docket_case.ex                assert_diagnostic + compile helpers

test/docket/schema_test.exs                validate/2 minimal engine
test/docket/graph/compiler/validation_test.exs
test/docket/graph/compiler/lowering_test.exs
test/docket/graph/compiler/lowering_metadata_test.exs
test/docket/graph/compiler/determinism_test.exs
```

Coverage follows compiler design section 15.1/15.2/15.4 plus the baseline
assertions from the v1 test suite design section 4.3. Compile-and-run
integration tests (15.3) are deferred until `Docket.Test.run_inline/3` exists.
