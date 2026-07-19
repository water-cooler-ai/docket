# Docket v1.1 Roadmap — Composability & Ergonomics

Status: slices 1–5 implemented (PR #6, 2026-07-05): schema-v1.1, reducers,
schema-shorthand, inline-fields, telemetry-events. The reducer contract
rationale moved to `docs/architecture/docket-reducers-design.md`; API truth
lives in module docs. Themes 6 (graph module DSL) and 7 (subgraph
composition) remain open design space, recorded below. Theme 9 records the
tenant-claim fairness follow-up targeted at v0.1.1. Theme 10 records the
`{:await}` late-completion protocol, sized during the claim-freshness review.

The v1.1 theme: **make building graphs feel natural without adding a second
canonical model.** Every proposal below is sugar or extension over the existing
graph contract, and everything a helper does must be expressible as plain
`put_*` calls.

---

## Theme 1 — Reducer library (aggregates)

v1 ships only `:last_value`, and the runtime applies reducers only to resolve
_same-step_ write conflicts (`Docket.Runtime.Algorithm.apply_state_writes/3`
folds the step's writes and replaces the committed value). Aggregates change
the reducer contract: the reducer must fold the **prior committed value** with
the step's deterministically-sorted writes:

```
new_value = reduce(reducer, current_committed_value, sorted_step_writes)
```

`:last_value` is the degenerate case that ignores `current`. This is the one
real semantic extension in the theme; everything else is additive descriptors.

### Proposed built-ins

| Reducer       | Semantics                                            | Options                                                           |
| ------------- | ---------------------------------------------------- | ----------------------------------------------------------------- |
| `last_value`  | existing; last write in sorted node order wins       | —                                                                 |
| `first_value` | keep the first committed value; later writes ignored | —                                                                 |
| `append`      | list accumulation: `current ++ writes`               | `unique: true`, `max_length: n` (sliding window — chat histories) |
| `merge`       | map merge: `Map.merge(current, write)`               | `deep: true`                                                      |
| `sum`         | numeric accumulation                                 | —                                                                 |
| `union`       | list-as-set: append + dedupe                         | `by: path` (dedupe key, e.g. message `"id"`)                      |

### Contract details to settle

- **Write validation becomes reducer-aware.** For an `append` field the node
  writes an _item_, but the committed channel value is a _list_. Validation in
  `validate_write_schema/4` must validate the write against the schema's
  `item` for accumulating reducers, and the field schema stays the truth for
  the committed shape. Compiler diagnostics enforce reducer/schema pairing
  (`append` ⇒ `:list` schema, `sum` ⇒ numeric, `merge` ⇒ `:map`/`:object`).
- **Initial values.** Accumulating fields need a natural zero when unset:
  `[]` for `append`/`union`, `%{}` for `merge`, `0` for `sum` — applied as the
  effective default when the field has none.
- **Open question — list writes to `append`.** Does writing a list append one
  element or concatenate? LangGraph concatenates list writes and appends
  scalar writes. That's convenient but type-ambiguous when `item` is itself a
  list type. Proposal: concatenate when the write is a list and `item` is not
  a list type; otherwise append — and let the compiler flag the ambiguous case.
- **Interrupt resume.** `resolve_interrupt` writes through the resume field's
  reducer, so resolving into an `append` field accumulates naturally.
- **Wire format.** Reducer already serializes as `type` + `opts`; new types
  are additive. Old graphs load unchanged.

### Stretch: custom reducers

Module-referenced reducers, mirroring node implementation refs:
`%{type: :module, module: M, function: :reduce}` with contract
`reduce(current, values, opts) :: value`. Durable (module name is data),
must be pure/deterministic — replan after crash must produce identical
commits. Defer until built-ins prove insufficient; every custom reducer is a
determinism footgun in host hands.

---

## Theme 2 — Schema engine v1.1

Prerequisite for Theme 1 (`append` needs a list type) and the cheapest win in
the whole roadmap.

- **New types:** `:boolean`, `:integer`, `:list` (`Docket.Schema.list(item, opts)`
  — the struct's `item` slot already exists, there is just no builder or
  validation for it).
- **Enforce stored constraints:** `min`/`max` on numbers, `min_length`/
  `max_length`/`pattern` on strings, `min_items`/`max_items` on lists. These
  are already stored in `constraints` and documented as ignored — enforcing
  them changes runtime behavior for existing stored graphs that carry
  constraints. Decision needed: accept as a documented v1.1 behavior change
  (recommended — v1 docs said "ignored in v1", nobody should be relying on
  non-enforcement), or gate behind a graph policy.
- **Object openness:** an `open: true` option on `object` to permit unknown
  keys (today unknown keys are rejected; `map` is the all-or-nothing escape
  hatch).

---

## Theme 3 — Schema shorthand (the "Schema DSL", cheap tier)

Before reaching for macros: accept terse literals anywhere a schema is
expected, normalized by the constructors/editing API into real
`%Docket.Schema{}` values.

```elixir
# atoms as bare types
Docket.Graph.put_input!(g, "message", schema: :string, required: true)

# tuples as type + opts
Docket.Schema.object(%{
  name: :string,
  age: {:integer, min: 0},
  tags: {:list, :string}
})
```

Why this tier first: it is plain data, so it works identically for
hand-written Elixir and compilers—no new concepts, no macro layer to keep in
sync. A macro `config do ... end`
DSL for `Docket.Node.config_schema/0` can sit on top later if the shorthand
still feels heavy (see Theme 6).

---

## Theme 5 — Inline field declaration on `put_node!`

The requested helper: when **adding a node to the graph**, declare the fields
it reads and writes right there, and the editing API materializes them as
graph fields — no separate `put_field!`/`put_input!` calls:

```elixir
graph
|> Docket.Graph.put_node!("draft_reply",
  implementation: MyApp.Nodes.LLM,
  config: %{output_field: "draft_response", ...},
  inputs: %{"customer_message" => [schema: :string, required: true]},
  fields: %{
    "draft_response" => :string,                       # shorthand schema
    "llm_usage" => [schema: :map],
    "messages" => [schema: {:list, :map}, reducer: :append]
  }
)
```

This is pure editing-API expansion: `inputs:`/`fields:` entries become the
exact `put_input!`/`put_field!` calls you would have written by hand. Nothing
new lands on the node record — the document keeps fields as the single
canonical model (the v1 stance from `examples/llm-node.md`), and the hash
reflects the materialized fields because they are ordinary fields.

Rules:

- **Existing field, identical definition** — no-op, so two nodes can declare
  the same shared field and order doesn't matter.
- **Existing field, conflicting definition** — `{:error, %Graph.Error{}}`
  (or raise from the bang form), never a silent overwrite. An explicit
  `put_field!` after the fact still updates freely; only the inline
  declaration is conservative.
- **Deleting the node does not delete the fields** — fields are shared state;
  cleanup stays explicit (`verify/2` can flag orphaned fields as a
  diagnostic).
- Values accept everything `put_field!` accepts, plus the Theme 3 schema
  shorthand, so `"draft_response" => :string` is the minimal spelling.

For generic nodes (like the LLM node) whose config names its own output
fields, some duplication between `config` and `fields:` remains — that's
inherent to config-bound field names and stays the host's call. A
`state_contract/1` node callback that lets the _implementation_ report its
reads/writes for compiler diagnostics is a separate, complementary idea —
parked in Theme 8's explore list, not part of this slice.

---

## Theme 6 — Graph modules and DSL (macro tier)

Make `use Docket.Graph` the natural convention for static graph definitions.
Using the graph module installs the graph DSL, including the schema declaration
tools used by inputs, fields, node-local declarations, and outputs:

```elixir
defmodule MyApp.Graphs.SupportReply do
  use Docket.Graph, id: "support-reply"

  input :customer_message, :string, required: true
  field :messages, {:list, :map}, reducer: :append
  node :draft, MyApp.Nodes.LLM, config: %{...}
  chain [:start, :draft, :finish]
  output :draft_response
end

MyApp.Graphs.SupportReply.graph()           #=> %Docket.Graph{}
MyApp.Graphs.SupportReply.compiled_graph()  #=> %Docket.Runtime.Graph{}
```

The module is compiled and validated in `__before_compile__/1`. Its canonical
`Docket.Graph` and lowered `Docket.Runtime.Graph` are embedded into the BEAM;
they are not written to ETS or `:persistent_term` during compilation and do
not need to be rebuilt during application startup. Invalid static definitions
therefore fail the application build rather than the first run. Literal node
module references must establish compile dependencies so changes to a node's
schema cause dependent graph modules to be recompiled.

`graph/0` preserves the authored canonical document for inspection,
publication, and serialization. `compiled_graph/0` returns the immutable
runtime materialization and is the direct execution path. Runtime-created,
UI-built, and stored graphs remain ordinary `%Docket.Graph{}` values and
continue through the existing compiler and node-local runtime cache.

Hard requirement: every DSL macro expands to the exact same public editing and
schema APIs used by hand-written graph pipelines — one construction semantics,
two spellings. `use Docket.Graph` is an additive module frontend, not a second
graph representation. The macro tier only serves static definitions, which is
why Themes 3–5 still come first.

---

## Theme 7 — Subgraph composition

The headline "graph composability" item. Two distinct designs:

### 7a. Build-time inlining (proposed for v1.1)

```elixir
Docket.Graph.compose!(parent, "triage", child_graph,
  inputs: %{"message" => "customer_message"},   # child input <- parent field
  outputs: %{"result" => "triage_result"}        # child output -> parent field
)
```

A pure document transformation: namespace the child's nodes/fields/edges under
the prefix (`"triage/classify"`), rewire the child's `$start`/`$finish` to the
parent attachment points, map inputs/outputs to parent fields, and merge
diagnostics. Pros: zero runtime changes, the flat document stays the single
truth, the hash covers the composed whole, checkpoints/resume/interrupts all
work today. Cons: composition is by-value — editing the child later doesn't
update parents (that's host-side graph versioning, which v1 already assigns
to the host).

Design points: ID namespacing scheme (delimiter must be legal in IDs and
stable), guard/config references inside the child that name child field IDs
must be rewritten, collision diagnostics, and whether composed regions carry
provenance metadata (`metadata["$composed_from"]` is reserved-prefix
territory — needs a decision since `$` keys are reserved on the wire).

### 7b. Runtime subgraph node (defer, likely v2)

A `:subgraph` node type referencing a child graph by id+hash, executed as a
nested run. Requires nested checkpoint semantics, interrupt propagation,
resume across two run documents, and executor changes. Real value (independent
versioning, shared child instances, bounded document size) but it's a runtime
contract change, not an ergonomics slice. Record the design space now, build
after 7a proves insufficient.

---

## Theme 8 — Extension points: what else becomes a behaviour?

v1 has three behaviours, all at _effectful boundaries_: `Docket.Node` (do
work), `Docket.Checkpoint` (persist), `Docket.Executor` (dispatch). The
guiding split for opening more:

- **Effectful boundaries** (side effects, observability) are safe to open —
  they can't corrupt deterministic semantics.
- **Semantic pure functions** (reducers, guards, validation) are the
  determinism contract itself. Opening one hands the purity obligation to the
  host, and the module name becomes durable graph content (it's in the hash,
  and the graph only loads where that module exists). Open these sparingly,
  with loud contracts.

### Candidates, ranked

1. **`Docket.Reducer` behaviour** (semantic; pairs with Theme 1's stretch).
   `reduce(current, values, opts) :: value`, plus optional `init(opts)` (the
   zero value) and optional `write_schema(field_schema, opts)` (what a single
   write validates against, mirroring the built-in append-validates-item
   rule). Referenced from the document like node implementations
   (`%{type: :module, module: M}`). Must be pure — replan after crash must
   reproduce identical commits.
2. **`Docket.Guard` custom predicates** (semantic). A `:module` guard op with
   `evaluate(args, snapshot_view) :: boolean` over the committed snapshot
   only. Unlocks domain predicates the closed op set can't express
   (thresholds over nested data, cross-field conditions). Same
   purity-and-durability caveats as reducers; the read-only snapshot view is
   the enforcement surface.
3. **Event delivery** (effectful — safe). Today events exist only inside
   checkpoints; live UIs must parse checkpoint payloads. Two options:
   `:telemetry` emission (Elixir-native, zero new behaviour, recommended) or
   a `handle_event(event, context)` observer behaviour on the runtime config.
   Either is observability-only: delivery failures never affect the run.
4. **Retry/backoff strategy** (effectful-ish). Node retry _policies_ are data;
   the strategy interpreting them is fixed. A behaviour would allow jittered
   backoff or circuit-breaking. Timing doesn't touch committed-state
   determinism (task identity stays derived from plan), so this is safe but
   lower value — wait for a concrete host need.
5. **Schema validation** — recommend **keeping closed**. Validation runs on
   the commit path and defines graph portability; host-pluggable validators
   would make the same document valid on one node and invalid on another.
   Extend the engine itself (Theme 2) instead.
6. **ID generation** — already pluggable (`:id_generator` on
   `Docket.Graph.new/1`); nothing to do.

Suggested v1.1 scope: telemetry events (3) as a small standalone slice;
reducer behaviour (1) only if a real host graph exhausts the Theme 1
built-ins; guards (2) written up but deferred until a concrete predicate
can't be expressed with `path`/`equals`/`all`/`any`.

---

## Theme 9 — TenantFair follow-up work

The TenantFair claim policy is the single shipped PostgreSQL tenant scheduler.
Its sticky queued admission design is documented in
[`architecture/docket-tenant-fair.md`](architecture/docket-tenant-fair.md) and
its exact distributed-cap and fairness obligations in
[`architecture/docket-exact-cap-contract.md`](architecture/docket-exact-cap-contract.md):

```elixir
dispatcher: [concurrency: 100],
claim_policy: [
  implementation: Docket.Postgres.ClaimPolicy.TenantFair,
  default_max_active_runs: 4
]
```

The fixed design is:

- Claim selection is fair across eligible tenant partitions and FIFO-stable
  within each tenant.
- `max_active_runs` is an absolute ceiling enforced across every dispatcher sharing
  the database and prefix, not independently per BEAM node.
- Tenantless runs form their own partition, and Admin may set numeric per-tenant
  cap overrides.

Follow-up scope is limited to administration, observability, and operational
hardening of this path. Weighted service, preferred capacity, borrowing, TTL
slot expiry, and alternate admission modes are not parallel TenantFair designs.

This is deliberately separate from the two existing vehicle mechanisms:

- **Drain budgets provide run-level time slicing.** A vehicle may retain its
  claim for another superstep while both its moment and elapsed budgets remain.
  Once either budget is exhausted, the in-flight superstep finishes, and its
  commit atomically advances the run, releases its transient claim while
  retaining admission, and records an immediate wake. Nothing is preempted
  halfway through a superstep.
- **Finite attempt deadlines bound claim residency.** Runtime-owned activation
  processes are terminated at their effective timeout; fencing rejects stale
  results after crash recovery or steal.

Together, the partition limit prevents one tenant's collection of runs from
occupying the fleet, while the drain budget gives other admitted runs execution
opportunities without transferring sticky admission to later queued runs. This
is moderate fairness, not a promise of strict round robin, bounded queue
latency, weighted fair queuing, or equal resource usage: one superstep may
contain substantially more parallel node work than another.

### Storage and pooling constraints

- Vehicles continue to hold no database connection while node code runs.
  Partition accounting belongs in atomic claim/release/commit operations; it
  must not introduce a transaction spanning execution.
- Prefer claim-query/index changes over a row-locking coordinator or one queue
  per tenant. High-cardinality tenants must not create high-cardinality OTP
  processes, database queues, or telemetry labels.
- Measure claim latency, rows scanned, pool checkout time, and claim/commit
  throughput with many partitions, one hot partition, and mixed ready/expired
  claims.
- Any denormalized partition counters must be recoverable from authoritative
  admitted-run markers and remain correct across crash, expiry, steal, cancellation,
  terminal commit, and poison recovery. Avoid counters if the claim query can
  enforce the limit efficiently from indexed run rows.

### Required tests

- Multiple dispatchers cannot exceed a tenant's configured admission cap under
  concurrent claims.
- A tenant with thousands of eligible runs cannot block another tenant's first
  run from being claimed.
- Cooperative yields, claim expiry, steal, and vehicle crashes retain logical
  admission and allow the same run to reacquire ahead of later queued runs.
- External waits, future scheduling, terminal commits, poisoning, and
  interruption release admission exactly once so the FIFO head can be promoted
  without leaking or double-counting slots.
- Tenant partitioning changes scheduling only; existing tenant scope checks
  continue to prevent cross-tenant reads and mutations.

---

## Theme 10 — `{:await}` late-completion protocol for detached execution

v0.1.0 gives blocking node work two freshness strategies: keep each
between-commit stretch under the orphan TTL, or refresh the claim while work
runs under the finite runtime-owned attempt deadline. Work an external system
durably owns parks as an external wait instead. The remaining shape is work a
node started **in-process** but wants to hand back to the runtime — stop
holding a vehicle slot and claim while it finishes. The execution contract
reserves `{:await, term()}` for exactly this; v1 rejects it as permanent
failure. This theme promotes it to a specified protocol.

Design skeleton, established during the adversarial claim-freshness review:

- Detachment is voluntary. The runtime can never extract an await from opaque
  blocking code — any TTL-fired takeover is a timeout in disguise. The
  node/executor returns `{:await, token}`; the runtime dispatcher becomes
  partial-result-capable.
- A new detached `TaskState` status and wait kind make the mid-superstep park
  legal: the checkpoint records task identity only (task ID, attempt,
  idempotency key, input hash) — in-flight call state is uncheckpointable.
  The retry-park pending-writes machinery already proves the checkpoint shape.
- Result re-entry is a new serialized `RunMutation` through the row-locked
  signal path, fenced on the run still holding that exact task detached at
  that exact attempt; stale, duplicate, and superseded results are no-ops.
- `TaskState.deadline_at` becomes live with a detached-deadline sweeper.
  Every await carries a mandatory deadline: any bounded version of detachment
  contains a timeout as its own backstop, and a detached run must never sit
  `:waiting` with neither wake nor deadline.
- The completion needs a live home after the vehicle exits (node-local holder,
  unreplicated); holder crash resolves through deadline expiry.
- External effects stay replayable. A rejected late result's effects already
  happened; stable task/idempotency keys remain the only dedupe surface. This
  preserves the current boundary in
  [`delivery-guarantees.md`](delivery-guarantees.md): Docket makes no
  exactly-once external-effect promise.

Epic-sized (~15+ files: executor behaviour and both executors, runtime
dispatcher, TaskResult/TaskState/Moment, Loop/Algorithm, RunMutation,
Lifecycle, RunStore schedule + sweeper, Postgres dispatcher/vehicle, a holder
module, MemoryBackend, and both contract docs). This targets v1.1 after the
v0.1.0 line ships.

---

## Proposed slice order

Each slice is a branch per the v1 workflow; order reflects dependencies and
value-per-risk:

1. **schema-v1.1** — `:list`/`:boolean`/`:integer`, constraint enforcement,
   `open` objects. (Prereq for reducers; smallest risk.)
2. **reducers** — reducer contract extension (prior value + writes),
   `append`/`merge`/`sum`/`first_value`/`union`, reducer-aware write
   validation, compiler pairing diagnostics, interrupt-resume-through-reducer.
3. **schema-shorthand** — atom/tuple schema literals across all constructors.
4. **inline-fields** — `inputs:`/`fields:` options on `put_node!` that
   materialize graph fields, conflict rules, orphaned-field diagnostic.
5. **telemetry-events** — `:telemetry` emission for run/node/interrupt events
   (Theme 8.3); independent of everything above, can slot in anywhere.
6. **tenant-claim-fairness** — database-wide tenant-partitioned active-claim
   limits, fair partition selection, optional spare-capacity bursting, and
   contention/pool benchmarks (Theme 9; v0.1.1 operational follow-up).
7. **await-protocol** — `{:await, term()}` late-completion protocol for
   detached node execution (Theme 10, after the v0.1.0 line ships; it breaks
   down into its own slices when scheduled).

## Open questions (need a call before their slice starts)

1. `append` + list writes: concatenate, append-as-element, or compiler-flagged
   ambiguity? (Theme 1) **Concatenate**
2. Constraint enforcement: documented behavior change vs. policy-gated?
   (Theme 2) **document change**
3. Inline field conflict rule: is identical-definition-no-op /
   conflict-error the right balance, or should a `force: true` escape hatch
   exist? (Theme 5) **this is good for now**
4. Tenant claim enforcement: indexed live-run selection alone or recoverable
   denormalized partition counters? Decide from contention benchmarks rather
   than API preference. (Theme 9)
5. Burst semantics: how quickly must spare claims turn over once another
   tenant becomes eligible, without preempting an in-flight superstep?
   (Theme 9)
