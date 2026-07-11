# Docket v1.1 Roadmap — Composability & Ergonomics

Status: slices 1–5 implemented (PR #6, 2026-07-05): schema-v1.1, reducers,
schema-shorthand, inline-fields, telemetry-events. The reducer contract
rationale moved to `docs/architecture/docket-reducers-design.md`; API truth
lives in module docs. Themes 6 (graph module DSL) and 7 (subgraph
composition) remain open design space, recorded below.

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
  reducer, so answering into an `append` messages field accumulates naturally.
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

## Theme 6 — Graph module DSL (macro tier)

A `use Docket.Graph.DSL` frontend for defining graphs in modules:

```elixir
defmodule MyApp.Graphs.SupportReply do
  use Docket.Graph.DSL, id: "support-reply"

  input :customer_message, :string, required: true
  field :messages, {:list, :map}, reducer: :append
  node :draft, MyApp.Nodes.LLM, config: %{...}
  chain [:start, :draft, :finish]
  output :draft_response
end

MyApp.Graphs.SupportReply.graph()  #=> %Docket.Graph{}
```

Hard requirement if built: the macros expand to the exact same editing-API
calls — one construction semantics, two spellings. Risks: a second surface to
keep in lockstep, and it only serves compile-time graphs (UI-built graphs
can't use it), which is why Themes 3–5 come first. Decide after living with
the improved pipeline API; this may turn out unnecessary.

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

## Open questions (need a call before their slice starts)

1. `append` + list writes: concatenate, append-as-element, or compiler-flagged
   ambiguity? (Theme 1) **Concatenate**
2. Constraint enforcement: documented behavior change vs. policy-gated?
   (Theme 2) **document change**
3. Inline field conflict rule: is identical-definition-no-op /
   conflict-error the right balance, or should a `force: true` escape hatch
   exist? (Theme 5) **this is good for now**
