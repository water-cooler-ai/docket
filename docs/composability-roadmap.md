# Docket Composability and Ergonomics Roadmap

Docket 0.1.0 includes the schema, reducer, graph-editing, and telemetry work
summarized below. The remaining sections describe open work only. Module
documentation defines the implemented API.

## Implemented outcomes

### Schema and reducer support

- Schemas support strings, floats, integers, booleans, maps, objects, enums,
  and lists.
- Numeric, string, and list constraints are enforced. Objects may opt into
  unknown keys with `open: true`.
- Schema shorthand accepts primitive atoms and type tuples throughout graph
  construction.
- Reducers fold the prior committed value with deterministically ordered writes.
  Built-ins are `last_value`, `first_value`, `append`, `merge`, `sum`, and
  `union`.
- Accumulating reducers provide natural initial values and reducer-aware write
  validation. List writes to `append` and `union` concatenate their elements.

See `Docket.Schema`, `Docket.Reducer`, and the
[reducer rationale](architecture/docket-reducers-design.md).

### Inline graph fields

`Docket.Graph.put_node/4` and `put_node!/4` accept `inputs:` and `fields:`
declarations that materialize ordinary graph fields. Identical declarations are
idempotent, conflicting declarations fail, and deleting a node does not delete
shared fields.

### Event telemetry

Every committed `Docket.Event` with a public telemetry mapping emits after the
commit. This gives live instrumentation a direct event stream without requiring
checkpoint parsing. Delivery remains best effort; retained events are the
durable source.

See `Docket.Telemetry` and the [telemetry guide](telemetry.md).

### Claim policies

The WindowedInterleave claim policy provides statistical cross-tenant fairness
with sticky in-flight cohorts under its own admission mode; Legacy remains the
tenant-blind baseline. Hard per-tenant caps, weighted service, borrowing,
reclaim, and alternate schedulers are separate proposals in the
[future roadmap](future-roadmap.md).

## Open work

### Graph modules and a static DSL

A static graph module could expose a declarative frontend while preserving the
existing graph document as the only canonical representation:

```elixir
defmodule MyApp.Graphs.SupportReply do
  use Docket.Graph, id: "support-reply"

  input :customer_message, :string, required: true
  field :messages, {:list, :map}, reducer: :append
  node :draft, MyApp.Nodes.LLM, config: %{}
  chain [:start, :draft, :finish]
  output :draft_response
end
```

An implementation would need to:

- expand through the public graph-editing and schema APIs;
- preserve an authored `%Docket.Graph{}` for publication and serialization;
- validate static definitions at compile time; and
- establish compile dependencies on referenced node modules so schema changes
  recompile dependent graph modules.

Runtime-created, UI-built, and stored graphs would remain ordinary graph
documents using the existing compiler and runtime cache.

### Subgraph composition

The first candidate is build-time inlining:

```elixir
Docket.Graph.compose!(parent, "triage", child_graph,
  inputs: %{"message" => "customer_message"},
  outputs: %{"result" => "triage_result"}
)
```

Inlining would namespace child nodes, fields, and edges; map its inputs and
outputs to parent fields; and produce one flat graph document covered by one
hash. The design still needs stable namespacing, collision diagnostics,
rewriting rules for field references in guards and configuration, and a decision
on provenance metadata.

A runtime subgraph node referencing an independently versioned child graph is
deferred. It would require nested run, checkpoint, interrupt, and recovery
semantics rather than only a graph-document transformation.

### Detached node completion

Vehicles do not refresh claims. In-process node attempts run under the
runtime-owned hard deadline, and token/sequence fencing rejects a stale commit
after claim recovery. Work durably owned by an external system should park the
run and return through an interrupt or another serialized signal.

`{:await, term()}` is reserved by the executor contract and currently becomes a
permanent node failure. Supporting detached completion would require:

- a durable task state that records stable task and attempt identity without
  trying to checkpoint an in-flight process;
- a serialized result mutation fenced on that exact detached task and attempt;
- a mandatory durable deadline and recovery path;
- a supervised home for local completions after the vehicle exits; and
- unchanged replay and external-effect idempotency rules for late or rejected
  results.

The completion protocol must release the vehicle and claim while ensuring that
stale, duplicate, and superseded results cannot advance the run.

### Additional extension points

Custom reducer or guard behaviours remain possible, but they would make host
modules part of durable graph semantics and move determinism obligations to the
host. They should be added only for requirements that the built-in reducers and
guard expressions cannot represent. Retry/backoff strategy customization is
also deferred pending a concrete integration need.
