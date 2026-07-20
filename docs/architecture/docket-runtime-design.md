# Runtime Design Rationale And Background

Status: historical rationale with a current architecture summary; not an API spec
Date: 2026-06-25

Current APIs and exact types are authoritative in module docs. Production
configuration and recovery are documented in the
[PostgreSQL operations guide](../postgres-operations.md); durability boundaries
are documented in [delivery guarantees](../delivery-guarantees.md).

## Runtime Model

Docket executes cyclic, parallel, stateful graphs as logical actors connected by
channels. Actors read the last committed state, emit field writes, and advance
in bulk-synchronous supersteps. Updates become visible only after the superstep
commit barrier.

The model supports:

- fan-out and fan-in
- cycles bounded by max-supersteps policy
- deterministic reducers over concurrent writes
- guarded edge activation
- retries and finite attempt timeouts
- durable interrupts and external signals
- crash recovery from committed run state

Graph vertices are logical actors, not OTP processes. Concurrency and fault
containment use supervised tasks and backend execution vehicles without making
process identity part of graph semantics.

## Research Influences

### Pregel And LangGraph

Pregel supplied the vertex-centric superstep model: plan from prior messages,
compute locally, publish messages at a barrier, and terminate when no actor or
message remains active. LangGraph demonstrated how a public graph API can lower
to actors, channels, reducers, guards, and repeated Plan/Execute/Update phases.

Docket adopts bulk-synchronous visibility and channel-version activation. It
does not adopt LangChain types or expose a second public Pregel graph model.

Sources:

- https://research.google/pubs/pregel-a-system-for-large-scale-graph-processing/
- https://docs.langchain.com/oss/python/langgraph/pregel

### Apache Beam And Flink

Beam separates graph definition from execution backend and makes data
collection semantics explicit. Flink treats checkpoints as coordinated state
boundaries. Docket adopts the separation between public graph, compiled graph,
and execution vehicle plus an explicit durable commit boundary.

Windowing, watermarks, event-time triggers, and general stream processing are
outside the current runtime.

Sources:

- https://beam.apache.org/documentation/basics/
- https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/stateful-stream-processing/

### Temporal

Temporal separates durable workflow decisions from replayable external
activities. Docket similarly assigns stable attempt identity and requires
idempotency for external effects. It persists committed run state and retained
events rather than replaying deterministic user workflow code from the
beginning.

Source: https://docs.temporal.io/workflow-execution

### Timely Dataflow And OTP

Timely Dataflow motivated explicit logical progress and the possibility of
richer frontiers. OTP supplies supervision, isolated tasks, and process failure
semantics. Docket currently uses integer supersteps and durable wake state rather
than a general timestamp lattice or distributed Erlang membership.

Sources:

- https://timelydataflow.github.io/timely-dataflow/
- https://hexdocs.pm/elixir/Supervisor.html

## Current Architecture

### Public Documents

`Docket.Graph` is the editable public graph definition. Publishing materializes
node configuration defaults and stores an immutable effective graph version in
the configured backend. `Docket.GraphRef` identifies that exact content.

`Docket.Run` is the durable execution-state document encoded by the backend and
returned through committed reads. `Docket.Event` records retained ordered facts
about run transitions.

### Internal Values

`Docket.Runtime.Graph` is the compiler-produced executable graph. It contains
runtime nodes, state and activation channels, edge descriptors, output
projections, policies, and lowering metadata.

`Docket.Runtime.Moment` is one substrate-neutral proposed transition. It carries
the proposed run, assigned events, checkpoint metadata, pending attempt writes,
and the post-commit disposition.

`Docket.Runtime.Loop` and `Docket.Runtime.Algorithm` calculate moments without
writing storage or delivering observers.

### Runtime Instance And Vehicles

One `Docket.Runtime.Supervisor` tree represents a named Docket instance. It owns
the required backend bundle, immutable instance configuration, and task
supervision used by execution and after-commit observers.

The PostgreSQL backend schedules and claims due runs. An ephemeral vehicle loads
the run's pinned effective graph, obtains or builds the compiled runtime graph,
drives moments within its cooperative drain budget, and commits each accepted
moment under claim/checkpoint fences.

There is no resident process per run, public runtime registry, or public
run/resume lifecycle in v0.1.0.

## Superstep Semantics

```text
Plan
  read committed channels and activation versions
  select runnable nodes

Execute
  build stable attempt identities
  dispatch selected nodes
  buffer normalized results

Update
  validate writes
  apply reducers in deterministic writer order
  evaluate edge guards and barriers
  determine terminal, external, timed, or immediate disposition

Commit
  atomically store the proposed run and retained events
  schedule the next wake or terminal park
  deliver best-effort observers and telemetry after commit
```

No node observes another node's writes from the same superstep. A permanent
attempt failure prevents every buffered write in that superstep from committing.

## Channels And Reducers

Public inputs and state fields lower to last-value storage channels. The field's
`Docket.Reducer` controls how a step's writes update the committed value.
Generated edge channels are ephemeral or barrier channels and are not writable
by user node code.

Reducers fold the previous committed value with writes sorted by public writer
node ID. This makes concurrent updates deterministic. Supported reducer
semantics are documented in `Docket.Reducer` and
[reducer design](docket-reducers-design.md).

## Durability And Effects

An accepted transition and its retained events commit atomically. Claim tokens
and checkpoint sequences fence stale vehicles. Recovery starts from the latest
committed run and may re-execute work whose result did not commit.

Node attempts are therefore at-least-once. Stable idempotency keys let external
systems deduplicate cooperating effects; Docket cannot make an arbitrary effect
exactly once. Observers, notifications, and telemetry are after-commit,
best-effort projections rather than durable delivery mechanisms.

## Interrupts And Scheduling

Interrupts park a run when no other work can proceed. Resolution is a named
durable signal that validates and commits a resume-field write before scheduling
the next wake.

Retry deadlines produce timed parks. Cooperative vehicle drain limits produce
immediate handoff parks. Terminal runs never wake again. The backend owns these
schedule effects; the runtime core returns only the disposition needed to derive
them.

## Historical 0.0.1 Boundary

The original runtime used one GenServer per active run, a runtime registry,
public `run`/`resume`/live-`get_run` functions, and host-owned checkpoint
persistence. Process startup and callback acceptance were part of the public
lifecycle.

That design was removed because durable ownership, recovery, and scheduling
needed one backend transaction and fencing boundary. The processless loop,
superstep algorithm, executor boundary, guards, reducers, interrupts, and inline
test shell survived. The resident process, registry, host checkpoint committer,
and host-persisted run document did not.

See [historical graph execution contract](docket-graph-execution-contract-design.md)
for the retained execution rationale.

## Deliberately Separate Concerns

Applications own authorization, product relationships, UI projections, and
external effect implementations. The backend owns graph/run/event persistence,
claim fencing, scheduling, and recovery. Docket core owns graph compilation and
pure execution semantics.

Planned features and research topics live in the
[future roadmap](../future-roadmap.md), not in this historical rationale.
