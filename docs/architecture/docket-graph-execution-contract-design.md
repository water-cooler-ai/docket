# Historical Graph Execution Contract

Status: historical 0.0.1 design rationale; not a current API contract
Date: 2026-06-25

The 0.0.1 release used one resident process per active run, public
`run`/`resume`/live-`get_run` calls, and a host checkpoint callback that could
accept or reject an in-memory transition. Those lifecycle and persistence APIs
were removed for v0.1.0.

Current production behavior is documented by the `Docket` and
`Docket.Backend` module docs, the
[PostgreSQL operations guide](../postgres-operations.md), and
[delivery guarantees](../delivery-guarantees.md). The current facade publishes
graphs with `save_graph`, starts a stored `Docket.GraphRef` with `start_run`,
reads committed state from the backend, and sends named durable signals.

## Retained Execution Model

The runtime uses bulk-synchronous supersteps:

```text
Plan -> Execute -> Update -> Commit
```

- Plan sees only the previously committed channel state.
- Execute may run selected node attempts concurrently.
- Node results remain buffered until every selected attempt reaches a terminal
  result for that superstep.
- Update validates writes, applies reducers, evaluates outgoing edge guards,
  and determines the next activation frontier.
- Commit makes the proposed run and retained events durable together.

Writes from one superstep are invisible to other nodes until the next
superstep. Permanent failure prevents all writes from that superstep from
committing. This barrier is the basis for deterministic replay and stable
idempotency keys.

## Processless Runtime Core

`Docket.Runtime.Loop` proposes initialization and advancement moments from a
`Docket.Runtime.Graph` and committed `Docket.Run`. `Docket.Runtime.Algorithm`
contains planning, state snapshots, guard evaluation, write validation, reducer
application, edge activation, and output projection.

An accepted transition is represented by `Docket.Runtime.Moment`. Calculating a
moment performs no storage write, observer delivery, or telemetry emission.
Durable backend drivers commit it inside their transaction; `Docket.Test`
accepts the same value in the calling process for deterministic tests.

This separation survived the removal of the resident per-run GenServer. There
is no current `Docket.Runtime` process module or `Docket.Runtime.Registry`.
`Docket.Runtime.Supervisor` owns one named runtime instance, its backend bundle,
instance configuration, and task supervision. Backend vehicles drive claimed
runs.

## Node Execution

Public node modules implement `Docket.Node.call/3`:

```elixir
call(state, config, context)
  :: {:ok, map()}
   | {:interrupt, Docket.Interrupt.t()}
   | {:await, term()}
   | {:error, term()}
```

The configured `Docket.Executor` receives an internal activation and runtime
node definition. It normalizes callback success, interrupts, errors,
exceptions, exits, throws, and timeouts into runtime task results.

`:await` is reserved and is treated as a permanent node failure by the current
local execution contract. Queue, remote-completion, and replay-only executor
protocols are not part of v0.1.0.

Node writes are keyed by public graph field ID. Runtime validation rejects
unknown fields and invalid write values before reducers run. The executor does
not mutate `Docket.Run`, apply reducers, evaluate guards, or commit moments.

## Guards And Activations

Guards are durable `Docket.Guard` expressions rather than function captures.
The runtime evaluates them only for edge candidates produced by successful
source completion or a satisfied multi-source barrier.

Every public edge lowers to a generated activation channel. Guard-false edges
emit no activation. List-form sources use a barrier channel and activate only
after every source has completed since the previous firing.

## Failure And Retry

Node errors, exceptions, exits, throws, timeouts, invalid outputs, and guard
evaluation failures are attempt failures. Node retry policy determines whether
another attempt is scheduled. Exhaustion makes the superstep fail without
committing its buffered writes.

Attempt identity derives from committed run state. Re-executing an uncommitted
superstep after a crash uses stable idempotency keys. External effects are still
at-least-once and require a cooperating application idempotency scheme.

## Interrupts

An interrupt records an ID, public node ID, resume field, schema, and payload.
The run parks when no other work can proceed. `resolve_interrupt` validates the
open interrupt and resolution value, proposes the resume-field write, and
schedules the next durable wake. The backend commits the signal before it is
reported as accepted.

Authorization and tenant ownership remain application responsibilities around
the tenant-scoped durable facade.

## Commitment Boundary

The old checkpoint callback was a persistence gate. The current boundary is a
backend transaction protected by claim and checkpoint fences:

- the proposed `Docket.Run` and retained events commit atomically
- a lost claim or stale checkpoint sequence cannot commit
- observers, notifications, and telemetry run after commit and are best effort
- observer failure cannot roll back durable state

The committed checkpoint returned to tests or delivered to observers is a
read-only description of an already-accepted moment, not an opportunity to veto
it.

## Testing Boundary

`Docket.Test.run_inline/3` and `Docket.Test.step_inline/2` drive the same loop and
moment representation used by durable vehicles. Ordinary graph semantics tests
therefore require no resident runtime process, backend, or scheduling sleeps.

Backend-specific tests cover transactions, claim fencing, recovery, scheduling,
and after-commit delivery. Executor tests cover process and timeout behavior.
