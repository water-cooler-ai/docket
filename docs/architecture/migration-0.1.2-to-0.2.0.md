# Migrating backends from Docket 0.1.2 to 0.2.0

Docket 0.2.0 removes the deprecated 0.1.x lifecycle composition APIs and makes
`Docket.Backend.TransitionStore` mandatory. There is no database schema
change: 0.2.0 runs on the same schema version 2 as every 0.1.x release, so no
migration needs to be generated or run. Applications that configure
`Docket.Postgres` and write through the public `Docket` facade need no
changes beyond the telemetry rename below. Third-party backends that already
declare contract version 2 keep their declaration and transition store
unchanged, but must delete their implementations of the removed callbacks
listed below: 0.1.2 still required those callbacks, and `@impl` attributes on
them now produce compile warnings against the slimmed behaviours.

## Removed APIs

The 0.1.x compatibility window is over. The following are removed from the
public backend contract:

- `Docket.Backend.transaction/2`;
- `Docket.Backend.commit_transition/4`;
- `Docket.Backend.RunStore.insert_run/5`;
- `Docket.Backend.RunStore.commit/3`;
- `Docket.Backend.RunStore.mutate_run/4`;
- `Docket.Backend.EventStore.append_events/4`;
- `Docket.Backend.LegacyTransitionStore`, the adapter that routed undeclared
  0.1.x backends through the composed write path;
- the public types the removed callbacks carried:
  `Docket.Backend.transaction_fun/0`, `Docket.Backend.transaction_result/0`,
  and `Docket.Backend.RunStore.commit_proposal/0`, `mutation/0`,
  `mutation_decision/0`, `mutation_result/0`, and `checkpoint_type/0`. The
  transition `schedule` type is now defined on
  `Docket.Backend.TransitionStore`;
- the deprecated delegates on the bundled PostgreSQL backend:
  `Docket.Postgres.transaction/2`, `Docket.Postgres.commit_transition/4`,
  `Docket.Postgres.RunStore.insert_run/5`, `commit/3`, `mutate_run/4`, and
  `Docket.Postgres.EventStore.append_events/4`.

Focused graph, run, and event reads and the claim operations remain part of
their contracts unchanged. Backend-native transactions remain private
implementation details; they simply never cross the public contract.

## Renamed telemetry

The lifecycle write span is
`[:docket, :lifecycle, :transition, :start | :stop | :exception]`, renamed
from `[:docket, :lifecycle, :transaction, ...]` now that transactions are no
longer part of the public contract. Measurements and metadata are unchanged.
Reattach any handler or reporter subscribed to the old event name.

## Mandatory declaration

`capabilities/0` and `transitions/0` are now required `Docket.Backend`
callbacks. Contract negotiation accepts exactly one shape:

```elixir
@impl Docket.Backend
def capabilities do
  %{
    contract_version: 2,
    transitions: %{version: Docket.Backend.TransitionStore.version()}
  }
end

@impl Docket.Backend
def transitions, do: MyBackend.TransitionStore
```

Startup rejects everything else with a specific error: a backend that does
not export `capabilities/0`, a backend that declares contract version 1, a
version-2 declaration without `transitions/0`, and a transition store missing
any `Docket.Backend.TransitionStore` callback.

## Upgrading a 0.1.x backend

A backend still relying on the removed composition path implements the three
semantic operations directly against its native atomic primitive:

1. `initialize/4` — atomically create the initial run, its schedule and
   supporting state, and the assigned events;
2. `commit_claimed/4` — atomically publish one claim-token and
   checkpoint-sequence fenced transition and its events;
3. `commit_unclaimed/5` — atomically publish one signal/admin transition under
   an optimistic checkpoint fence, without a claim.

The required semantics are unchanged from 0.1.2 and documented on
`Docket.Backend.TransitionStore`: validate the complete proposal before
writing, conceal wrong-tenant and unknown resources as `:not_found`, validate
immutable identity before fences, treat events as idempotent by canonical
content at `{run_id, seq}`, and publish nothing on failure. The shared
conformance suite under `test/support/backend_tests/` exercises the full
matrix and runs against any backend bundle.
