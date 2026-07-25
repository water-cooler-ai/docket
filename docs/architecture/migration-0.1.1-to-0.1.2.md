# Migrating backends from Docket 0.1.1 to 0.1.2

Docket 0.1.2 replaces lifecycle use of callback transactions with the
versioned `Docket.Backend.TransitionStore` contract. There is no database
schema change: 0.1.2 runs on the same schema version 2 as 0.1.0 and 0.1.1.
Applications using `Docket.Postgres` need no changes. Third-party backend
authors can upgrade during the 0.1.x compatibility window described below.

## What changed

Core now resolves one transition capability and calls:

1. `initialize/4` for the initial run, schedule/supporting state, and events;
2. `commit_claimed/4` for a claim-token and checkpoint-sequence fenced moment;
3. `commit_unclaimed/5` for a signal/admin mutation fenced only by the expected
   checkpoint sequence.

Each proposal contains data only. It never contains a transaction handle,
callback, or `Docket.Runtime.Moment`. Backend-native transactions remain
private implementation details.

Signals now perform scoped fetch, pure mutation evaluation, and an optimistic
unclaimed commit. `:stale_checkpoint` causes a bounded refetch and
re-evaluation; other errors do not. Mutation functions may therefore run more
than once and must be deterministic, bounded, and free of external side
effects. A no-change or error decision does not invoke storage and publishes
nothing.

## Declaring transition support

An upgraded backend implements `Docket.Backend.TransitionStore`, returns it
from `transitions/0`, and declares contract version 2:

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

Startup validates all three transition callbacks. A version-2 declaration with
a missing accessor or incomplete store is rejected; Docket never silently
downgrades a partially upgraded backend.

Backends that omit `capabilities/0` are treated as legacy contract version 1.
Core routes their lifecycle operations through
`Docket.Backend.LegacyTransitionStore`, which composes the existing
`transaction/2`, run-store, and event-store writes. That adapter preserves
source compatibility; it cannot fuse a legacy backend's writes into one
round trip.

## Deprecated 0.1.x APIs

The following lifecycle composition APIs remain available in 0.1.2 only for
the compatibility adapter and focused backend-internal use:

- `c:Docket.Backend.transaction/2`;
- `c:Docket.Backend.RunStore.insert_run/5`;
- `c:Docket.Backend.RunStore.commit/3`;
- `c:Docket.Backend.RunStore.mutate_run/4`;
- `c:Docket.Backend.EventStore.append_events/4`;
- `c:Docket.Backend.commit_transition/4`.

Version-2 backends retain these callbacks throughout the 0.1.x compatibility
window because the behavior and legacy adapter still require them. They are
scheduled for removal from the public backend contract in 0.2.
Focused graph/run/event reads and claim operations remain.

## Required semantics

Validate the complete proposal before writing. Wrong tenant and unknown
resources both return `:not_found`. Immutable identity is validated before
claim/checkpoint fences. Events are idempotent by canonical content at
`{run_id, seq}`: a pre-existing identical event is accepted and different
content at a stored sequence returns `:event_conflict`. A failed operation
publishes no run, schedule, support, or event changes.

The portable permanent errors are `:not_found`, `:invalid_transition`,
`:conflict`, and `:event_conflict`. A lost claim or checkpoint fence is
`:stale_checkpoint`. Retryable infrastructure failures use
`{:retryable, reason}` and non-retryable infrastructure failures use
`{:permanent, reason}`. Core never automatically retries an ambiguous
infrastructure result; recovery flows through the claim machinery exactly as
it did through the composed 0.1.x write path.
