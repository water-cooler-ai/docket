# Migrating backends from Docket 0.1.1 to 0.1.2

Docket 0.1.2 replaces lifecycle use of callback transactions with the
versioned `Docket.Backend.TransitionStore` contract. Applications using
`Docket.Postgres` need no configuration change. Third-party backend authors can
upgrade during the 0.1.x compatibility window described below.

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
unclaimed commit. A conflict causes a bounded refetch and re-evaluation.
Mutation functions may therefore run more than once and must be deterministic,
bounded, and free of external side effects. A no-change or error decision does
not invoke storage and publishes nothing.

## Declaring transition support

An upgraded backend implements `Docket.Backend.TransitionStore`, returns it
from `transitions/0`, and declares contract version 2:

```elixir
@impl Docket.Backend
def capabilities do
  %{
    contract_version: 2,
    transitions: %{
      version: Docket.Backend.TransitionStore.version(),
      limits: Docket.Backend.TransitionStore.portable_limits(),
      replay: :durable_receipts,
      durability: :documented_backend_policy
    }
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
source compatibility but cannot strengthen a legacy backend's replay,
durability, size-limit, or topology guarantees.

## Deprecated 0.1.x APIs

The following lifecycle composition APIs remain available in 0.1.2 only for
the compatibility adapter and focused backend-internal use:

- `Docket.Backend.transaction/2`;
- `Docket.Backend.RunStore.insert_run/5`;
- `Docket.Backend.RunStore.commit/3`;
- `Docket.Backend.RunStore.mutate_run/4`;
- `Docket.Backend.EventStore.append_events/4`;
- `Docket.Backend.commit_transition/4`.

They are scheduled for removal from the public backend contract in 0.2.
Focused graph/run/event reads and claim operations remain.

## Required semantics

Validate the complete proposal and configured limits before writing. Wrong
tenant and unknown resources both return `:not_found`. Immutable identity is
validated before claim/checkpoint fences. Event equality compares complete
canonical event content.

Every logical transition has a stable `transition_id`. Exact replay after an
ambiguous outcome returns success; reuse with different proposal/event content
returns `:conflict`. If events may be pruned, use a durable receipt independent
of retained events.

The portable permanent errors are `:not_found`, `:invalid_transition`,
`:conflict`, `:event_conflict`, and `:too_large`. Retryable infrastructure
failures use `{:retryable, reason}`. Core does not automatically retry an
ambiguous infrastructure result; retry the exact transition only when the
backend can distinguish replay from a stale fence.

Each backend must also document acknowledged durability and topology
constraints. Redis Cluster implementations must co-slot every key touched by a
transition or reject clustered mode. SQLite implementations must document busy,
journal, synchronous, and reopen behavior. DynamoDB implementations must
normalize transaction cancellation and throttling while respecting item and
transaction limits.
