# Docket PostgreSQL Backend

Status: implementation guide for the `0.1.0-dev` branch

The canonical boundary matrix for durable commits, replayable execution,
external effects, event export, and best-effort observers is
[Delivery and Execution Guarantees](../delivery-guarantees.md).

Docket's PostgreSQL backend stores published graphs, durable run state, and an
optional event history. It also provides the queue primitives needed to claim
and recover runs without a separate jobs table.

This guide describes the code that exists today. `Docket.Postgres` assembles
the migration-backed stores, claim dispatcher, LISTEN/NOTIFY wake notifier,
claimed-run vehicles, and retention pruner into one runnable backend bundle.
The host application owns and supervises its Ecto Repo.

## The model

The central idea is deliberately small:

> The run row is the queue.

Each row in `docket_runs` contains both the last committed graph state and the
operational fields that determine whether another process may advance it:

- `wake_at` says when an unclaimed running run is eligible.
- `claim_token` and `claimed_at` identify the current commit authority.
- `checkpoint_seq` fences graph-state commits.
- `claim_attempts` counts consecutive claims that reached execution without
  graph progress.
- `claim_abandons` counts consecutive claims handed back before execution
  because the claiming node could not run the graph.
- `poisoned_at` and `poison_reason` remove repeatedly orphaned or
  persistently unrunnable work from dispatch until an operator retries it.

This is similar in operational shape to Oban: install versioned database
objects, supervise a polling process, claim due work with bounded concurrency,
and recover work after process failure. Docket does not use Oban internally,
however, and it doesn't create a second job for a graph run. PostgreSQL is the
coordination layer and the run itself is the scheduled unit.

## Tables

Schema version 1 installs three tables with graph versions and their run
references scoped to an explicit tenant owner.

### `docket_graph_versions`

Published graphs are immutable and content addressed within an owner scope by
`{scope_key, graph_id, graph_hash}`. Publication materializes node
configuration defaults before hashing and stores the effective graph as
deterministic, versioned ETF.

A run always references the exact graph version it started from. The composite
foreign key uses `ON DELETE RESTRICT`, so a retained run cannot lose its graph.

### `docket_runs`

This is both the durable run aggregate and the scheduling relation. Frequently
queried operational facts are relational columns; the complete private
`Docket.Run` value is encoded in `state`.

The durable graph status is one of:

- `running` — ready, claimed, scheduled, or parked for retry
- `waiting` — requires an external mutation, such as interrupt resolution
- `done` — completed successfully
- `failed` — terminal graph failure
- `cancelled` — intentionally stopped

Scheduling and claiming are orthogonal to graph status. In particular, there
are no `ready`, `scheduled`, `executing`, or `retrying` status values. A running
row is exactly one of scheduled, claimed, or poisoned. Database checks enforce
that shape even for SQL issued outside Docket.

### `docket_events`

Events are append-only facts with an identity of `{run_id, seq}`. Event
sequence allocation happens in the runtime before persistence; the event store
does not derive identity with `MAX(seq)`.

Every committed runtime moment includes a metadata-only
`:checkpoint_committed` event after its runtime facts. Events cascade when a
run is deleted. `Docket.Postgres.Pruner` can delete persisted events earlier
under an explicit event-retention policy, but that retention cannot exceed the
run-retention period.

The same bounded pass deletes only terminal runs older than the configured run
retention. It then removes unreferenced graph versions older than the newest ten
publications for each owner scope and graph ID. Graph publication order is the
database-authored `inserted_at`, with graph hash breaking ties. Any referencing
run protects its exact scoped graph version regardless of age.

## Publication and start

Graph publication and run creation are separate boundaries:

```elixir
{:ok, graph_ref} = MyApp.Docket.save_graph(graph)
{:ok, run} = MyApp.Docket.start_run(graph_ref, input)
```

This example uses the default `tenant_mode: :none`. With
`tenant_mode: :required`, pass the same non-empty binary `tenant_id` to
`save_graph`, `start_run`, and every graph/run/event read or signal.

`save_graph/2` validates the graph, snapshots schema defaults, computes its
private hash, and stores the immutable effective document. Concurrent saves of
the same identity are idempotent; a conflicting document for the same identity
is rejected.

`start_run/3` fetches that exact version and compiles it against the node
modules installed locally. It never republishes the graph or adds defaults
introduced after publication. The initial run and its events commit in one
transaction through `Docket.Lifecycle`.

These facade functions work with any conforming bundle and are exercised
end-to-end with `backend: Docket.Postgres` against the revision-8 schema.

## Claiming and recovery

`Docket.Postgres.RunStore.claim_due/3` is the queue operation. In one database
transaction it:

1. considers due unclaimed rows and expired claims;
2. orders each path by its scheduling timestamp and stable row ID;
3. locks a bounded set with `FOR UPDATE SKIP LOCKED`;
4. increments `claim_attempts` and assigns fresh claim tokens; and
5. poisons rows that reached the configured attempt ceiling rather than
   returning them as leases.

`SKIP LOCKED` allows dispatchers on many BEAM nodes to poll concurrently
without a leader. A claim is a lease on commit authority, not an exactly-once
execution guarantee. If a worker pauses long enough for its claim to expire,
another worker may execute the same graph work. Execution is therefore
replayable: the same attempt may run more than once, while only the transaction
holding the current token and expected checkpoint sequence can win the durable
commit.

Every `:retain_claim` commit refreshes claimed time. Between commits, finite
runtime-owned node attempt deadlines bound executor callback residency and
the cooperative drain budget stops new moments at its boundary. The drain
limit must leave operational headroom below orphan TTL for termination,
moment construction, database checkout, commit, and release. Vehicles never
refresh claims while node work runs; the token/sequence fence remains the
sole commit authority after crash recovery or steal. Work that an external system owns end-to-end
should not hold a claim at all — it parks as an external wait and resumes
through the signal path.

The dispatcher keeps at most `concurrency` vehicles active on a node. Polling
is demand bounded and jittered so nodes don't continually wake in phase. A
failed vehicle launch releases its claim. Shutdown stops new polling and waits
for active vehicles up to the configured drain timeout.

`Docket.Postgres.Vehicle` is that execution piece. A drain loads the
committed run under the lease fence, compiles the stored effective graph
node-locally exactly once (optionally through `Docket.Postgres.GraphCache`,
which validates every read against the local module generation), and then
commits one runtime moment at a time through `Docket.Lifecycle`, exiting at
the first park. The bundle wires `Docket.Postgres.Vehicle.launcher/1` through
a dedicated `Task.Supervisor`. The execution subtree uses `:one_for_all`, so a
dispatcher restart also terminates vehicles it can no longer account for.
Notifier and pruner failures remain isolated from that execution subtree.

### Pre-execution abandon

Compiled graphs are node-local: a claimed vehicle fetches the stored
effective graph and compiles it against the node modules installed on that
node. During a rolling deployment a node can therefore hold a valid claim on
a run whose graph it cannot compile. That is a deployment-compatibility
condition, not node execution failure, and it gets its own disposition:
`Docket.Backend.RunStore.abandon_claim/5`.

An ordinary release would mishandle this. Claim acquisition already
incremented `claim_attempts`, and `release_claim` records an immediate wake —
so an incompatible node would re-claim instantly, burn attempts, and
eventually poison a run in which no node ever executed, misreporting a
deploy-window condition as execution failure.

Abandon is a single fenced update conditioned on the exact claim token and
the lease's committed `checkpoint_seq` (it can only apply before the
holder's first commit; a steal or signal commit makes it a no-op). A matched
abandon hands the acquisition increment back, counts the abandon, and
reschedules the run at the caller's jittered future `retry_at`, so a
compatible node picks it up after the backoff while `claim_attempts` still
counts only execution recoveries. Once `claim_abandons` reaches the
configured maximum, the abandon poisons the run with
`max_claim_abandons_exceeded` instead of rescheduling: a fleet that can
never run the graph (a failed deploy, or a corrupt stored document) becomes
an explicit, distinctly labeled operator concern rather than unbounded
claim churn. Both counters reset on any committed run mutation, and
`retry_poisoned_run` recovers either poison class.

The boundary the vehicle must respect: deterministic pre-execution
failure — the fetched document does not validate or compile against local
node contracts — abandons; transient infrastructure failure (for example
the graph fetch itself erroring) releases or crashes into claim expiry; and
anything after the first committed moment is ordinary execution failure. A
vehicle that crashes between compile failure and its abandon call simply
leaves the claim to expire, which consumes an attempt like any other crash.
Vehicles should also negative-cache incompatibility per
`{graph_id, graph_hash, local release}` so repeated claims of the same
doomed version do not re-fetch and re-compile it.

## Fenced commits

Runtime code proposes one `Docket.Runtime.Moment` at a time. A moment contains
the next run, assigned events, checkpoint metadata, and a disposition such as
continue, wait for external input, wake at a timestamp, or terminate.

`Docket.Lifecycle.commit_moment/5` commits the run and events together.
`Docket.Postgres.RunStore.commit/3` accepts the update only when both fences
match:

```text
stored claim_token   = vehicle claim_token
stored checkpoint_seq = expected checkpoint_seq
```

The commit also applies the moment's schedule:

| Disposition | Stored result |
| --- | --- |
| continue while claimed | retain the claim |
| immediate | release claim, set `wake_at` to now |
| external | release claim, clear `wake_at` |
| timestamp | release claim, set `wake_at` |
| terminal | release claim, clear `wake_at` |

After a successful graph-state commit, `claim_attempts` resets to zero.
Checkpoint observers run afterward and are best effort; they cannot veto or
roll back durable state.

## Signals and tenancy

Interrupt resolution and cancellation use a serialized row mutation rather
than dispatching a second job. The caller receives the committed result.

Every run and event operation has an explicit storage scope:

```text
:system                 trusted dispatch and recovery
:tenantless             only rows where tenant_id IS NULL
{:tenant, tenant_id}    only that tenant's rows
```

At the facade, `tenant_mode: :none` selects tenantless access and
`tenant_mode: :required` requires a non-empty `tenant_id`. Omitting a tenant
never becomes an unscoped read. Docket provides storage isolation, while the
host application remains responsible for deciding who may act for a tenant.

## Migrations

Applications own one Ecto migration that delegates to Docket, following the
same versioned-migration pattern used by libraries such as Oban:

```elixir
defmodule MyApp.Repo.Migrations.AddDocketTables do
  use Ecto.Migration

  def up, do: Docket.Postgres.Migration.up(version: 1)
  def down, do: Docket.Postgres.Migration.down(version: 1)
end
```

Generate it with:

```console
mix docket.gen.migration -r MyApp.Repo
mix ecto.migrate -r MyApp.Repo
```

For a PostgreSQL schema other than `public`, pass the same prefix to both
directions:

```elixir
def up, do: Docket.Postgres.Migration.up(version: 1, prefix: "automation")
def down, do: Docket.Postgres.Migration.down(version: 1, prefix: "automation")
```

The generator has no prefix switch: edit both functions in the generated
migration, then configure the runtime with the same `prefix: "automation"`:

```elixir
defmodule MyApp.Docket do
  use Docket,
    repo: MyApp.Repo,
    backend: Docket.Postgres,
    prefix: "automation",
    pruner: [
      interval_ms: :timer.hours(1),
      event_retention_ms: :timer.hours(24 * 30),
      run_retention_ms: :timer.hours(24 * 90),
      batch_size: 1_000
    ]
end
```

The migration records its installed version in a comment on `docket_runs` and
applies only missing steps. `Docket.Postgres` resolves the configured Repo and
optional prefix into the opaque backend context used by its stores.

## Operational inspection

The public durable read surfaces are designed around two views:

```elixir
{:ok, run} = MyApp.Docket.fetch_run(run_id)
{:ok, info} = MyApp.Docket.inspect_run(run_id)
```

These reads are tenantless. A required-tenant facade must receive the run's
non-empty binary `tenant_id` on both calls.

`fetch_run/2` returns the exact last committed `%Docket.Run{}`. Claim and
release operations don't rewrite `run.updated_at`.

`inspect_run/2` returns `%Docket.RunInfo{}` with the run plus `wake_at`,
`claimed_at`, `claim_attempts`, `claim_abandons`, and poison facts. Claim
tokens are deliberately excluded. A sustained abandon count is the durable
signal that the current deployment cannot yet execute a run. `await_run/2` polls this projection until the run waits, terminates,
becomes poisoned, or reaches its required timeout.

Use the facade for application operations. The store modules remain internal
capabilities and focused backend-development test surfaces.

## Backend configuration

The bundle requires `repo:` and an explicit `pruner:` policy containing
`interval_ms`, `event_retention_ms`, `run_retention_ms`, and `batch_size`.
Event retention cannot exceed run retention. `notifier: :none` omits the
LISTEN connection for poll-only deployments. Dispatcher and vehicle policies
may be supplied through `dispatcher:` and `vehicle:` keyword lists; the store
capabilities themselves are fixed and cannot be mixed independently.

The old host-owned supervised `run`/`resume`/`get_run` path is absent in
v0.1.0. A configured backend is the only supervised production lifecycle.

## Design boundaries

- Docket schedules graph runs, not arbitrary jobs. Run Oban beside Docket when
  the application also needs a general-purpose job queue.
- Node attempts are replayable around claim loss; this describes duplicate
  risk, not an unconditional promise that every effect eventually occurs. Use
  Docket's stable task/idempotency identity in integrations that require
  deduplication, and apply it atomically with the external effect.
- Compiled graphs are local and ephemeral. Deployments must keep retained run
  state compatible with installed node modules or version those modules.
- The opaque ETF columns are private storage. Applications should store run
  IDs and their own projections, not decode or duplicate Docket state.
