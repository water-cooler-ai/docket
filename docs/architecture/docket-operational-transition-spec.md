# Docket Operational Transition Spec

Status: transition spec (rev 8, amended 2026-07-10 — backend bundle and lifecycle owner, mandatory backend-owned production lifecycle, five durable graph statuses, explicit poison/failure facts, transactional wake hints, durable retry state, retained-run graph integrity, bounded claim selection, desynchronized polling)
Date: 2026-07-09
Amended: 2026-07-10 (`backend:` public configuration name; backend-only production lifecycle)
Target: move from core runtime library to an Oban-shaped durable runtime

## Lock Amendment 1 — Effective Graphs, Node-Local Compilation

An ABI-pinned distributed-artifact design was considered and rejected before
it became part of the locked spec. Durable runs and graph references pin only
`{graph_id, graph_hash}`. The hash identifies the canonical **effective**
graph document: publication snapshots node `config_schema/0` contracts once,
materializes omitted defaults into node configuration, canonicalizes again,
then hashes, validates, compiles, and stores that document.

Later local compilation treats the stored document as already effective: it
validates against the node contracts installed on that node but never injects
newly introduced schema defaults. A new default affects future publications
and therefore produces a new graph hash; it cannot change a retained version.

Compiled `%Docket.Runtime.Graph{}` values are ephemeral node-local state. A
start, signal operation, recovery, or claimed vehicle fetches the exact
canonical document and compiles it with the Docket and application code
installed on that node. A claimed vehicle compiles once, then reuses that
runtime graph while draining multiple supersteps under its claim. An optional
local cache may avoid later compilation on the same release, but cache loss
affects performance only and its key must include both graph identity and a
local release/compiler generation.

`max_supersteps` is optional safety configuration, not a validity condition.
Cyclic graphs without a limit are publishable and may run indefinitely. A
limit declared in graph policy is part of the canonical document and therefore
changes graph identity; a host/runtime limit remains external operating policy
and does not change the graph hash.

There is no compiler ABI in `Docket.GraphRef`, `Docket.Run`, leases, claim
routing, scheduling indexes, or relational identity, and no distributed
compiled-artifact table. Compiler ABI could select a lowered plan but could
not freeze the referenced application modules: a module atom still resolves
to whatever code is installed on the executing node. An honest historical
execution guarantee would require a future execution-package identity plus
version-routed application releases, not an artifact ABI alone.

A run may therefore cross compatible application releases at claim/yield
boundaries. Whole-node replacement or graceful vehicle drain is the expected
deployment model; hot replacement of node modules while a vehicle is running
is host-owned and cannot be frozen by graph compilation. Applications must
keep retained checkpoints compatible, migrate them explicitly, or use
versioned node modules when behavior must remain stable.

Compilation incompatibility is operational, not graph-node execution failure,
but its claim disposition is intentionally unresolved in this amendment.
DCKT-35 must choose preflight, token-fenced pre-launch abandon, administrative
halt, or another invariant-safe design before DCKT-20 implements the vehicle.
Normal `release_claim` is not an answer by itself because acquisition already
incremented attempts and repeated release/reclaim eventually poisons the run.

Why the lock changed: a distributed artifact was derived using one node's
application contracts but executed against potentially different module code
on another node, creating a stronger-looking guarantee than Docket could
enforce. Node-local compilation makes the actual deployment boundary explicit
while durable effective configuration keeps omitted defaults content-addressed.
DCKT-34 owns this no-code lock amendment; DCKT-35 owns the remaining
compilation-failure decision.

## 1. Purpose

This document defines the transition from the current Docket package shape to
the first operational, distributable Docket release.

The guiding product posture is Oban-like — Oban-like in *shape*, not built on
Oban:

```text
Users define work.
Users submit work.
The package owns durable lifecycle, claims, retries, recovery, telemetry,
pruning, and operational tables.
```

For Docket, that becomes:

```text
Users define graph nodes.
Users publish graphs and start or signal runs.
Docket owns durable graph-run execution.
```

This is a package-shape and operational-contract decision. It changes how the
core durable-state invariant is enforced:

```text
One active graph run has one current commit authority, and one durable mutation
wins each commit boundary.
```

In `0.0.x` a resident `Docket.Runtime` process is the single writer for a run's
whole lifetime. In the operational backend a stateless worker takes a
lightweight claim, advances the run, commits under a token-and-sequence fence,
and releases. Claim expiry and steal can briefly leave two workers executing;
the claim does not make duplicate execution or external effects impossible.
It identifies the only current commit authority, and the fence guarantees one
durable state winner. Integrations deduplicate external effects only when they
honor Docket's stable idempotency keys.

The substrate is deliberately minimal: the Postgres backend ships inside the
`docket` package behind optional `ecto_sql`/`postgrex` dependencies (section
4), so the paved road is one line in `deps`, exactly as Oban is. **The run
row is the queue.** A run carries its own schedule (`wake_at`), its own
execution ownership (`claim_token` / `claimed_at`), and its own consecutive
claim counter. A per-node dispatcher polls for runnable runs with
`FOR UPDATE SKIP LOCKED`, claims one, and drives it through supersteps until
a yield boundary, then parks. Crash recovery is a claim expiring and the next
poll picking the run up. There is no job table beside the run table. In
`0.1.0`, database constraints require every non-poisoned `running` row to be
either scheduled or claimed; a silently idle running row is invalid data, not
a supported lifecycle state. None of this coordination is
novel, and that is deliberate: it is the standard Postgres queue pattern
(Oban, GoodJob, River) with the run as the job. Docket's novelty budget is
spent entirely on graph execution semantics; the operational layer's job is
to be recognizable and boring.

## 2. Release Boundary

### 0.0.1 - Core Runtime

`0.0.1` is the current package line.

It includes:

- `Docket.Graph` document construction and serialization.
- Graph compiler and runtime lowering.
- `Docket.Run` durable run document.
- Pregel-style runtime loop.
- Supervised per-run `Docket.Runtime` processes.
- Local registry ownership inside one BEAM node.
- Checkpoint callback boundary.
- Inline test runtime.

It deliberately leaves these to the host application:

- Run and graph persistence.
- Tenant ownership and authorization.
- Durable signal delivery.
- Cross-node execution and recovery.
- Database migrations and operational tables.

### 0.1.0 - Operational Runtime

`0.1.0` is the first release where Docket's Postgres backend owns the durable
lifecycle of runs.

That ownership is the only supported production lifecycle in `0.1.0`.
Supervised Docket instances require one `Docket.Backend`; the `0.0.1`
host-owned checkpoint driver and its `run`, `resume`, and live `get_run`
facade are removed rather than retained as a second operating mode. This is a
narrow production-boundary break: node modules, graph definitions, schemas,
reducers, interrupts, executors, `Docket.Run` serialization, and the
Postgres-free `Docket.Test` graph-semantics helpers remain public.

It should include:

- A first-class `Docket.Postgres` backend inside the `docket` package,
  enabled by optional `ecto_sql` and `postgrex` dependencies — one package to
  install, no version pair to manage.
- Ecto migrations for Docket-owned operational tables.
- Durable run store and graph store.
- Per-run claim and optimistic commit fence for one durable state winner.
- Synchronous, serialized signal application with confirmed results.
- Optional event persistence, including one metadata-only
  `:checkpoint_committed` event per retained checkpoint.
- Run scheduling via `wake_at` on the run row and a `SKIP LOCKED` dispatcher.
- Crash recovery via claim expiry — the dispatch poll is the recovery path.
- Optional tenant scoping on run APIs — no tenant concept required to adopt.
- Stateless workers with no run-to-node affinity.
- A single backend-owned supervised production API; no storage-free supervised
  fallback or host-owned checkpoint committer.

The 0.1.0 user experience should be closer to installing Oban than to wiring a
set of low-level behaviours manually.

## 3. North Star

The host app should feel like this:

```elixir
defmodule MyApp.Docket do
  use Docket,
    repo: MyApp.Repo,
    backend: Docket.Postgres,
    concurrency: 50
end
```

`backend: Docket.Postgres` is self-contained: no Oban, no extra queue
framework, no second package, no second supervision story to configure. It is
also the substitution boundary. A backend bundle supplies one compatible
transaction boundary, graph store, run-aggregate store, event store, and
supervision tree. Those focused capabilities remain independently testable,
but callers cannot mix arbitrary store modules whose contexts cannot share one
atomic transaction. `concurrency` is the
dispatcher's per-node limit on concurrently advancing runs — the one knob for
how much work this node takes. Because the database connection is released
during node execution (section 6.1), that limit can be set high without pool
pressure. Per-workload isolation — routing a browser node's work separately,
capping llm work independently — is a post-v1 concern that arrives with
`Docket.Executor.Queue`, not something `0.1.0` needs.

Application code defines nodes and graphs, then starts or signals runs:

```elixir
{:ok, graph_ref} = MyApp.Docket.save_graph(graph)

{:ok, run} =
  MyApp.Docket.start_run(graph_ref, input,
    tenant_id: account.id,
    metadata: %{"workflow_id" => workflow.id}
  )

{:ok, run} =
  MyApp.Docket.resolve_interrupt(run.id, interrupt_id, "approved",
    tenant_id: account.id
  )
```

Signals are synchronous: the call applies the state change under the commit
fence in the caller's process and returns the updated run (or a validation
error) directly. `cancel_run` returns a confirmed cancellation, not an
"accepted" acknowledgment.

`tenant_id` is optional at the public facade. Internally every run operation
receives an explicit scope: `:system` for dispatcher/recovery, `:tenantless`
for public `tenant_mode: :none` access (`tenant_id IS NULL`), or
`{:tenant, tenant_id}` for scoped access. A missing option never becomes a
privileged unscoped read (section 5).

Docket owns the lifecycle behind those calls:

```text
insert durable run with wake_at = now
a dispatcher claims the run and advances supersteps
checkpoint each superstep under the commit fence
park: final checkpoint, release the claim, set the next wake_at (or none)
apply signals synchronously under the fence and wake the run when runnable
recover crashed runs when their claim expires
finalize terminal runs
prune old operational data
```

One internal, substrate-neutral `Docket.Lifecycle` layer owns the transaction
recipes for start, checkpoint commit, and signal application. Facades,
vehicles, and durable test drivers call that layer; stores never orchestrate
other stores, and `Docket.Postgres.Storage` remains a transaction boundary
only.

## 4. Package Shape

One package: `docket`. The core runtime keeps no dependency beyond
`telemetry`; the Postgres backend lives in the same package behind optional
`ecto_sql` and `postgrex` dependencies and compiles only when the host has
them — which any Ecto application already does. Adoption is one `deps` line,
one version, one changelog.

#### Decision record: why not a separate `docket_postgres` package

An earlier revision of this spec split the backend into its own
`docket_postgres` package to keep the core dependency-free. Optional
dependencies achieve the same purity — the core-only user still pulls in
nothing but `telemetry` — while removing a real adoption tax: a second
dependency line, a second version stream, and a `docket`/`docket_postgres`
compatibility matrix every upgrade has to consult. Oban is attachable partly
because it is *one* line in `deps`. A separate backend package would earn its
keep only if alternative first-party backends were imminent, and they are
explicitly a non-goal (section 13). The follow-on packages listed below stay
separate because they are genuinely optional products, not halves of one
product.

### Core (`Docket.*`)

The dependency-free core.

Owns:

- Public graph and run documents.
- Compiler and runtime graph lowering.
- Node, checkpoint, executor, and guard contracts.
- Runtime loop semantics.
- Processless initialization/transition driver that produces one uncommitted
  runtime moment at a time without storage writes, handler delivery, or
  committed telemetry.
- Inline test runtime.
- One backend-bundle contract plus focused transaction, graph-store,
  run-aggregate-store, and event-store capability contracts.
- Substrate-neutral lifecycle composition for start, moment commit, and signal
  application.
- Signal application as pure run-mutation functions (section 8).

Must avoid:

- A hard (non-optional) dependency on Ecto or Postgres.
- Assuming global BEAM clustering.
- Assuming one deployment topology.
- Owning enterprise-specific authorization policy.

### Postgres backend (`Docket.Postgres.*`)

Reference production backend, compiled when the optional `ecto_sql` and
`postgrex` dependencies are present.

Owns:

- Migrations for Docket-owned tables.
- Graph store for immutable, content-addressed graph versions.
- Run-aggregate store for row encoding, due-claim acquisition, claim
  refresh/release, fenced lifecycle commits, serialized mutation, scheduling,
  and poison recovery. These operations share one row invariant and are not
  independently swappable coordination and persistence plugins.
- Event store for append policy. Retention execution belongs to the pruner;
  retained checkpoint history uses metadata-only events.
- Per-run claim, optimistic commit fence, and atomic commit-and-schedule.
- The dispatcher: `SKIP LOCKED` claim polling, a `LISTEN/NOTIFY` fast path,
  per-node concurrency, and graceful shutdown draining.
- Synchronous signal application.
- Consecutive claim accounting and poison-run marking inside atomic claim
  acquisition.
- The pruner (a periodic, idempotent cleanup process).
- Telemetry for dispatch, claim, checkpoint, signal, and prune operations.

This backend should not feel like an example adapter. It is the paved road.

#### Decision record: why not on Oban

An earlier revision of this spec built the Postgres backend on Oban: advance jobs
and signals as Oban jobs, recovery via Oban retry plus a cron reconciler,
pruning via Oban's pruner. That path was rejected, and the reasoning is
recorded here so it does not get re-litigated.

The unit of scheduling in Docket is the **run**, not the job. A run needs
exactly one pending wake at a time — now, at a future instant, or none until
an external event. Modeling that on a general job queue meant maintaining two
sources of truth (the run row and its job) and building machinery whose only
purpose was reconciling them:

- A uniqueness dance (advance jobs unique per run over schedulable-but-not-
  executing states) to simulate "at most one pending wake per run."
- A cron reconciler to heal runs that were runnable but had no advance job —
  an invariant that can only be violated because the schedule lived in a
  different table than the run.
- Poison mirroring, so an Oban job discard became visible as run state.
- Two lock layers (Oban's job lock and Docket's run claim) and two recovery
  clocks (Oban's orphan rescue and Docket's `orphan_ttl`) that had to be tuned
  relative to each other.

With `wake_at` on the run row, that entire class of machinery is structural:
the park commit sets the next wake in the same fenced UPDATE it already
performs, so a run is never advanced-but-unscheduled by construction. The
dependency story also matters: Oban is an application-level framework, and a
library embedding it must either share the host's Oban instance (entangling
retention, pruning, and plugin config with correctness) or run a private
instance (a second notifier, second cron leader, and a version constraint on
the host's Oban upgrade cadence). Oban itself depends only on Ecto and
Postgres; for Docket to be attachable the way Oban is, it should too.

What Docket takes on in exchange: the dispatcher loop itself — a well-understood
Postgres pattern (`FOR UPDATE SKIP LOCKED`) with no leadership and no
clustering — plus its own pruner interval and its own telemetry. That is less
code than the signal worker, reconciler, uniqueness tuning, and poison
mirroring it replaces.

If asked "why didn't you use Oban?": the unit of scheduling is the run, not
the job — one row per run with a wake time *is* the queue, and Oban would have
been a second copy of that truth to keep consistent.

### Later Optional Packages

Potential follow-on packages:

- `docket_queue`: durable remote executor protocol (`Docket.Executor.Queue`
  for off-slot node dispatch; it may itself integrate with a host's queue).
- `docket_dashboard`: run inspector, graph timeline, open interrupts, failed
  nodes, in-flight runs, and recovery status.

These should remain optional. Correctness belongs to durable storage and the
per-run claim, never to a process registry. The stateless model means multi-node
and multi-region scale-out is "run more dispatchers," not a clustering package.

## 5. Durable Data Model

The Postgres backend should start with boring, explicit tables.

Recommended baseline:

```text
docket_graph_versions
docket_runs
docket_events
```

There is no signal table, no job table, and no checkpoint table. Signals are
synchronous fenced commits (section 8), the run row itself carries the
schedule, and checkpoint history is metadata-only event data (see the
state-size notes below). Everything the correctness path reads lives on the
run row. The exact naming of the Docket tables can change, but the ownership
concepts should not.

`docket_graph_versions` exists because recovery is autonomous: a worker
picking up a recovered run on another node must be able to load the exact
graph content by `graph_id + graph_hash` with no host call in the loop. Graph
content that any retained run references must outlive that run. A composite
foreign key from `docket_runs(graph_id, graph_hash)` to
`docket_graph_versions(graph_id, graph_hash)` enforces this relationship. The
pruner deletes a graph version only when no run row references it — not merely
when no active run references it. This preserves inspection and operator retry
of retained failed runs and closes start-versus-prune races.

Retained events do not outlive their run in `0.1.0`. A foreign key from
`docket_events.run_id` to the unique `docket_runs.run_id` uses delete cascade.
Event retention may be shorter than run retention, but not longer; otherwise
an orphaned event would lose the tenant and graph scope carried by its run.
Longer-lived audit export is a separate product contract, not an accidental
consequence of missing referential integrity.

There is deliberately no `docket_graphs` parent table in `0.1.0`. Every
operation the release defines — publish upsert, recovery load, prune
retention — is keyed by `(graph_id, graph_hash)` and touches only version
rows. A parent table earns its place when graph-level data exists (a
latest-version pointer, listing, graph-level ownership), which arrives with
graph catalog/version-management APIs post-`0.1.0`; adding it then is a purely
additive migration.

Publishing is explicit and content-addressed. `save_graph(graph)` snapshots
node configuration schemas once, materializes their defaults, validates and
compiles that effective graph, then stores its canonical
`Docket.Graph.to_map/1` document keyed by `graph_id + graph_hash` and returns a
stable graph reference.
Postgres implements `save_graph` with `ON CONFLICT DO NOTHING`. Two nodes racing
to publish the same version both succeed idempotently. If the existing document
under that key is not structurally equal to the canonical JSON document,
`save_graph` returns a graph-content conflict rather than treating the conflict
clause as proof that the content matched.

`start_run(graph_ref, input)` fetches the already-saved effective canonical
document and compiles it locally before entering the run/event lifecycle
transaction. It never writes the graph store. Recovery uses the same
`fetch_graph` operation. A vehicle compiles once per claim and reuses the
derived runtime graph across its drain loop. The canonical document remains
the portable storage contract; an optional release-scoped local cache is not a
correctness boundary.

### `docket_runs`

**The row is the run.** There is no `docket_run` document column: the run's
stable public fields are relational columns, and only the Docket-owned
execution internals — channels, interrupts, pending nodes and writes, active
tasks, timers, internal counters — live in a single `state` jsonb column. The
line between the two is not new; it is the contract `Docket.Run` already
declares: hosts may inspect the top-level fields (`id`, `graph_id`,
`graph_hash`, `status`, `step`, `input`, `output`, timestamps) and must never
interpret the internals. Columns are that inspectable surface plus what the
operational layer itself reads and writes; `state` is exactly the
"do not interpret" blob, versioned internally by the document's own `version`
field so its shape can evolve without migrations. The store maps between the
row and `Docket.Run` via the existing `to_map/1` / `from_map/1` boundary; core
is unchanged.

This shape means every fact is stored once. `status`, `step`, and the fence
sequence are not denormalized copies of fields inside a document blob — they
are the storage. `checkpoint_seq` the column and `checkpoint_seq` the document
field are the same value, so there is no dual-write to keep consistent and no
drift class to test for.

Required concepts:

```text
tenant_id
run_id
graph_id
graph_hash
status
step
input
output
failure
metadata
state
checkpoint_seq
latest_checkpoint_type
claim_token
claimed_at
wake_at
claim_attempts
poisoned_at
poison_reason
inserted_at
started_at
updated_at
finished_at
```

There is no owner node, no lease epoch, and no companion job row. Three column
groups carry the whole operational model:

- **Schedule — `wake_at`.** For an unclaimed run, this is the single source of
  truth for when it should next advance. `now` means runnable, a future
  instant means a timer or retry backoff, and `NULL` means the row is claimed,
  externally parked, poisoned, or terminal. Claim acquisition clears
  `wake_at`; a park atomically clears the claim and restores the next wake.
- **Execution ownership — `claim_token` / `claimed_at`.** Single-writer is a
  claim plus an optimistic fence on `checkpoint_seq` at commit — not a
  held connection and not a correctness-bearing expiry (see section 6).
  `claimed_at` lets the dispatcher tell whether the current holder is still
  alive.
- **Operational health — `claim_attempts`, `poisoned_at`, `poison_reason`.**
  `claim_attempts` counts claims actually launched since the last committed
  run mutation and resets on committed progress. If another claim is needed
  after the configured maximum has already launched, atomic claim acquisition
  poisons the row instead of launching one more vehicle. Thus a maximum of
  three permits three executions and poisons only when all three produced no
  commit. `poisoned_at IS NULL` is the normal condition; no `active` enum is
  stored, and the undefined `blocked` state is not part of `0.1.0`.
  `retry_poisoned_run` clears the poison facts and claim counter and records an
  immediate wake. Poison is orthogonal to graph status and is surfaced through
  operational inspection rather than added to `Docket.Run`.

The durable graph status set is deliberately small:

```text
running | waiting | done | failed | cancelled
```

`running` covers ready, claimed, timer-scheduled, budget-yielded, and
retry-backoff positions; those positions are derived from schedule, claim, and
active-superstep facts. `waiting` means no autonomous work can proceed and an
external graph mutation is required. The three terminal values are retained
because success, graph failure, and intentional cancellation have different
API, retry, and operational semantics. `finished_at` records when any terminal
outcome occurred. Every smaller vocabulary was audited and rejected — see the
decision record below.

#### Decision record: why five flat values survive the trim audit (2026-07-09)

A dedicated audit steelmanned each smaller status model before the `v0.1.0`
lock. All of them lost, for the same underlying reason: minimalism must be
measured as total moving parts across the system — CHECK constraints, partial
indexes, signal preconditions, await/inspect shapes, and every consumer's
render path — not as enum length. Each trim saves at most one token while
relocating a currently SQL-enforceable or typed-column invariant into opaque
JSON or application-only derivation.

- **Fold `cancelled` into `failed` + cause.** Removes no column (`failure`
  already exists) and relocates three typed contracts into JSON:
  `cancel_run`'s three-way idempotency branch would read `failure->>'kind'`
  instead of one indexed status comparison; the "`failure` present exactly for
  `failed`" CHECK dies, because a cancelled run has no failure to describe and
  a synthetic payload would have to be fabricated; and the reserved
  graph-semantic `retry_failed` must never auto-retry an intentional
  cancellation, which a folded encoding can only express as a negative JSON
  predicate carried by every retry and dashboard query.
- **Fold `waiting` into `running`.** Relaxes the lost-run detector — "a
  non-poisoned `running` row has exactly one of a wake or a claim" — to "at
  most one", making a stuck advanced row (no wake, no claim, no open
  interrupt) a legal row and demoting that invariant from SQL-enforced to
  application-only. `await_run` and operational inspection would re-derive
  "blocked on input" from interrupt JSON that hosts must not interpret.
- **Replace the enum with outcome timestamps** (`done_at` / `failed_at` /
  `cancelled_at`). A weaker three-null encoding of the same sum type: it
  admits two-outcomes-set and terminal-with-no-outcome rows that the enum
  forbids structurally, turns "not terminal" into a negative three-column
  predicate, and cannot back the positive dispatch-eligibility index.
- **Split into two fields** (`status ∈ running | waiting | finished` plus
  `outcome ∈ done | failed | cancelled`, GitHub-Actions-style). The strongest
  alternative — dispatch eligibility and the partial indexes survive — but it
  expands rather than trims: six tokens across two columns instead of five in
  one, a `finished` value that duplicates what `finished_at` already encodes,
  a new coupling CHECK (`outcome` present exactly when `finished`), and a
  fresh class of illegal combinations the flat enum cannot even express.

Mature precedent agrees with the flat model, weighted by similarity to the
run-row-is-the-queue design: Oban keeps intentional `cancelled` distinct from
exhausted `discarded` on the job row; Temporal, Step Functions, and GitHub
Actions all keep operator stop distinct from failure. No surveyed system folds
cancellation into failure. Where Docket deliberately departs from Oban —
queue position derived from `wake_at`/claim columns rather than fused into
status — the derived facts carry no invariant of their own, which is exactly
why they may be derived while the five semantic values may not.

`:created` remains only as a private fresh-run sentinel used while calculating
an initialization moment. It is never a durable/public operational status,
never appears in a checkpoint, is not cancellable, and backends reject it.
`Docket.Run.failure` is a stable JSON-safe description of a terminal graph
failure and is promoted to the `failure` column. It is present exactly when
`status = 'failed'`; it is distinct from retryable node-attempt failures,
poison reasons, API validation errors, fence loss, and observer failures.

**Tenancy is optional but scope is explicit.** Runs are keyed by `run_id`
alone; `tenant_id` is a nullable, indexed scoping column. `tenant_mode: :none`
uses `:tenantless` and can read only rows whose `tenant_id IS NULL`.
`tenant_mode: :required` uses `{:tenant, id}` and a mismatch reads as
`:not_found`. Dispatcher/recovery use the privileged `:system` scope. Store
APIs require one of those values; omitting a tenant keyword can never fall
through to system access.

There is no separate resume-target column: a recovered run has no fixed resume
target. A vehicle loads the committed row, reconstructs `Docket.Run`, and
drives from whatever state it finds. `step` is a column because it is the
single storage location of the document's field — introspection reads it, but
nothing in recovery treats it as an instruction.

Important indexes:

```text
unique (run_id)
partial (tenant_id, status) WHERE tenant_id IS NOT NULL
partial (tenant_id, graph_id, status) WHERE tenant_id IS NOT NULL
partial (wake_at, id) WHERE status = 'running' AND poisoned_at IS NULL
                      AND claim_token IS NULL AND wake_at IS NOT NULL
partial (claimed_at, id) WHERE status = 'running' AND poisoned_at IS NULL
                         AND claim_token IS NOT NULL
partial (poisoned_at) WHERE poisoned_at IS NOT NULL
(status, updated_at)                              -- ops introspection
```

Ready-unclaimed and expired-claim recovery are separate indexed paths. Each
index carries the path's scheduling timestamp followed by the internal
`bigserial id`, which is the stable, compact tie-breaker for equal timestamps.
These are baseline ordered partial indexes, not a promise that their exact
shape is permanently optimal: included columns, alternative predicates, and
replacement indexes remain evidence-driven migration choices. If the internal
surrogate key is removed later, `run_id` becomes the unique tie-breaker. Fresh
claimed rows never retain an old `wake_at`, so they cannot dominate the ready
LIMIT scan.

Database CHECK constraints make the lifecycle tuple authoritative even for
raw claim SQL:

- stored status is one of the five durable values above;
- `started_at` is present for every stored run;
- `finished_at` is present exactly for terminal status;
- `failure` is present exactly for `failed`, and `output` only for `done`;
- `claim_token` and `claimed_at` are paired;
- `poisoned_at` and `poison_reason` are paired;
- terminal and `waiting` rows have no claim, wake, or current poison;
- a poisoned row is `running` with no claim or wake;
- a non-poisoned `running` row has exactly one of a wake or a claim; and
- `step`, `checkpoint_seq`, and `claim_attempts` are non-negative.

The dispatcher uses positive eligibility (`status = 'running' AND
poisoned_at IS NULL`) so a future status cannot become runnable by omission
from a negative terminal list.

### State size and write amplification

A run document is not a job row. Oban args are small; `state` carries every
channel value, and for the target workload — agentic LLM sessions — that can
be megabytes of transcript rewritten at every superstep commit. The hot path
must not multiply copies of that document:

- **The run row is the only full document.** Recovery needs exactly the
  latest committed run, and it is reconstructed from the `docket_runs` row —
  promoted columns plus `state`. No other table stores a full run snapshot.
- **The column/state split keeps small writes small.** A superstep commit
  rewrites `state` because channels changed — that cost is intrinsic. But a
  signal that touches no execution state (`cancel_run` flipping `status`)
  updates in-line columns only; Postgres carries the unchanged out-of-line
  `state` TOAST value over without rewriting it. Under a single-document
  design the same cancel would rewrite megabytes to flip one field. The row
  codec keeps the complete canonical run state for `0.1.0`, including any
  input values also present in input-backed channels. That duplication is
  measured and documented. Omit/rehydrate optimization is deferred until its
  immutability and versioning rules can be specified without weakening cold
  recovery.
- **Checkpoint history is events, not a table.** Nothing on the correctness
  path reads checkpoint history: recovery loads the run row, the fence is the
  `checkpoint_seq` column, and the latest checkpoint type is a run column. A
  checkpoint proposal therefore allocates one metadata-only
  `:checkpoint_committed` `Docket.Event` from the run's independent
  `event_seq`, after its runtime facts. Its metadata carries
  `checkpoint_seq`, checkpoint type, step, park reason, and wake disposition.
  EventStore appends assigned identities; it never allocates with `MAX(seq)`
  or reuses `checkpoint_seq`. The fact is stored in `docket_events` — never
  the run document — under the same persistence policy and pruner as every
  other event. Turning event
  persistence off costs history, never correctness. Storing
  O(supersteps × state size) snapshots would be the hidden cost that melts an
  adopter's database; if replay or time-travel debugging ever becomes a goal,
  full-snapshot retention becomes an explicit opt-in policy at that point.
- **Event persistence is a policy, not a given.** Events default to on, but
  `0.1.0` ships the volume knob (persist all, none, or selected types),
  because turning event volume down is the first request every high-volume
  adopter makes.

The remaining cost — TOAST churn and WAL volume from rewriting `state` once
per superstep — is bounded per dispatch by the drain budget. Per-channel or
delta storage is a possible post-v1 optimization scoped to the `state` column
alone; it must not change the correctness story (one fenced, single-row
commit).

**What belongs in state — the cold-resume test.** Megabyte state is the
workload the write path must survive, not the pattern the docs should teach.
State is a resume contract, not an audit log: a value belongs in a channel
only if a run resuming cold on another node needs it for the next superstep
to behave correctly. A sub-graph or specialist node that can rebuild its
context from the inputs it is handed should write back its conclusion — the
artifact, the decision, the structured result — not its transcript; only
memory that must survive across invocations checkpoints. Data kept for
observation rather than resumption goes to `docket_events` or to host storage
by pointer, and conversational fidelity uses compaction — a rolling summary
plus a recent-turns window (the `:append` reducer's `max_length` option) —
rather than an unbounded transcript channel. The paved-road docs and examples
should demonstrate compact state from the first example.

## 6. Coordination And Fenced Commits

### Execution model: state, schedule, vehicle

The backend separates three roles that a resident process previously fused:

- **State — Postgres.** The durable run — the `docket_runs` row — is the
  source of truth. A parked run lives entirely here and needs no process.
- **Schedule — `wake_at` plus the dispatcher.** The run row says *when* it
  should next advance; the per-node dispatcher turns due rows into work,
  coordinates with the run claim so only one vehicle drives at a time, and owns
  waits, retries, and backoff. It drives *when*, not *how*.
- **Vehicle — a BEAM process.** When the dispatcher claims a run, it spawns an
  ephemeral runtime that loads the run and compiled graph into memory and drives
  `Docket.Runtime.Loop` through a drain of supersteps — the same loop the
  resident `Docket.Runtime` runs, scoped to one drain between two parks. At the
  park it checkpoints and exits.

A run is swapped out to Postgres at each park and swapped in to a fresh vehicle
on the next wake — demand paging for graph runs. Normally one vehicle executes
it. After expiry and steal, a stale and current vehicle can overlap; only the
current token can commit. Because the runtime is processless (section 4), the
vehicle is just a third shell over it alongside the GenServer and inline test
drivers.

The vehicle is a local, ephemeral compute resource, never a unit of addressing.
It runs on whichever node's dispatcher claimed the run, has no home node, and
nothing outside needs to find it. Correctness lives entirely in the state and
the schedule; the process is disposable. An optional per-node registry can make
an *active* vehicle locally reachable for fast reads (see sections 7 and 10),
but that is a latency optimization, never a correctness path.

### The dispatcher

Each node runs one dispatcher (under the backend supervision tree that
`MyApp.Docket` starts). It computes demand and asks the run-aggregate store for
an atomic batch claim. The store, not the dispatcher, owns eligibility,
attempt accounting, steal, and poison mutation.

```sql
ready path:
  status = 'running'
  AND poisoned_at IS NULL
  AND claim_token IS NULL
  AND wake_at <= now()
  ORDER BY wake_at, id
  LIMIT $path_demand
  FOR UPDATE SKIP LOCKED

expired path:
  status = 'running'
  AND poisoned_at IS NULL
  AND claim_token IS NOT NULL
  AND claimed_at < now() - $orphan_ttl
  ORDER BY claimed_at, id
  LIMIT $path_demand
  FOR UPDATE SKIP LOCKED

for each locked candidate, atomically either:
  - if claim_attempts < max_claim_attempts, assign a fresh token,
    set claimed_at = now(), clear wake_at, increment claim_attempts,
    and return a lightweight claim lease; or
  - otherwise clear claim/wake, set poisoned_at/poison_reason, and return an
    operational poison result without launching a vehicle.
```

`$demand` is `concurrency` minus in-flight vehicles on this node. A maximum of
three therefore launches claims one, two, and three; only a later recovery
need poisons the run. The claim API returns leases/run identities and poison
outcomes, not a promise that arbitrary decoded run documents were selected
outside the transaction.

Candidate selection is part of the bounded-claim correctness contract, not
merely a planner optimization. The store fences each limited candidate
relation before the mutation; the Postgres `0.1.0` implementation uses a
materialized CTE optimization fence, or an equivalent statement whose plan and
concurrency tests prove that the UPDATE cannot expand beyond the selected
rows. Across the ready and expired paths together, claimed plus poisoned
outcomes never exceed the caller's demand. The ordering above defines scan
preference among candidates visible and unlocked in that transaction; it does
not promise a global execution order across concurrent `SKIP LOCKED`
dispatchers.

The exact SQL statement shape and the split of demand between ready and
expired paths are internal policy. They may be tuned under production load
without changing the RunStore contract, provided neither continuously eligible
path is permanently starved and the combined demand bound remains intact.

Two wake paths feed the loop:

- **Poll.** A short interval (default around one second) covers scheduled
  wakes, expired claims, and any lost notification. `poll_interval` is the
  maximum scheduled delay, not a fixed metronome: each dispatcher chooses a
  bounded jittered delay no greater than that value so nodes do not repeatedly
  stampede the database in phase. The jitter distribution and any future
  adaptive idle backoff are internal tuning policy; they may change only while
  preserving the configured upper bound. The ordinary ready and expired-claim
  scans are the recovery path; there is no reconciler.
- **Notify.** `Docket.Postgres.RunStore` issues `pg_notify` inside the same
  transaction whenever one of its writes records an immediate wake (start,
  park, signal, or poison recovery). PostgreSQL makes the notification visible
  only after commit and suppresses it on rollback. A matching claim release
  also records an immediate wake but is a rare failure path and deliberately
  relies on the scheduled poll rather than a notification site. Dispatchers listen and
  request an immediate poll, so the common park-to-resume hop is milliseconds,
  not a poll interval. Immediate requests are coalesced: a dispatcher runs at
  most one claim poll at a time and a notification burst cannot build an
  unbounded queue of redundant polls. Notifications are a latency optimization
  only; the scheduled poll guarantees progress without them.

The notify path needs a dedicated `Postgrex.Notifications` connection, and
`LISTEN` does not survive PgBouncer-style transaction pooling. Poll-only
operation is therefore a supported, documented configuration — not a degraded
accident: hosts behind a transaction-pooling proxy disable the notifier and
the poll interval alone sets wake latency. Correctness is identical by
construction, because notifications were never load-bearing.

Multiple dispatchers across any number of nodes race via `SKIP LOCKED`; one
wins each row and the rest skip it. No leadership, no clustering, no
coordination beyond the database.

On graceful shutdown the dispatcher stops claiming, waits for in-flight
vehicles up to a drain timeout, and lets anything that outlives the timeout be
recovered by claim expiry elsewhere.

### Single-writer commits

Two things must be true, and they have very different durations. Conflating them
into one held lock is a mistake, because one of them can span minutes of node
I/O.

- **Single committer (short).** Only one worker may commit a checkpoint for a
  run at a time. This is a millisecond database write.
- **Execution ownership (long).** A fresh claim normally prevents another
  worker from driving the run. After expiry/steal, overlap is possible. This
  interval can span slow external I/O, so it must not hold a database
  connection; token fencing, not mutual exclusion, protects durable state.

**Advance commits are optimistic.** Every vehicle commit uses a conditional
UPDATE fenced on monotonic `checkpoint_seq` and its `claim_token`. Nothing is
held while node code executes; only the row lock intrinsic to the millisecond
write exists:

```sql
-- one short transaction per advance-worker superstep
UPDATE docket_runs
SET state = $state,
    status = $status,
    step = $step,
    output = $output,              -- non-null only at a terminal commit
    failure = $failure,            -- non-null only at a failed terminal commit
    checkpoint_seq = $seq + 1,
    latest_checkpoint_type = $type,
    claim_attempts = 0,            -- committed graph progress proves health
    claimed_at = now(),            -- mid-drain: refresh
    -- at a park: claim_token = NULL, wake_at = $next_wake_or_null
    updated_at = now()
WHERE run_id = $run_id
  AND checkpoint_seq = $seq
  AND claim_token = $claim_token;

```

**Signals are serialized by the storage backend.** A signal transaction takes
a short row lock, loads and validates the current run, applies the pure core
transition, increments the sequence, clears any live claim, and chooses the
next wake before committing. This lock is never held across node execution or
external I/O. An advance commit already in progress finishes first; subsequent
advance commits wait briefly and then fail their token/sequence fence. This
gives `cancel_run` a confirmed result without an unbounded optimistic retry
loop.

If this affects zero rows, someone else already committed past `$seq`, the
worker lost its claim, or a signal changed the run first. An advance worker
discards its uncommitted work, **releases its claim, and stops**. The release
is its own small UPDATE fenced on `claim_token` alone; it never touches
`checkpoint_seq`. A matching release clears the claim **and records an
immediate wake** (`wake_at = now()`):

```sql
UPDATE docket_runs
SET claim_token = NULL, claimed_at = NULL, wake_at = now(), updated_at = now()
WHERE run_id = $run_id AND claim_token = $claim_token
```

The recorded wake is required, not a courtesy. Claim acquisition cleared
`wake_at`, so a release that only cleared the token would leave a non-poisoned
`running` row with neither claim nor wake — stranded from both the ready and
expired-claim dispatch paths and forbidden by the section 5 CHECK ("exactly
one of a wake or a claim"). With the wake recorded, the row re-enters the
ready path and its next claim either makes committed progress or drives
`claim_attempts` toward the poison threshold, which is exactly how a run that
repeatedly cannot advance becomes an explicit operator concern. The same
release runs when a claimed run turns out not to be runnable and on the
event-append rollback path where no commit won. When the fence was lost
because another writer won — a steal, or a serialized signal mutation, both of
which clear or replace the live claim and set the schedule themselves — the
release matches zero rows and is a no-op, so it can never disturb a schedule a
winning commit chose. A matching release can never hit a `waiting` or terminal
row, because those rows carry no claim to match. The fence is checked only at
commit time; nothing is held during execution.

Losing the fence has a real cost, and the docs should state it plainly: the
vehicle discards an uncommitted superstep, which may include completed
expensive node work such as an LLM call. If the winning commit was a signal
that changed state, replanning after re-dispatch can produce different task
identities, so idempotency keys will not dedupe the discarded attempt's
external effects — that work is wasted, or its side effects fire again under
new keys. This is the correct trade: cancellation and interrupt resolution
must win over in-flight work, and the window is one superstep commit. But it
is a cost model, not a free lunch, and mid-drain signals against runs with
expensive supersteps are where it shows.

**Execution ownership is a claim, not a connection.** The dispatcher wins the
claim in its one short claim statement, then releases the connection; the
vehicle executes node code holding nothing.

Every mid-drain superstep commit refreshes `claimed_at`, so a multi-step drain
keeps its claim fresh through commits. A single long superstep is different: an
LLM or browser node may spend minutes in external I/O before the next commit
exists. `0.1.0` must make that window explicit by either (a) enforcing a maximum
node execution timeout comfortably below `orphan_ttl`, or (b) running a
lightweight claim refresh guarded by `claim_token` while the worker awaits node
results. The MVP may choose timeout alignment first, but it must not rely on
mid-drain commits alone to keep a long single superstep fresh.

The **park commit** does the opposite: it **releases the claim** (`claim_token =
NULL`) and sets the next `wake_at` in the same transaction as the final
checkpoint, so the very next wake — including one a signal triggers moments
after a run parks waiting — dispatches immediately instead of waiting out
`$orphan_ttl`. A claim is therefore held only for the duration of one drain,
cleared on any normal park or fence loss, and left set only by a crash (no
release ran). The dispatcher never hands out a run with a fresh claim — the
claim predicate in the dispatch query excludes it — and steals the claim only
once `claimed_at` is older than `$orphan_ttl`, which by construction only
happens after lost liveness (a crash, killed task, or shutdown timeout).

`$orphan_ttl` is a **liveness hint, not a correctness mechanism.** Set it above
the vehicle drain budget plus expected clock skew and shutdown delay; with the
heartbeat option, the ttl bounds how long recovery waits after the last
successful refresh. If the ttl were wrong, the worst case is two workers briefly
executing the same superstep — the advance fence lets only the current claim
holder commit, the sequence fence lets only one state mutation win, and the core
idempotency-key invariant gives cooperating integrations the same key for both
attempts. Docket guarantees one durable state commit; external effects are
deduplicated only when the integration honors that key. Clock skew can cost
duplicated work or effects, but it cannot cause a double state commit. This is
the same safety
posture as the resident model's split-brain window, with no owner node and no
lease epoch.

The atomic commit-and-schedule happens at a park boundary (section 9).
`Docket.Lifecycle`, not the core loop or either store, composes the transaction.
Inside one `Docket.Storage.transaction/2`, the run store commits the run under
its fence, releases or refreshes the claim, and sets the next `wake_at`; the
event store appends the proposed event rows (checkpoint-commit metadata among
them). Neither store independently commits. When the wake is immediate, the
Postgres run store executes `pg_notify` in that transaction; PostgreSQL
delivers it only after commit. Within a drain, each superstep
commits on its own, and the in-process loop is the "next" — the live vehicle
is itself the schedule. The invariant: a run is always either terminal, parked
with an explicit wake source, poisoned with an operator recovery path, or
claimed by a vehicle holding the current token. It never sits advanced with no way to resume —
and unlike a job-table design, this is enforced by the shape of the data, not
by a reconciler. This also makes step persistence effectively synchronous
regardless of the core's `:step_committed` async hint: the worker durably
commits each superstep before continuing, so execution never runs ahead of
persistence.

Advancing is idempotent because a vehicle drives from committed state, not a
fixed target. A recovered run — its vehicle crashed mid-drain — is re-claimed
and resumes from the last committed superstep; there is nothing stale to skip.
Eligible selection cannot produce a claim on a terminal or waiting row, so a
vehicle holding a current claim on a run that turns out not to be runnable
reports an invariant violation, releases its claim, and does not drive; it is
never a routine release-and-ignore path. A crash before a
superstep commits re-executes only that superstep, with the same idempotency
keys available to cooperating integrations.

## 6.1 Execution And Worker Slots

Node code runs inside the vehicle, so a long node — an LLM call, a browser
task — occupies one unit of dispatcher concurrency for its duration. The
resident-process model avoided this by running nodes in cheap BEAM Tasks off
the run's process, with concurrency bounded only by the BEAM. Two things keep
the slot cost from becoming that model's feared starvation:

- **The connection is released during execution (section 6).** A vehicle
  blocked on node I/O holds no database connection and no lock — it is just a
  parked BEAM process, exactly what the resident+Task model used. The scarce
  resource, connections, is not tied up, so the concurrency limit can be set
  high (thousands of concurrently-awaiting supersteps per node) without pool
  pressure. Most of the resident model's non-blocking behavior is recovered.
- **One per-node limit with a high ceiling.** `concurrency` bounds how many
  vehicles a node runs at once; total throughput scales with node count.
  Because a blocked vehicle is cheap, a burst of long nodes does not back up
  unrelated runs until the ceiling itself is reached.

What this does not do is isolate one workload from another or take a very long
node off its slot: every advancing run draws from the same per-node ceiling,
and a blocked BEAM process still counts against it. Per-workload limits,
per-node-kind routing, and off-slot execution — for hour-long jobs or truly
remote executors — all arrive together post-v1 with `Docket.Executor.Queue`,
whose `{:await, …}` node return suspends the superstep, runs the node as its
own durable unit, and resumes by setting `wake_at` from the completion
callback, so the vehicle slot is held only for the short plan and apply
phases. The executor seam and `{:await}` shape are reserved for exactly this.
`0.1.0` ships synchronous execution behind the per-node concurrency limit;
per-workload isolation and off-slot async dispatch are the post-v1 evolution.

## 7. Routing Model

There is almost nothing to route. A run is not pinned to a node or a PID, so
there is no owner to find, no registry to keep correct across a cluster, and no
cross-node forwarding.

```text
storage = source of truth for run state
run claim + commit fence = one current commit authority and durable winner
wake_at + dispatcher = where the next unit of work waits for any free node
```

Request flow:

```text
authenticate the caller (tenancy, when used, is a scoping filter)
load run row by run_id (scoped by tenant_id when the caller passes one)
if terminal, serve from storage
otherwise apply the signal synchronously under the fence and return the result
```

A read (`fetch_run`) is a storage read. A state change (`cancel_run`,
`resolve_interrupt`, and the other signal functions) is a fenced commit
executed in the caller's process. Neither needs to know which node, if
any, is currently advancing the run. Dispatchers across any number of nodes —
or regions, pointed at the same database — pull work; correctness comes from
the claim, the commit fence, and the store, never from process lookup.

One current claim normally yields one worker; a steal may overlap a stale
worker, but only the current token can commit. Many runs advance concurrently
across all nodes with no affinity. This is what eliminates the clustering,
node-discovery, and PID-routing layers the earlier design carried.

### Optional local fast path

While a vehicle is actively draining a run, it may register itself in a
**per-node** registry keyed by `run_id`. A caller on the same node can then read
the live in-memory run (section 10) instead of the last durable checkpoint.
This is purely a latency optimization and stays within one node: the registry
is never authoritative, never consulted across nodes, and always backed by the
durable path — a miss (no local vehicle, or the vehicle on another node) falls
back to a storage read. `0.1.0` can ship with no registry at all; the
registered vehicle and its live reads are an additive later step that changes
no contract.

## 8. Signals And Operational Commands

State-changing public APIs are synchronous serialized storage mutations. There
is no signal queue, no signal worker, and no "accepted" acknowledgment: the
caller receives the committed result.

`0.1.0` separates graph-state signals from operational recovery commands:

- Graph signals mutate `Docket.Run` and produce an uncommitted runtime moment
  containing the next sequence, events, and explicit schedule disposition.
- Operational commands mutate backend-owned health columns and emit
  operational telemetry; they do not consume the graph run's checkpoint/event
  sequence or pretend those columns are part of `Docket.Run`.

Initial graph signals:

- `resolve_interrupt`
- `cancel_run`

Initial operational command:

- `retry_poisoned_run/2`

`resume_run` and graph-semantic `retry_failed` are deferred until their core
state transitions are specified. A waiting run resumes only by resolving its
open interrupt; an allegedly runnable but idle run is an operational invariant
violation, not a state that needs a generic resume signal.

| Operation | Allowed current state | Repeated call | Durable result |
| --- | --- | --- | --- |
| `resolve_interrupt` | `running` or `waiting` with the named open interrupt | terminal (`done` / `failed` / `cancelled`) returns `:inactive_run`, checked before the interrupt lookup; unknown returns `:not_found`; resolved returns `:already_resolved`; a different value is never silently accepted | write resolution, mark interrupt resolved, status `running`, `:interrupt_resolved`, release claim, clear current poison, `wake_at = now` |
| `cancel_run` | `running` or `waiting` | already `cancelled` returns the stored run; `done` / `failed` returns `:inactive_run` | status `cancelled`, finished timestamp, `:run_cancelled`, release claim, clear current poison, no wake |
| `retry_poisoned_run` | non-terminal run with `poisoned_at` | terminal always returns `:inactive_run`; an already-unpoisoned non-terminal run returns the stored run | clear `poisoned_at` / `poison_reason`, reset `claim_attempts`, clear claim, immediate wake, operational telemetry |

Signal application rules:

- Signals are addressed by `run_id`; the facade converts its tenancy mode to a
  required explicit scope before the storage mutation begins.
- The run store owns serialized mutation. The backend calls it inside one short
  storage transaction. Postgres uses `SELECT ... FOR UPDATE`; a non-relational
  backend may use an equivalent serialized mutation primitive. Core validation
  and transition calculation are pure and perform no external I/O while the
  lock is held.
- In that same storage transaction, the run store commits the new run, claim
  release, and wake while the event store appends checkpoint metadata and
  retained events. Only after the transaction commits do checkpoint observers
  and telemetry run.
- Validation errors return directly to the caller and change nothing.
- A concurrent advance commit either precedes the locked signal read or fails
  its next fence after the signal clears its claim and increments the sequence.
  There is no bounded retry loop that can make confirmed cancellation fail
  merely because the run is committing quickly.
- Precondition ordering is terminal-first: a terminal run returns
  `:inactive_run` before any interrupt lookup, matching `cancel_run`, so a
  terminal run that still carries an open interrupt reports `:inactive_run`
  rather than `:not_found`, and a resolution can never resurrect a finished
  run.
- A successful graph-signal mutation clears current poison facts. The signal
  is a deliberate operator or graph intervention that supersedes the
  stalled-claim history the poison recorded, and a poisoned row may not carry
  a wake (section 5 CHECK), so healing is required for `resolve_interrupt`'s
  immediate wake to be committable. `cancel_run` and `resolve_interrupt`
  therefore both clear poison; `retry_poisoned_run` remains the explicit
  operational recovery for a poisoned run nobody is mutating.

There is no signal struct, no signal behaviour, and no generic
`signal(run_id, type, opts)` dispatch. Core defines each graph signal as a pure
named run-mutation function producing a `Docket.Runtime.Moment` (or equivalent
core type). `Docket.Lifecycle` wraps it in the run store's serialized mutation
and composes event append through the shared transaction; the GenServer and
inline runtimes adapt the same transition directly.

Durability is reached when the transaction commits, before the call returns.
There is no general exactly-once request receipt in `0.1.0`; operations define
their repeated-call result explicitly in the table above.

## 9. Runtime Lifecycle

Core initialization, advancement, and graph signals produce exactly one
pre-commit runtime moment at a time. A moment contains the proposed run,
runtime events, checkpoint metadata, and an explicit schedule algebra:

```text
:continue
{:park, :immediate, reason}
{:park, :external, reason}
{:park, {:at, timestamp}, reason}
{:park, :terminal, reason}
```

It is not a public committed `Docket.Checkpoint`. `Docket.Lifecycle` persists
the moment and only transaction success creates/delivers the committed
checkpoint value. Backends may invoke separately configured best-effort
`checkpoint_observers` after commit; observers never own persistence and
cannot accept, veto, or roll back a moment.

### Start Run

```text
validate tenant and graph access
fetch the effective canonical graph by graph reference and compile it locally
Docket.Lifecycle.start(backend, scope, fn tx ->
  calculate initialized runtime moment without handler delivery
  Runs.insert_run(tx, initialized run with :immediate disposition)
  Events.append_events(tx, assigned :run_initialized runtime fact +
    :checkpoint_committed metadata fact, subject to event policy)
end)
Postgres RunStore issued transactional pg_notify for the immediate wake
deliver post-commit checkpoint observers and telemetry
return initialized run
```

### Advance (one dispatch cycle)

```text
the dispatcher asks RunStore.claim_due for demand
  each launched claim atomically increments claim_attempts and clears wake_at
  an exhausted candidate is poisoned atomically and is not launched
release the connection; load committed Docket.Run and effective graph document
compile once on this node outside GraphStore; reuse it for the whole claim drain
if the run is not runnable (terminal, or waiting with no input),
  report an invariant violation, release the current token, and stop
the vehicle loops: propose one moment -> Docket.Lifecycle.commit_moment -> continue
until a yield boundary
  each mid-drain commit refreshes the claim and resets claim_attempts
  a failed fence means discard, release the claim, and stop
  a single long superstep either fits inside orphan_ttl or refreshes the claim
at each moment, Storage.transaction(backend.storage, fn tx ->
  Runs.commit(tx, moment run, mandatory sequence+token fence, disposition)
  Events.append_events(tx, assigned runtime + :checkpoint_committed events)
end)
RunStore issues transactional notify for an immediate wake
after commit, deliver checkpoint observers/telemetry
```

### Yield Boundaries And Parking

A vehicle does not run a fixed number of supersteps. Holding the run claim it
drains supersteps in a tight in-process loop — matching the low latency of the
`0.0.x` resident loop — until it reaches a *yield boundary*, then *parks*.
Parking is what bounds how long a vehicle holds the claim and a concurrency
slot (the connection is already released during execution, section 6.1), and it
defines exactly how the run resumes. Every park below **releases the claim and
sets `wake_at`** in one transaction (section 6), so a wake — especially a
signal firing right after a `waiting` park — never has to wait out a stale
claim.

| Yield boundary | When | Park action |
| --- | --- | --- |
| Terminal | run reached `done` / `failed` / `cancelled` | commit terminal checkpoint; `wake_at = NULL` (final) |
| Waiting on interrupt | an interrupt is open and nothing else is runnable | commit `waiting` checkpoint; `wake_at = NULL` — a `:resolve_interrupt` signal sets `wake_at = now` |
| Timer / scheduled wake | a node scheduled a future wake or deadline | commit checkpoint; `wake_at = wake time` |
| Remote node await (post-v1) | a node returned `{:await, …}` pending external completion | commit checkpoint; `wake_at = NULL` — the completion callback sets `wake_at = now` |
| Max drain budget | drained N supersteps or T ms of wall-clock and more work is ready | commit last checkpoint; `wake_at = now` + notify (fairness yield) |
| Retryable failure | a node attempt failed and retry policy allows another | checkpoint the failed attempt (per the core retry contract); `wake_at = backoff time` |

Retry control commits use an explicit `:retry_scheduled` checkpoint type (and
the ordinary `:node_failed` runtime fact); reusing `:step_committed` would be
misleading because the graph step does not advance. A retry park remains graph
status `running`. Only a permanent/exhausted graph failure becomes `failed`
and populates `Docket.Run.failure`.

Retry parking requires durable execution-control state; it is not implemented
by sleeping inside the vehicle. Before `0.1.0`, the core dispatcher is split so
one call executes one attempt. The committed run internals can retain the
active superstep's stable activation identities and snapshots, completed
results/pending writes (still invisible to channels until the barrier), the
next attempt number, accumulated failures, and retry deadline. A retryable
failure checkpoints that control state without incrementing the graph step and
parks at the deadline. Recovery therefore repeats only an attempt that never
committed, with the same idempotency key; it does not reset the node retry
budget or rerun sibling activations whose results were already checkpointed.

This requires adding the currently reserved active-task, pending-write, and
timer fields to the durable run codec. If that work is descoped, durable retry
parking must be removed from `0.1.0` rather than silently falling back to an
in-vehicle sleep with different crash semantics.

Every park action is the same mechanic — one fenced UPDATE choosing the next
`wake_at`. There is no separate "enqueue" step to keep atomic with the
checkpoint, no snooze-on-contention (the dispatcher never dispatches a claimed
run), and no way for a run to park advanced-but-unscheduled.

The drain budget (a superstep count and a wall-clock cap) is what keeps a long
or tight-cycling graph from monopolizing a concurrency slot or holding the run
claim indefinitely. It parks with `wake_at = now` plus a notify, so a budgeted
yield costs one dispatch hop, never lost progress.

### Signal Run

```text
in the caller's process, Docket.Lifecycle.signal(backend, scope, fn tx ->
  Runs.mutate_run(tx, fn current_run ->
    validate signal and produce one pure runtime moment
    increment checkpoint_seq, clear any live claim, and clear current poison
    map the moment's explicit schedule disposition
  end)
  Events.append_events(tx, assigned runtime + :checkpoint_committed events)
end)
Postgres RunStore issued transactional notify for an immediate wake
deliver checkpoint observers and telemetry after commit
return the updated run (or a validation error) to the caller
```

### Recovery

```text
crash mid-drain: the claim expires after orphan_ttl; the ordinary dispatch
  recovery scan claims the run (claim_attempts + 1) and resumes from the last committed
  superstep
crash at a park boundary after commit: wake_at was set in the same
  transaction as the final checkpoint; the run dispatches normally
when the maximum number of claims has already launched without progress, the
  next recovery need marks the run poisoned instead of dispatching,
  so a crash-looping run becomes an explicit operator concern rather than an
  infinite retry
```

There is no reconciler: the dispatch poll's eligibility predicate *is* the
recovery predicate. One knob (`orphan_ttl` plus the poll interval) bounds
recovery latency.

### Terminal Run

```text
commit terminal checkpoint with wake_at = NULL
serve future reads from storage
prune according to policy
```

## 10. Core API Transition

The `0.0.1` production API is:

```elixir
MyApp.Docket.run(graph, input, opts)
MyApp.Docket.resume(graph, run, opts)
MyApp.Docket.get_run(run_id, opts)
MyApp.Docket.resolve_interrupt(run_id, interrupt_id, value, opts)
```

It remains the API of the `0.0.x` release line, not a second mode inside
`0.1.0`. The `0.1.0` facade replaces it with names and semantics that make
backend-owned durable lifecycle explicit:

```elixir
MyApp.Docket.save_graph(graph, opts)
MyApp.Docket.start_run(graph_ref, input, opts)
MyApp.Docket.resolve_interrupt(run_id, interrupt_id, value, opts)
MyApp.Docket.cancel_run(run_id, opts)
MyApp.Docket.retry_poisoned_run(run_id, opts)
MyApp.Docket.fetch_run(run_id, opts)
MyApp.Docket.inspect_run(run_id, opts)
MyApp.Docket.await_run(run_id, opts)
```

The graph signal functions and operational command are synchronous: each
returns the updated run on success and a typed error on validation failure,
preserving the `0.0.x` error-reporting contract (`resolve_interrupt` returns
schema validation errors directly to the caller). There is no generic
`signal/3`; each operation has its own arguments and validation, and the named
functions carry that surface directly (section 8). `resolve_interrupt` keeps
its source-level shape but becomes exclusively storage-backed; it never falls
back to a local Runtime process.

`fetch_run` is a storage read and is the primary read API — it always works,
because a parked run's truth is in the backend. `get_run` is removed: an
uncommitted in-memory snapshot is node-local, timing-dependent, and not a
stable distributed contract. Callers use `fetch_run` for the committed run or
`inspect_run` when they also need operational state. A future live-progress
projection, if justified, must use a distinct name and explicitly weak
consistency contract rather than reviving `get_run`.

`resume` is removed from the production facade. Recovery is backend-owned:
dispatchers reclaim eligible or expired runs from the last committed state.
Applications no longer load a `Docket.Run`, choose a graph, and relaunch a
resident process. `Docket.Test.resume_inline` remains available for
processless graph-semantics tests.

`inspect_run` returns a substrate-neutral `Docket.RunInfo` projection containing
the run plus `wake_at`, `claimed_at` (never the claim token),
`claim_attempts`, and current poison facts. Operational health does not belong
in `Docket.Run`, but it must not be invisible: a poisoned run otherwise looks
graph-semantically `running` while making no progress.

`await_run(run_id, opts)` blocks the caller until the run reaches a terminal
state, parks waiting on input, becomes poisoned, or reaches an explicit
`:timeout`. Waiting/terminal return the run; poison returns a typed operational
halt carrying `RunInfo` rather than timing out mysteriously. `0.1.0` implements
it as bounded polling of `inspect_run` (a `:poll_interval` plus a required
`:timeout`), which is correct in every configuration including poll-only mode.
A `LISTEN`-based fast path is additive. `await_run` exists for tests and
short-lived callers.

Checkpoint observers are best-effort after-commit hooks and may be lost or
duplicated across a process crash. They are not a long-lived delivery guarantee.
Integrations that require durable consumption must enable retained events and
consume/export them by event sequence (or use a future outbox product).
`Docket.Checkpoint` may remain as the committed notification value, but
`checkpoint:` is no longer a persistence or supervision configuration.

### 0.0.1 adopter migration

The break is deliberately narrower than replacing Docket's programming model:

- Node modules and graph definitions carry over unchanged, including
  `Docket.Node`, `Docket.Graph`, `Docket.Schema`, reducers, interrupts, and
  executors.
- `Docket.Test.run_inline` and related processless helpers remain the
  Postgres-free graph-semantics testing surface.
- `Docket.Run.to_map` / `from_map` remain the public wire boundary and are also
  used by backend row codecs.

An adopter performs one explicit cutover:

1. Stop `0.0.1` writers and drain, terminate, or explicitly export any active
   runs. There is no transparent dual-driver period.
2. Remove the host `Docket.Checkpoint` committer and its Docket-specific run /
   checkpoint tables.
3. Install the Docket migration and configure `use Docket` with
   `repo: MyApp.Repo` and `backend: Docket.Postgres`.
4. Publish each effective graph through `save_graph` and retain its
   `Docket.GraphRef`.
5. Replace `run(graph, input)` with `start_run(graph_ref, input)`, `get_run`
   with `fetch_run` or `inspect_run`, and delete application-owned `resume`
   orchestration.
6. Move non-transactional notifications to `checkpoint_observers`; use
   retained events for delivery that must survive crashes.

Because `0.0.1` persistence is host-defined, Docket cannot provide a universal
database migration. The supported migration is drain-and-cut-over. A separate
one-shot conversion helper may translate a latest `0.0.1` run document for an
advanced importer, but it does not preserve the old runtime API or accept old
wire formats indefinitely.

## 11. Required Core Changes For 0.1.0

The core package needs a few seams before the Postgres backend can be excellent.
These seams are substrate-independent: they would be identical under any
durable backend.

- Define one `Docket.Backend` bundle as the configuration/substitution unit.
  It supplies a compatible `Docket.Storage` transaction boundary and focused
  store capabilities. Store modules remain testable ports but are not public
  mix-and-match configuration. A transaction passes one backend context to
  all participating stores, which must join it rather than commit independently:

  - `Docket.Storage.Graphs`: simple, static `save_graph` and `fetch_graph`
    operations for immutable content-addressed graph versions.
  - `Docket.Storage.Runs`: `insert_run`, `fetch_run`, `inspect_run`, atomic
    batched `claim_due`, heartbeat/release, mandatory-token fenced `commit`,
    serialized mutation, scheduling, and poison recovery. This is one
    run-aggregate port because all operations enforce the same row invariant;
    there is no independently configurable `Docket.Coordinator`.
  - `Docket.Storage.Events`: append already-assigned events according to
    persistence policy. The pruner executes retention.

  Required run/event scope is `:system`, `:tenantless`, or `{:tenant, id}`;
  missing options never imply system access. `Docket.Lifecycle` is the single
  named composer for start, moment commit, and signal transactions. This
  preserves atomicity without making a store orchestrate another entity.
- Add first-class `resolve_interrupt` and `cancel_run` pure named run-mutation
  functions: validate against the loaded `Docket.Run`, apply the mutation, and
  return one runtime moment with an explicit schedule disposition. Add `:run_cancelled`
  checkpoint/event types. Keep operational recovery (`retry_poisoned_run`)
  outside `Docket.Run` signal application because it changes backend-owned
  health state. No signal structs and no signal behaviour — the reified
  envelope was queue machinery, and there is no queue (section 8). The
  Postgres backend wraps these functions in synchronous fenced commits. Core
  keeps no queue or scheduling dependency.
- Expose processless initialization and "advance one commit boundary"
  entrypoints that return one `Docket.Runtime.Moment` without storage writes,
  handler delivery, or committed telemetry. The vehicle owns the drain loop;
  `Docket.Lifecycle` commits each moment before asking core for the next. This is
  the seam the dispatcher's vehicle drives;
  `Docket.Test.step_inline` already proves the same loop runs outside a
  GenServer, so backend execution and tests share one interpreter.
- Include superstep/attempt and run identity in checkpoint context, so the
  backend can make commit-and-schedule atomic and recovery idempotent.
- Make moment calculation independent of checkpoint delivery. The operational
  driver wins the storage commit, then creates/delivers the committed
  checkpoint and telemetry. Only separately configured, best-effort
  `checkpoint_observers:` remain; their failures cannot veto state.
- Allocate one `:checkpoint_committed` event from `Run.event_seq` for every
  moment, independently of `checkpoint_seq`; add `:retry_scheduled` for retry
  control commits.
- Add the five durable graph statuses, JSON-safe terminal `Run.failure`, and
  `Docket.RunInfo` operational projection. Keep `:created` transient only.
- Persist active-superstep retry control state and replace recursive
  sleep-and-retry dispatch with one-attempt execution plus retry parking.
- Apply signals on the same step boundary as superstep progress.
- Make storage-backed reads distinct from live process reads.
- Preserve inline testing without requiring Postgres.
- Require one backend for every supervised production instance; remove
  `run`, `resume`, `get_run`, the `checkpoint:` committer configuration, and
  the storage-free per-run GenServer/registry driver from the v0.1.0 public
  facade once the Postgres vehicle, assembly, and deterministic backend test
  modes are complete.
- Require exact sequence progression and a non-nil current claim token for
  every advance commit. Serialized mutation is the only unclaimed update path.

The goal is not to move Postgres code into core. The goal is to let the core
runtime be owned by a durable backend without weakening the execution model.

## 12. `Docket.Postgres` MVP

The 0.1.0 implementation should include:

- `mix docket.gen.migration` or documented migration copy path.
- Migrations for runs, graph versions, and events.
- `Docket.Postgres` implementing the backend bundle and supervision boundary;
  `Docket.Postgres.Storage` implementing the shared transaction boundary, with
  `Docket.Postgres.GraphStore`, `Docket.Postgres.RunStore`, and
  `Docket.Postgres.EventStore` implementing the three store behaviours against
  its transaction context. The run row codec maps promoted columns plus
  `state` to `Docket.Run`. `GraphStore` returns canonical effective documents
  only; vehicles own node-local compilation and may use a release-scoped cache.
- An explicit `save_graph` facade that materializes node configuration
  defaults, validates/compiles, and stores the canonical content-addressed
  effective graph document, returning a stable reference.
- `Docket.Lifecycle` start orchestration that composes `Runs.insert_run` and
  `Events.append_events` inside one storage transaction, inserting the
  initialized run, assigned initialization and `:checkpoint_committed` events,
  and wake atomically. Lifecycle never publishes a graph document.
- RunStore atomic batched `claim_due` for separately ordered ready and expired
  paths, including a fenced candidate LIMIT, combined demand bound,
  claim-attempt accounting, poison disposition, refresh, and token-guarded
  release. Advance storage commits require both
  `checkpoint_seq` and `claim_token`; the connection is released before node
  execution.
- The checkpoint commit path composes `Runs.commit` and
  `Events.append_events` inside one storage transaction, persisting
  the metadata-only `:checkpoint_committed` event and retained runtime events atomically with
  the run row and disposition. The event store owns the policy to persist all,
  none, or selected event types.
- `Docket.Postgres.Dispatcher` (per-node demand over RunStore `SKIP LOCKED`
  claims, bounded jittered scheduled polling, a coalesced `LISTEN/NOTIFY` fast
  path with a supported poll-only configuration for transaction-pooled
  environments, per-node concurrency demand, poison marking at claim time,
  graceful shutdown drain).
- Vehicle supervision (a Task-per-drain shell over the processless loop).
- Durable active-superstep/retry state and one-attempt dispatch so retry
  backoff parks without resetting attempt budgets or rerunning committed sibling
  results.
- Synchronous signal application through a short serialized row mutation,
  returning the committed result to the caller; plus the separate
  `retry_poisoned_run` operational command.
- Claim freshness policy for long single-superstep execution: either strict
  timeout alignment (`node timeout < orphan_ttl`) or a token-guarded lightweight
  claim heartbeat while awaiting node results.
- `Docket.Postgres.Pruner` (periodic, idempotent pruning of events before their
  run retention cap and terminal runs with cascading remaining events; deletes a graph version only when no
  retained run references it, backed by the composite foreign key).
- Documented operational introspection queries (runnable backlog, stale
  claims, poisoned runs, oldest due wake) — the psql-level story that precedes
  `docket_dashboard`.
- Telemetry events for dispatch poll/claim/steal, claim release, checkpoint
  persist, superstep advance, signal application, poison marking, and prune.
- Oban-shaped testing modes, because a background dispatcher claiming rows
  fights the SQL sandbox by design: `testing: :inline` advances runs
  synchronously in the caller's process with no dispatcher, and
  `testing: :manual` plus `Docket.Postgres.Testing.drain_runs/1` advances due
  runs deterministically inside the test's sandboxed transaction. Each runtime
  moment still crosses the production `Storage.transaction/2` boundary; inline
  mode must not wrap node execution or a whole drain in one Docket transaction
  (the SQL sandbox may itself own the outer test connection).
  `Docket.Test.run_inline` continues to cover graph semantics with no
  database at all.

## 13. Non-Goals For 0.1.0

Do not require:

- Distributed Erlang across regions.
- Swarm or Horde, or any cluster-wide process registry for run ownership.
- Resident per-run processes or run-to-node affinity in the operational backend.
- Concurrent mutation of one run.
- Perfect exactly-once external effects.
- Full backend parity beyond Postgres.
- A dashboard.
- A general-purpose job queue. The Postgres backend schedules graph runs, not
  arbitrary jobs; hosts that need a job queue should run Oban beside Docket,
  and the two coexist without touching each other.

Design for those spaces, but ship the durable Postgres path first.

## 14. Milestones

### Milestone A - Reframe Current Release

- Set current package version to `0.0.1`.
- Document that `0.0.x` is the core runtime line.
- Preserve the `0.0.1` host-owned lifecycle as release-history and migration
  documentation, not as a `0.1.0` runtime mode.

### Milestone B - Core Operational Seams

Substrate-independent.

- Introduce one backend bundle plus transaction, graph, run-aggregate, and
  event store capability contracts, with a concurrency-safe in-memory
  conformance backend. Store capabilities are focused but not independently
  configurable across incompatible transaction contexts.
- Add explicit scope (`:system | :tenantless | {:tenant, id}`) and the named
  `Docket.Lifecycle` composition owner.
- Define `Docket.Runtime.Moment`; separate moment calculation from committed
  checkpoints and best-effort durable observers.
- Introduce pure named `resolve_interrupt` / `cancel_run` functions with no
  signal structs or behaviour, including `:run_cancelled` checkpoint/event
  facts and storage-backed serialized mutation semantics.
- Add processless initialization and advance-one-moment entrypoints. The
  vehicle, not core, owns the drain loop.
- Add superstep/attempt context to checkpoints for atomic commit-and-schedule.
- Add the five durable statuses, terminal failure payload, RunInfo inspection,
  `:checkpoint_committed` event, and explicit `:retry_scheduled` checkpoint.
- Make active-superstep results, retry attempts, pending writes, and retry
  timers durable; change the dispatcher from recursive retry to one attempt per
  invocation.
- Add storage-backed `fetch_run` / `inspect_run` semantics at the facade layer.
- After the operational replacement is complete, remove the host-owned
  supervised driver and require a backend at supervised startup.

### Milestone C - `Docket.Postgres` MVP

- Add the three-table migration and Postgres storage (including `wake_at`,
  `claim_attempts`, poison/failure facts, run-to-graph and event-to-run foreign
  keys, lifecycle CHECK constraints, and metadata-only checkpoint history events).
- Implement the Postgres transaction boundary and graph, run, and event stores.
- Publish and verify the effective canonical graph before run creation,
  rejecting a graph key whose existing document differs. Then compose
  initialized run insertion, initial wake, and initialization-event append in
  one lifecycle transaction; failure leaves the prepublished graph unchanged.
- Implement the run claim with fenced, stably ordered, demand-limited candidate
  selection over the two baseline ordered partial indexes; implement the
  optimistic commit fence, atomic commit-and-schedule, and claim release on
  fence loss.
- Compose the run-store commit and event-store append inside one storage
  transaction so run/events/disposition remain atomic; invoke handlers and
  telemetry only after a successful commit.
- Implement the dispatcher: `SKIP LOCKED` claiming, bounded jittered polling,
  coalesced `LISTEN/NOTIFY` fast path, per-node concurrency, shutdown drain.
- Implement synchronous signals through a short serialized row transaction and
  implement the separate `retry_poisoned_run` operational command.
- Define and test exact poison marking via `claim_attempts`.
- Implement the pruner so graph versions survive every referencing retained
  run.
- Add telemetry and the release-gate failure-mode matrix for transaction
  rollback, claim steal, fence races, bounded claim selection under concurrent
  dispatchers and tied timestamps, retry recovery, poison, notification loss
  and bursts, jittered poll scheduling, tenancy, pruning, query-plan shape, and
  database invariants.
- Implement the `:inline` and `:manual` testing modes with `drain_runs/1`.
- After backend assembly and those deterministic modes land, execute DCKT-37:
  require a backend at supervised startup and remove the `0.0.1` host-owned
  `run` / `resume` / live `get_run` / `checkpoint:` production driver before
  the final release-gate suite.
- Provide install and supervision docs plus the introspection query guide.

### Milestone D - Multi-Region

- Run dispatchers in multiple regions against the same store.
- Add read-locality options.
- No process routing: correctness stays with the store, the run claim, and the
  commit fence.

### Milestone E - Enterprise Hardening

- Add tenant quotas and concurrency caps.
- Add advanced retention tiers/export, workload quotas, and richer operational
  policy beyond the v0.1 release gates.
- Add dashboard or dashboard-ready read models.

## 15. Success Criteria

The transition succeeds when an adopter can:

- Install one `docket` dependency; core retains only its telemetry dependency,
  while the Postgres backend compiles when the host supplies optional
  `ecto_sql` and `postgrex`.
- Run migrations.
- Add one supervised Docket module.
- Define nodes and graphs.
- Start, resolve, cancel, fetch, inspect, and await runs without writing custom
  claim or routing logic. Generic resume and graph-semantic failed-run retry
  remain deferred.
- Call `cancel_run` and get back a confirmed cancelled run, synchronously.
- Fetch a failed run and retain its structured terminal failure even when
  event persistence is disabled.
- Inspect a poisoned run and have `await_run` report the operational halt
  instead of silently timing out; recover it explicitly.
- Kill a worker mid-superstep and watch any other node's dispatcher pick the
  run up from the last checkpoint within `orphan_ttl` plus one poll interval.
- Trust that two workers cannot both commit progress for one run, even if a
  stale claim briefly lets both execute.
- Rely on database constraints to reject a durable `created`, terminal-with-wake,
  unscoped tenant fallthrough, or running-but-neither-scheduled-nor-claimed row.
- Test graph semantics without Postgres and test operational semantics
  deterministically via the `:inline` / `:manual` testing modes under the
  Ecto sandbox.
- Migrate a `0.0.1` application without rewriting its nodes, graphs, schemas,
  reducers, executors, inline semantic tests, or run serialization; only the
  lifecycle ownership and its call sites change.
- Fail supervised startup without a configured backend; expose no `run`,
  `resume`, `get_run`, or host-owned `checkpoint:` production path in
  `0.1.0`.

The package should feel boring to operate. That is the bar.
