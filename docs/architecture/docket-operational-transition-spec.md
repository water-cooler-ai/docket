# Docket Operational Transition Spec

Status: transition spec (rev 3 — single package with optional Ecto deps, optional tenancy, metadata-only checkpoint history)
Date: 2026-07-08
Target: move from core runtime library to an Oban-shaped durable runtime

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

This is a package-shape and operational-contract decision. It does not change
the core graph execution invariant:

```text
One active graph run has one live mutator.
```

The invariant is unchanged, but its enforcement moves. In `0.0.x` a resident
`Docket.Runtime` process is the single writer for a run's whole lifetime. In the
operational backend the mutator becomes momentary: a stateless worker takes a
lightweight run claim, advances the run, checkpoints under an optimistic commit
fence, and releases. At any instant at most one worker is advancing a run, so
"one live mutator" still holds — it is just placeless and short-lived instead of
a long-lived process pinned to a node.

The substrate is deliberately minimal: the Postgres backend ships inside the
`docket` package behind optional `ecto_sql`/`postgrex` dependencies (section
4), so the paved road is one line in `deps`, exactly as Oban is. **The run
row is the queue.** A run carries its own schedule (`wake_at`), its own
execution ownership (`claim_token` / `claimed_at`), and its own operational
attempt counter. A per-node dispatcher polls for runnable runs with
`FOR UPDATE SKIP LOCKED`, claims one, and drives it through supersteps until
a yield boundary, then parks. Crash recovery is a claim expiring and the next
poll picking the run up. There is no job table beside the run table, so there
is nothing to reconcile: a run cannot be "runnable but unscheduled," because
the schedule is a column on the run itself. None of this coordination is
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

It should include:

- A first-class `Docket.Postgres` backend inside the `docket` package,
  enabled by optional `ecto_sql` and `postgrex` dependencies — one package to
  install, no version pair to manage.
- Ecto migrations for Docket-owned operational tables.
- Durable run store and graph store.
- Per-run claim and optimistic commit fence for single-writer.
- Synchronous, fenced signal application with confirmed results.
- Checkpoint and event persistence.
- Run scheduling via `wake_at` on the run row and a `SKIP LOCKED` dispatcher.
- Crash recovery via claim expiry — the dispatch poll is the recovery path.
- Optional tenant scoping on run APIs — no tenant concept required to adopt.
- Stateless workers with no run-to-node affinity.

The 0.1.0 user experience should be closer to installing Oban than to wiring a
set of low-level behaviours manually.

## 3. North Star

The host app should feel like this:

```elixir
defmodule MyApp.Docket do
  use Docket,
    repo: MyApp.Repo,
    storage: Docket.Postgres,
    concurrency: 50
end
```

`storage: Docket.Postgres` is self-contained: no Oban, no extra queue
framework, no second package, no second supervision story to configure. `concurrency` is the
dispatcher's per-node limit on concurrently advancing runs — the one knob for
how much work this node takes. Because the database connection is released
during node execution (section 6.1), that limit can be set high without pool
pressure. Per-workload isolation — routing a browser node's work separately,
capping llm work independently — is a post-v1 concern that arrives with
`Docket.Executor.Queue`, not something `0.1.0` needs.

Application code defines nodes and graphs, then starts or signals runs:

```elixir
{:ok, run} =
  MyApp.Docket.start_run(graph, input,
    tenant_id: account.id,
    metadata: %{"workflow_id" => workflow.id}
  )

{:ok, run} =
  MyApp.Docket.signal(run.id, :resolve_interrupt,
    tenant_id: account.id,
    interrupt_id: interrupt_id,
    value: "approved"
  )
```

Signals are synchronous: the call applies the state change under the commit
fence in the caller's process and returns the updated run (or a validation
error) directly. `cancel_run` returns a confirmed cancellation, not an
"accepted" acknowledgment.

`tenant_id` is optional here and everywhere: hosts without a tenant concept
omit it and never see it again (section 5). Passing it scopes reads and
signals to that tenant.

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
- Supervised runtime process (the in-process, Postgres-free driver).
- Processless loop driver that a stateless worker can call.
- Inline test runtime.
- Storage, coordinator, and signal behaviour contracts.

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
- Run store.
- Graph store.
- Checkpoint and event store.
- Per-run claim, optimistic commit fence, and atomic commit-and-schedule.
- The dispatcher: `SKIP LOCKED` claim polling, a `LISTEN/NOTIFY` fast path,
  per-node concurrency, and graceful shutdown draining.
- Synchronous signal application.
- Operational attempt counting and poison-run marking.
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
docket_graphs
docket_graph_versions
docket_runs
docket_checkpoints
docket_events
```

There is no signal table and no job table. Signals are synchronous fenced
commits (section 8), and the run row itself carries the schedule. The exact
naming of the Docket tables can change, but the ownership concepts should not.

`docket_graphs` and `docket_graph_versions` exist because recovery is
autonomous: a worker picking up a recovered run on another node must be able to
load the exact graph content by `graph_id + graph_hash` with no host call in
the loop. Graph content that a non-terminal run still references must therefore
outlive pruning: the pruner must never delete a graph version an active run
still points at.

Publishing is implicit and content-addressed. `start_run(graph, input)`
upserts the compiled document keyed by `graph_id + graph_hash` (the hash
already exists in core) with `ON CONFLICT DO NOTHING`, in the same
transaction that inserts the run row. Two nodes racing to publish the same
version both succeed idempotently — content addressing makes the row
byte-identical, so there is nothing to merge. No separate publish workflow is
required for `0.1.0`; an explicit publish/version-management API can arrive
with graph tooling later without changing this contract.

### `docket_runs`

Required concepts:

```text
tenant_id
run_id
graph_id
graph_hash
status
operational_status
operational_error
docket_run
metadata
current_superstep
latest_checkpoint_seq
latest_checkpoint_type
claim_token
claimed_at
wake_at
attempts
inserted_at
updated_at
finished_at
```

There is no owner node, no lease epoch, and no companion job row. Three column
groups carry the whole operational model:

- **Schedule — `wake_at`.** The single source of truth for when the run should
  next advance. `now` means runnable, a future instant means a timer or retry
  backoff, and `NULL` means parked with an external wake source (an open
  interrupt, a remote completion) or terminal. A run cannot be runnable and
  unscheduled: the schedule is a column on the run.
- **Execution ownership — `claim_token` / `claimed_at`.** Single-writer is a
  claim plus an optimistic fence on `latest_checkpoint_seq` at commit — not a
  held connection and not a correctness-bearing expiry (see section 6).
  `claimed_at` lets the dispatcher tell whether the current holder is still
  alive.
- **Operational health — `attempts`, `operational_status`,
  `operational_error`.** `attempts` increments when a claim is won and resets
  to zero on any successful fenced commit, so it counts consecutive
  crashes-without-progress. A run whose `attempts` reaches the configured
  maximum is marked `poisoned` instead of dispatched again. `status` remains
  the graph-run status visible in `Docket.Run`; operational exhaustion lives
  separately in `operational_status` / `operational_error` so a poisoned run
  cannot sit graph-semantically `running` while silently stranded.
  `operational_status` defaults to `active`; `blocked` / `poisoned` stop
  automatic dispatch and make operator intervention explicit. An operator (or
  a `retry_failed` / `resume_run` signal) recovers a poisoned run by resetting
  `operational_status` to `active`, `attempts` to zero, and `wake_at` to now.

**Tenancy is optional.** Runs are keyed by `run_id` alone; `tenant_id` is a
nullable, indexed scoping column. Nothing in the claim/fence design needs a
tenant, and requiring every adopter to have a tenant concept before starting
a run is adoption friction Oban never imposes. Hosts that pass `tenant_id`
get scoped reads and signals — a tenant mismatch reads as `:not_found` — and
hosts that don't never see the concept.

`current_superstep` is the resume point a recovered run drives from.

Important indexes:

```text
unique (run_id)
partial (tenant_id, status) WHERE tenant_id IS NOT NULL
partial (tenant_id, graph_id, status) WHERE tenant_id IS NOT NULL
partial (wake_at) WHERE wake_at IS NOT NULL      -- the dispatch scan
partial (operational_status) WHERE operational_status <> 'active'
(status, updated_at)                              -- ops introspection
```

The dispatch scan reads only rows with a non-null `wake_at`, which excludes
terminal and externally-parked runs structurally.

### State size and write amplification

A run document is not a job row. Oban args are small; `docket_run` carries
every channel value, and for the target workload — agentic LLM sessions —
that can be megabytes of transcript rewritten at every superstep commit. The
hot path must not multiply copies of that document:

- **The run row holds the only full document.** Recovery needs exactly the
  latest committed document, and it lives in `docket_runs.docket_run`. No
  other table stores a full run snapshot.
- **`docket_checkpoints` is metadata-only.** A checkpoint row records seq,
  type, step, park action, and timestamps — never the run document.
  Checkpoint history is audit and observability data; storing
  O(supersteps × state size) snapshots for it would be the hidden cost that
  melts an adopter's database. If replay or time-travel debugging ever
  becomes a goal, full-snapshot retention becomes an explicit opt-in policy
  at that point.
- **Event persistence is a policy, not a given.** Events default to on, but
  `0.1.0` ships the volume knob (persist all, none, or selected types),
  because turning event volume down is the first request every high-volume
  adopter makes.

The remaining cost — TOAST churn and WAL volume from rewriting one large
document per superstep — is bounded per dispatch by the drain budget.
Per-channel or delta storage is a possible post-v1 optimization; it must not
change the correctness story (one fenced, single-document commit).

## 6. Coordination And Single-Writer Commits

### Execution model: state, schedule, vehicle

The backend separates three roles that a resident process previously fused:

- **State — Postgres.** The durable run (`docket_runs.docket_run`, checkpoints)
  is the source of truth. A parked run lives entirely here and needs no process.
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
on the next wake — demand paging for graph runs. "One live mutator" still
holds: the vehicle is that mutator, the claim guarantees only one exists at a
time, and it is short-lived rather than resident. Because `Docket.Runtime.Loop`
is processless (section 4), the vehicle is just a third shell over it alongside
the GenServer and the inline test runtime.

The vehicle is a local, ephemeral compute resource, never a unit of addressing.
It runs on whichever node's dispatcher claimed the run, has no home node, and
nothing outside needs to find it. Correctness lives entirely in the state and
the schedule; the process is disposable. An optional per-node registry can make
an *active* vehicle locally reachable for fast reads (see sections 7 and 10),
but that is a latency optimization, never a correctness path.

### The dispatcher

Each node runs one dispatcher (under the backend supervision tree that
`MyApp.Docket` starts). Its loop is deliberately small:

```sql
-- claim up to $demand due runs in one statement
UPDATE docket_runs
SET claim_token = $new_token,
    claimed_at = now(),
    attempts = attempts + 1
WHERE run_id IN (
  SELECT run_id FROM docket_runs
  WHERE wake_at IS NOT NULL
    AND wake_at <= now()
    AND status NOT IN ('done', 'failed', 'cancelled')
    AND operational_status = 'active'
    AND (claim_token IS NULL OR claimed_at < now() - $orphan_ttl)
  ORDER BY wake_at
  LIMIT $demand
  FOR UPDATE SKIP LOCKED
)
RETURNING *
```

`$demand` is `concurrency` minus in-flight vehicles on this node. Rows whose
`attempts` now exceed the configured maximum are marked `poisoned` (and their
claim released) instead of being handed to a vehicle. Everything else becomes
a vehicle.

Two wake paths feed the loop:

- **Poll.** A short interval (default around one second) covers scheduled
  wakes, expired claims, and any lost notification. The poll *is* the recovery
  path: there is no separate reconciler, because an eligible row and a due
  `wake_at` are the same thing.
- **Notify.** Any transaction that sets `wake_at` to now — a park scheduling an
  immediate successor, a signal making a run runnable — also issues a
  `pg_notify`. Dispatchers listen and poll immediately, so the common
  park-to-resume hop is milliseconds, not a poll interval. Notifications are a
  latency optimization only; the poll guarantees progress without them.

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
- **Execution ownership (long).** While a worker is driving a superstep —
  including a multi-minute LLM or browser node — others should not also drive
  it. This can span slow external I/O, so it must not hold a database connection.

**Single committer is optimistic, not locked.** Every state mutation commits
with a conditional UPDATE fenced on the monotonic `latest_checkpoint_seq` — no
long-held lock, only the row lock intrinsic to the write. Advance-worker commits
also fence on their `claim_token`; signal commits intentionally do not. That
keeps cancellation and interrupt resolution timely while preventing a stale
advance worker from refreshing or clearing a claim it no longer owns:

```sql
-- one short transaction per advance-worker superstep
UPDATE docket_runs
SET docket_run = $run,
    status = $status,
    current_superstep = $next_superstep,
    latest_checkpoint_seq = $seq + 1,
    latest_checkpoint_type = $type,
    attempts = 0,                  -- any committed progress proves health
    claimed_at = now(),            -- mid-drain: refresh
    -- at a park: claim_token = NULL, wake_at = $next_wake_or_null
    updated_at = now()
WHERE run_id = $run_id
  AND latest_checkpoint_seq = $seq
  AND claim_token = $claim_token;

-- signal commits use the same sequence fence but omit claim_token:
-- they may cancel or resume a run while an advance worker is in flight
```

If this affects zero rows, someone else already committed past `$seq`, the
worker lost its claim, or a signal changed the run first. An advance worker
discards its uncommitted work, **releases its claim, and stops**. The release
is its own small UPDATE fenced on `claim_token` alone (it does not touch
`latest_checkpoint_seq` or `wake_at`, which the winning commit already set
correctly):

```sql
UPDATE docket_runs
SET claim_token = NULL, updated_at = now()
WHERE run_id = $run_id AND claim_token = $claim_token
```

The same release runs when a claimed run turns out not to be runnable. This
matters: a signal that cancels or resumes a run mid-drain schedules the next
wake immediately, and without the release that wake would stall behind a claim
whose holder has already given up, waiting out `$orphan_ttl` for no reason. The
fence is checked only at commit time; nothing is held during execution.

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
happens after a crash.

`$orphan_ttl` is a **liveness hint, not a correctness mechanism.** Set it above
the vehicle drain budget plus expected clock skew and shutdown delay; with the
heartbeat option, the ttl bounds how long recovery waits after the last
successful refresh. If the ttl were wrong, the worst case is two workers briefly
executing the same superstep — the advance fence lets only the current claim
holder commit, the sequence fence lets only one state mutation win, and the core
idempotency-key invariant (keys derive only from committed superstep/attempt)
dedupes any external effect from the loser. Clock skew can cost duplicated,
deduped work; it can never cause a double commit. This is the same safety
posture as the resident model's split-brain window, with no owner node and no
lease epoch.

The atomic commit-and-schedule happens at a park boundary (section 9). The
Postgres backend, not the core loop, owns that transaction: it persists the
proposed checkpoint/events, updates the run row under the fence, releases the
claim, and sets the next `wake_at` as one database operation — followed by a
`pg_notify` when the wake is immediate. Within a drain, each superstep
checkpoints on its own, and the in-process loop is the "next" — the live vehicle
is itself the schedule. The invariant: a run is always either terminal, parked
with an explicit wake source (`wake_at` or an external event that will set it),
or claimed by a live vehicle. It never sits advanced with no way to resume —
and unlike a job-table design, this is enforced by the shape of the data, not
by a reconciler. This also makes step persistence effectively synchronous
regardless of the core's `:step_committed` async hint: the worker durably
commits each superstep before continuing, so execution never runs ahead of
persistence.

Advancing is idempotent because a vehicle drives from committed state, not a
fixed target. A recovered run — its vehicle crashed mid-drain — is re-claimed
and resumes from the last committed superstep; there is nothing stale to skip.
A claimed run that turns out not to be runnable (already terminal, or parked
waiting for input) releases its claim without driving. A crash before a
superstep commits re-executes only that superstep, and the idempotency-key
invariant dedupes its external effects.

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
run claim + commit fence = single-writer guarantee
wake_at + dispatcher = where the next unit of work waits for any free node
```

Request flow:

```text
authenticate the caller (tenancy, when used, is a scoping filter)
load run row by run_id (scoped by tenant_id when the caller passes one)
if terminal, serve from storage
otherwise apply the signal synchronously under the fence and return the result
```

A read (`fetch_run`) is a storage read. A state change (`signal`) is a fenced
commit executed in the caller's process. Neither needs to know which node, if
any, is currently advancing the run. Dispatchers across any number of nodes —
or regions, pointed at the same database — pull work; correctness comes from
the claim, the commit fence, and the store, never from process lookup.

One run is advanced by one worker at a time. Many runs advance concurrently
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

## 8. Signals

State-changing public APIs (`signal/2` and friends) are synchronous fenced
commits. There is no signal queue, no signal worker, and no "accepted"
acknowledgment: the caller gets the actual result.

Initial signal types:

- `:resolve_interrupt`
- `:cancel_run`
- `:resume_run`
- `:retry_failed`

Signal application rules:

- Signals are addressed by `run_id`; a `tenant_id`, when passed, scopes the
  lookup, and a mismatch returns `:not_found`.
- A signal executes in the caller's process as one short transaction: load the
  run row, validate the signal against it (schema validation for interrupt
  resolutions, state checks for cancel/resume), apply the mutation through the
  same fenced commit as ordinary superstep progress — but without needing to
  hold the run claim — and set `wake_at = now` (plus `pg_notify`) in that same
  transaction when the signal makes the run runnable.
- Validation errors return synchronously to the caller, exactly as in the
  `0.0.x` API. A bad interrupt value is a `{:error, ...}` return, not a
  dead-lettered job the caller never sees.
- `cancel_run` is a confirmed, synchronous cancel: it commits `cancelled` under
  the fence and returns the cancelled run. An in-flight advance's next commit
  then fails the fence and discards its work — cancellation never waits for
  the drain to notice.
- A signal that races an in-flight advance commit and loses the fence re-reads
  the new state and retries in a short bounded loop (the window is one
  superstep commit, milliseconds). If the signal is no longer meaningful after
  the re-read — the run completed, the interrupt was resolved — it returns the
  appropriate result instead.
- The v1 signal types are naturally idempotent state transitions: resolving an
  already-resolved interrupt, cancelling an already-cancelled run, and resuming
  an already-running run each return a well-defined result on repeat calls.
  There is no retention-window idempotency contract because there is no queue.
  If a post-v1 signal type ever needs cross-request exactly-once semantics, a
  small receipts table keyed by `tenant_id + idempotency_key` can be added
  then; nothing in v1 requires it.

Durability is trivial: a signal is durable when its transaction commits, which
is before the call returns. What the earlier design achieved with "enqueue and
return accepted," this design achieves with "commit and return the result" —
strictly stronger.

## 9. Runtime Lifecycle

### Start Run

```text
validate tenant and graph access
in one transaction: insert run row with wake_at = now,
  emit :run_initialized checkpoint
pg_notify dispatchers
return initialized run
```

### Advance (one dispatch cycle)

```text
the dispatcher claims a due run (short transaction; attempts + 1)
  if attempts exceeded the maximum, mark poisoned and release instead
release the connection; load committed Docket.Run and the cached compiled graph
if the run is not runnable (terminal, or waiting with no input),
  release the claim and stop
drain supersteps, checkpointing each under the advance fence, until a yield
boundary
  each mid-drain commit refreshes the claim and resets attempts
  a failed fence means discard, release the claim, and stop
  a single long superstep either fits inside orphan_ttl or refreshes the claim
at the boundary, in one transaction: final checkpoint, release the claim,
  set the park's wake_at (or NULL)
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
in the caller's process, one short transaction:
  load run row, validate signal, apply mutation under the sequence fence
  set wake_at = now in the same transaction if the run became runnable
pg_notify dispatchers
return the updated run (or a validation error) to the caller
if it raced an in-flight advance commit, re-read and retry briefly
```

### Recovery

```text
crash mid-drain: the claim expires after orphan_ttl; the ordinary dispatch
  poll claims the run (attempts + 1) and resumes from the last committed
  superstep
crash at a park boundary after commit: wake_at was set in the same
  transaction as the final checkpoint; the run dispatches normally
attempts reaching the maximum marks the run poisoned instead of dispatching,
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

The current API:

```elixir
MyApp.Docket.run(graph, input, opts)
MyApp.Docket.resume(graph, run, opts)
MyApp.Docket.get_run(run_id, opts)
MyApp.Docket.resolve_interrupt(run_id, interrupt_id, value, opts)
```

Should remain available for `0.0.x` core usage.

The `0.1.0` operational API should introduce names that make durable lifecycle
clear:

```elixir
MyApp.Docket.start_run(graph, input, opts)
MyApp.Docket.signal(run_id, signal, opts)
MyApp.Docket.fetch_run(run_id, opts)
MyApp.Docket.await_run(run_id, opts)
```

`signal/3` is synchronous: it returns the updated run on success and a typed
error on validation failure, preserving the `0.0.x` error-reporting contract
(`resolve_interrupt` returning schema validation errors directly to the
caller).

`fetch_run` is a storage read and is the primary read API — it always works,
because a parked run's truth is in Postgres. `get_run` is the optional live read:
it succeeds only when a vehicle is actively draining the run and is reachable
through the per-node fast path (section 7), returning the in-memory snapshot,
which may be a superstep or two ahead of the last durable checkpoint. It returns
`:not_found` whenever no local vehicle is active — a parked run, or a vehicle on
another node — and callers fall back to `fetch_run`. So `get_run` is a
best-effort freshness optimization, not a contract; `0.1.0` may implement it as
always-`:not_found` (no registry) and add the live path later. It also remains
the in-process read for the Postgres-free GenServer driver.

`await_run(run_id, opts)` blocks the caller until the run reaches a terminal
state or parks waiting on input, or until an explicit `:timeout` elapses —
whichever comes first — and returns the run as of that boundary. `0.1.0`
implements it as bounded polling of `fetch_run` (a `:poll_interval` plus a
required `:timeout`), which is correct in every configuration including
poll-only mode. A `LISTEN`-based fast path on run-lifecycle notifications is
an additive latency optimization with the same contract. `await_run` exists
for tests and short-lived callers; checkpoints remain the integration surface
for anything long-lived.

## 11. Required Core Changes For 0.1.0

The core package needs a few seams before the Postgres backend can be excellent.
These seams are substrate-independent: they would be identical under any
durable backend.

- Define storage and coordination behaviours.
- Add first-class signal structs and signal application APIs. Core defines the
  signal shape and the application seam; the Postgres backend applies signals
  as synchronous fenced commits. Core keeps no queue or scheduling dependency.
- Expose a processless "advance one superstep, or drain to a yield boundary"
  entrypoint that a stateless worker can call, returning proposed checkpoint(s),
  events, and the park action without performing durable storage writes itself.
  The Postgres backend then commits those effects and the next `wake_at` in one
  transaction. This is the seam the dispatcher's vehicle drives;
  `Docket.Test.step_inline` already proves the same loop runs outside a
  GenServer, so this is a third driver, not a second interpreter.
- Include superstep/attempt and run identity in checkpoint context, so the
  backend can make commit-and-schedule atomic and recovery idempotent.
- Apply signals on the same step boundary as superstep progress.
- Make storage-backed reads distinct from live process reads.
- Preserve inline testing without requiring Postgres.
- Keep the current checkpoint callback usable for custom backends.

The goal is not to move Postgres code into core. The goal is to let the core
runtime be owned by a durable coordinator without weakening the execution model.

## 12. `Docket.Postgres` MVP

The 0.1.0 implementation should include:

- `mix docket.gen.migration` or documented migration copy path.
- Migrations for runs, graph versions, checkpoints, and events.
- `Docket.Postgres.RunStore`.
- `Docket.Postgres.GraphStore` (content-addressed publish-on-start upsert,
  section 5).
- `Docket.Postgres.Coordinator` (run claim win/refresh/steal, claim release on
  fence loss or non-runnable claim, and the commit fence; advance commits
  require both `latest_checkpoint_seq` and `claim_token`; releases the
  connection before node execution).
- `Docket.Postgres.CheckpointStore` (fenced checkpoint metadata + `wake_at`
  in one transaction; checkpoint rows are metadata-only per section 5).
- Event persistence policy (persist all, none, or selected event types).
- `Docket.Postgres.Dispatcher` (per-node `SKIP LOCKED` claim polling,
  `LISTEN/NOTIFY` fast path with a supported poll-only configuration for
  transaction-pooled environments, per-node concurrency demand, poison
  marking at claim time, graceful shutdown drain).
- Vehicle supervision (a Task-per-drain shell over the processless loop).
- Synchronous signal application (fenced, claim-free commits with bounded
  fence-race retry, returning results to the caller).
- Claim freshness policy for long single-superstep execution: either strict
  timeout alignment (`node timeout < orphan_ttl`) or a token-guarded lightweight
  claim heartbeat while awaiting node results.
- `Docket.Postgres.Pruner` (periodic, idempotent pruning of terminal runs,
  checkpoints, and events per policy; never prunes a graph version an active
  run references).
- Documented operational introspection queries (runnable backlog, stale
  claims, poisoned runs, oldest due wake) — the psql-level story that precedes
  `docket_dashboard`.
- Telemetry events for dispatch poll/claim/steal, claim release, checkpoint
  persist, superstep advance, signal application, poison marking, and prune.
- Oban-shaped testing modes, because a background dispatcher claiming rows
  fights the SQL sandbox by design: `testing: :inline` advances runs
  synchronously in the caller's process with no dispatcher, and
  `testing: :manual` plus `Docket.Postgres.Testing.drain_runs/1` advances due
  runs deterministically inside the test's sandboxed transaction.
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
- Keep host-owned persistence and tenancy in the README accurate.

### Milestone B - Core Operational Seams

Substrate-independent; unchanged from rev 1 of this spec.

- Introduce storage/coordinator behaviour contracts.
- Introduce signal structs and storage-backed signal semantics.
- Add a processless advance entrypoint (advance one superstep or drain to a
  yield boundary) the worker driver can call, returning proposed durable effects
  rather than committing them itself.
- Add superstep/attempt context to checkpoints for atomic commit-and-schedule.
- Add storage-backed `fetch_run` semantics at the facade layer.

### Milestone C - `Docket.Postgres` MVP

- Add migrations and Postgres-backed stores (including `wake_at` and
  `attempts` on `docket_runs`; checkpoint rows metadata-only).
- Implement publish-on-start graph version upsert.
- Implement the run claim, optimistic commit fence, atomic
  commit-and-schedule, and claim release on fence loss.
- Persist checkpoints/events.
- Implement the dispatcher: `SKIP LOCKED` claiming, `LISTEN/NOTIFY` fast path,
  per-node concurrency, shutdown drain.
- Implement synchronous signal application with bounded fence-race retry.
- Define and test poison-run marking via the `attempts` counter.
- Implement the pruner with the graph-version retention rule.
- Implement the `:inline` and `:manual` testing modes with `drain_runs/1`.
- Provide install and supervision docs plus the introspection query guide.

### Milestone D - Multi-Region

- Run dispatchers in multiple regions against the same store.
- Add read-locality options.
- No process routing: correctness stays with the store, the run claim, and the
  commit fence.

### Milestone E - Enterprise Hardening

- Add tenant quotas and concurrency caps.
- Add pruning policies.
- Add operational telemetry guide.
- Add failure-mode tests for: claim steal after `orphan_ttl`, stale advance
  commit after claim steal, signal-vs-advance fence races (including cancel
  mid-drain), claim release on fence loss, worker crashes mid-superstep,
  crash between park commit and notify (poll fallback), repeated signals
  (natural idempotency), long single-superstep claim freshness, poison marking
  after consecutive crashes and operator recovery from it, dispatcher shutdown
  drain, and notification loss under poll fallback.
- Add dashboard or dashboard-ready read models.

## 15. Success Criteria

The transition succeeds when an adopter can:

- Install one `docket` dependency whose only transitive requirements are Ecto
  and Postgres, which the host already has.
- Run migrations.
- Add one supervised Docket module.
- Define nodes and graphs.
- Start, signal, resume, and inspect runs without writing custom lease or
  routing logic.
- Call `cancel_run` and get back a confirmed cancelled run, synchronously.
- Kill a worker mid-superstep and watch any other node's dispatcher pick the
  run up from the last checkpoint within `orphan_ttl` plus one poll interval.
- Trust that two workers cannot both commit progress for one run, even if a
  stale claim briefly lets both execute.
- Test graph semantics without Postgres and test operational semantics
  deterministically via the `:inline` / `:manual` testing modes under the
  Ecto sandbox.

The package should feel boring to operate. That is the bar.
