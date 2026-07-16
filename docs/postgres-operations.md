# Docket.Postgres Operations and Correctness Guide

This guide is the operator-facing reference for the v0.1.0 PostgreSQL runtime.
Start with the [README quickstart](../README.md), then use
this page for production configuration, inspection, and failure recovery.

## Fresh application setup

Add Docket plus the optional PostgreSQL dependencies to the host, configure
`MyApp.Repo`, then install the tables through a host-owned migration:

```elixir
def deps do
  [
    {:docket, "~> 0.1.0"},
    {:ecto_sql, "~> 3.10"},
    {:postgrex, "~> 0.17"}
  ]
end
```

```sh
mix deps.get
mix docket.gen.migration -r MyApp.Repo
mix ecto.migrate -r MyApp.Repo
```

Define one complete facade. Production retention has no implicit defaults:

```elixir
defmodule MyApp.Docket do
  use Docket,
    repo: MyApp.Repo,
    backend: Docket.Postgres,
    tenant_mode: :none,
    notifier: :none,
    pruner: [
      interval_ms: :timer.hours(1),
      event_retention_ms: :timer.hours(24 * 30),
      run_retention_ms: :timer.hours(24 * 90),
      batch_size: 1_000
    ]
end
```

Start it after the Repo in `MyApp.Application`:

```elixir
children = [MyApp.Repo, MyApp.Docket]
Supervisor.start_link(children, strategy: :one_for_one, name: MyApp.Supervisor)
```

This setup uses polling and tenantless storage. Removing `notifier: :none`
enables LISTEN/NOTIFY; changing to `tenant_mode: :required` requires the same
authorized non-empty binary tenant ID on every run, read, and signal call.

## Persistence and transaction ownership

`Docket.Postgres` is one fixed `Docket.Backend` bundle. It supplies compatible
transaction, graph, run, event, and supervision capabilities. The focused
store modules are capability and backend-test boundaries, not public
mix-and-match configuration.

`Docket.Runtime.Moment` is a pre-commit proposal containing the next run,
assigned events, checkpoint metadata, and scheduling disposition.
`Docket.Lifecycle` is the single transaction composer: it persists the run,
schedule, and events atomically, then invokes best-effort observers only after
commit. A failed event append or lost claim fence commits none of the moment.

This is the exact durable guarantee boundary, not an exactly-once execution
promise. Node code that proposed the moment may already have executed, and may
execute again after a crash, timeout, claim steal, or ambiguous commit result.
The complete boundary matrix and external idempotency requirements are in
[Delivery and Execution Guarantees](delivery-guarantees.md).

## Durable statuses and derived operational views

The durable status enum is deliberately flat:

- `running`: graph work may be ready, future-scheduled, claimed, or poisoned.
- `waiting`: the graph requires an external signal, such as interrupt input.
- `done`: successful terminal outcome.
- `failed`: terminal graph failure; `%Docket.Run{failure: %Docket.Run.Failure{}}`
  retains the structured cause.
- `cancelled`: explicit operator or application cancellation.

Outcome timestamps do not replace status: time and outcome are different
facts. Folding cancellation into `failed` plus a cause would make an intentional
control action indistinguishable from graph failure. A status/outcome pair
would add combinations the database must reject without adding information.
`finished_at` already carries the terminal-phase bit that a separate
`finished` status would duplicate.

`waiting` is necessary because the database enforces the useful invariant that
every healthy `running` row has exactly one of a wake or a claim. An externally
parked run has neither, so calling it `running` would weaken the SQL-enforceable
queue shape.

Ready, scheduled, claimed, recoverable, and poisoned are derived operational
views, not durable graph statuses:

- ready: unclaimed healthy `running`, with `wake_at <= now`;
- scheduled: unclaimed healthy `running`, with `wake_at > now`;
- claimed: healthy `running`, with paired token and claim timestamp;
- recoverable: claimed with `claimed_at` older than the orphan TTL;
- poisoned: `running` with paired poison facts and neither wake nor claim;
- invalid: any row violating these tuples (normally impossible because CHECK
  constraints reject it).

`fetch_run` returns the exact committed `%Docket.Run{}` execution document.
`inspect_run` returns `Docket.RunInfo`, adding token-free wake, claim timestamp,
attempt/abandon counters, and poison facts. Claim operations never rewrite the
run document's `updated_at`.

## Scope is mandatory

Storage operations accept exactly `:system`, `:tenantless`, or
`{:tenant, non_empty_binary_id}`. `:system` is reserved for trusted dispatcher
and recovery code. Tenantless access matches only `tenant_id IS NULL`; it is
not an unscoped or system read. A required-tenant facade rejects omission
before storage, and cross-tenant reads return not found.

Applications remain responsible for authorization before deriving the stable
tenant ID passed to the facade. See the
[parent-application example](../examples/parent-app-integration.md).
The public outcomes fail closed:

```elixir
{:error, %Docket.Error{type: :invalid_tenant}} =
  MyApp.RequiredDocket.fetch_run(run_id)

{:error, :not_found} =
  MyApp.RequiredDocket.fetch_run(run_id, tenant_id: "different-tenant")

{:error, :not_found} =
  MyApp.TenantlessDocket.fetch_run(tenant_owned_run_id)
```

The facade never accepts `:system` from these options; that scope exists only
inside the backend dispatcher and recovery paths.

## Claims, fences, and poison

The run row is the queue, and `RunStore` owns the whole aggregate transition:
claim, release, fenced commit, serialized mutation, and poison
recovery. A claim token remains authoritative after TTL expiry until another
claimant actually steals it. A steal changes the token, invalidating stale
commit; stale release is an idempotent no-op.

`claim_attempts` counts consecutive launched claims without committed graph
progress. With maximum `N`, exactly `N` launches are allowed; the next recovery
need poisons instead of launching. A successful commit resets the counter.
Pre-execution deployment incompatibility uses the separate `claim_abandons`
counter and poison reason.

A graph whose explicit node timeout exceeds this host's attempt maximum is
handed back without poisoning: the run stays valid, the handback counts one
claim abandon, and its wake backs off exponentially with the consecutive
abandon count up to `abandon_backoff_cap_ms`. A fleet with no compatible host
therefore parks the run at the cap instead of spinning or failing it; watch
the `[:docket, :postgres, :claim, :operation]` abandon telemetry with reason
`:host_incompatible` and audit stored graphs before tightening host limits.
Any committed progress resets the abandon count.

Claims and checkpoint fences guarantee one durable winner. They do not
guarantee one executor, cancel arbitrary in-flight effects, or make external
calls exactly-once. Replaying an uncommitted attempt preserves its stable task
and idempotency identity. An external integration must atomically deduplicate
that identity with the effect when duplicates are unacceptable; a separate
check followed by an effect retains the crash window.

## Failure, poison, and signals

A fresh supervised application can exercise the complete interrupt lifecycle
with an ordinary node and graph:

```elixir
defmodule MyApp.Nodes.Review do
  @behaviour Docket.Node

  @impl true
  def config_schema do
    Docket.Schema.object(%{
      "resume_field" => Docket.Schema.string(required: true),
      "result_field" => Docket.Schema.string(required: true)
    })
  end

  @impl true
  def call(state, config, _context) do
    case Map.fetch(state, config["resume_field"]) do
      :error ->
        {:interrupt,
         %Docket.Interrupt{
           schema: Docket.Schema.string(),
           resume_channel: config["resume_field"]
         }}

      {:ok, decision} ->
        {:ok, %{config["result_field"] => decision}}
    end
  end
end

graph =
  Docket.Graph.new!(id: "review")
  |> Docket.Graph.put_field!("decision", schema: Docket.Schema.string())
  |> Docket.Graph.put_field!("result", schema: Docket.Schema.string())
  |> Docket.Graph.put_node!("review",
    implementation: MyApp.Nodes.Review,
    config: %{resume_field: "decision", result_field: "result"}
  )
  |> Docket.Graph.put_edge!("start-review", from: "$start", to: "review")
  |> Docket.Graph.put_edge!("review-finish", from: "review", to: "$finish")
  |> Docket.Graph.put_output!("result", [])

{:ok, graph_ref} = MyApp.Docket.save_graph(graph)
{:ok, started} = MyApp.Docket.start_run(graph_ref, %{})
{:ok, waiting} = MyApp.Docket.await_run(started.id, timeout: 5_000)
[{interrupt_id, %{status: :open}}] = Map.to_list(waiting.interrupts)

{:ok, _scheduled} =
  MyApp.Docket.resolve_interrupt(waiting.id, interrupt_id, "approved")

{:ok, done} = MyApp.Docket.await_run(waiting.id, timeout: 5_000)
:done = done.status
%{"result" => "approved"} = done.output
```

With `tenant_mode: :required`, add the same authorized non-empty binary
`tenant_id` to `save_graph`, `fetch_graph`, graph-version listing/latest reads,
`start_run`, both `await_run` calls, and `resolve_interrupt`. Cancellation
follows the same serialized signal path:

```elixir
{:ok, %Docket.Run{status: :cancelled}} =
  MyApp.Docket.cancel_run(run_id, tenant_id: tenant_id)
```

```elixir
{:ok, %Docket.Run{status: :failed, failure: failure}} =
  MyApp.Docket.fetch_run(run_id, tenant_id: tenant_id)

failure.code
failure.message
failure.details
```

PostgreSQL v0.1.0 always persists assigned runtime events; there is no public
`events: :none` production option. Structured terminal failure is stored in
the run aggregate and does not depend on reconstructing it from event history.

Poison is operational, not a graph status. Inspection and bounded waiting
surface it explicitly:

```elixir
{:ok, %Docket.RunInfo{poisoned_at: poisoned_at, poison_reason: reason}} =
  MyApp.Docket.inspect_run(run_id, tenant_id: tenant_id)

{:error, {:poisoned, %Docket.RunInfo{}}} =
  MyApp.Docket.await_run(run_id, tenant_id: tenant_id, timeout: 5_000)

{:ok, %Docket.Run{status: :running}} =
  MyApp.Docket.retry_poisoned_run(run_id, tenant_id: tenant_id)
```

`resolve_interrupt` and `cancel_run` are serialized lifecycle mutations.
Repeated calls return explicit inactive/not-found outcomes; they do not replay
the prior success. There is no public `resume_run` or graph-semantic
`retry_failed` operation in v0.1.0.

## Configuration reference

| Option | Default | Guidance |
| --- | --- | --- |
| `repo:` | required | A started Ecto PostgreSQL Repo owned by the host. |
| `backend:` | required | Use the complete `Docket.Postgres` bundle. |
| `prefix:` | `public` | Must match both directions of the generated migration. |
| `tenant_mode:` | `:none` | Use `:required` for tenant-scoped rows; all calls then require a non-empty binary ID. |
| `claim_policy.implementation` | `Docket.Postgres.ClaimPolicy.Legacy` | Internal instance-level admission rollout switch. Implementations are validated before startup and cannot be selected per call. |
| `dispatcher.concurrency` | `10` | Maximum active vehicles per runtime instance. |
| `dispatcher.poll_interval_ms` | `1_000` | Correctness fallback and poll-only wake latency. |
| `dispatcher.orphan_ttl_ms` | `60_000` | Crash-recovery lease TTL; must exceed the finite drain residency limit with operational headroom. |
| `dispatcher.max_claim_attempts` | `5` | Launches allowed before the next recovery need poisons. |
| `dispatcher.drain_timeout_ms` | `30_000` | Shutdown wait for tracked vehicles. |
| `max_attempt_elapsed_ms` | `2_000` | Instance-owned finite host maximum inherited by nodes without `timeout_ms`; larger explicit graph timeouts are rejected before execution. |
| `vehicle.drain_budget` | 100 moments / 3 seconds | Cooperative moment-boundary yield; `max_elapsed_ms` must be finite, at least the attempt maximum, and below orphan TTL. |
| `vehicle.abandon_backoff_ms` | `30_000` | Base delay before retrying a pre-execution incompatibility; host-limit handbacks double it per consecutive abandon. |
| `vehicle.abandon_backoff_cap_ms` | `3_600_000` | Ceiling on the exponential host-incompatibility backoff. |
| `vehicle.max_claim_abandons` | `5` | Abandons allowed before incompatibility poison; host-limit handbacks share the counter but never poison. |
| `executor` | `Docket.Executor.Local` | Instance-owned executor used by inline, manual, and supervised vehicles. All executors run inside runtime-owned per-activation processes with the same hard deadline. |
| `executor_opts` | `[]` | Instance-owned options passed to the configured executor. |
| `max_supersteps` | unbounded | Optional host safety ceiling; publish a graph policy when it is graph identity. |
| `clock` | system clock | Testing-only deterministic wall clock shared by public lifecycle operations, admission, and vehicles; requires `testing: :inline` or `:manual` and cannot be nested or overridden per call. |
| dispatcher/vehicle `jitter` | random jitter | Separate polling and abandon-backoff injection points; production overrides should distribute work. |
| `dispatcher.on_poisoned` | no-op | Best-effort operational callback for newly poisoned claims; inspect durable rows for truth. |
| `notifier:` | enabled | Use `:none` for poll-only. LISTEN needs a direct/session-pooled endpoint. |
| `notifier.connection` | derived from Repo | Override only to use a direct/session-pooled LISTEN endpoint. |
| `pruner:` | required, no defaults | Supply interval, event/run retention, and batch size. Event retention must not exceed run retention. |
| `checkpoint_observers:` | `[]` | Best-effort after commit; delivery may be lost or duplicated. |
| `testing:` | production | `:inline` drains synchronously; `:manual` advances only through bounded `drain_runs`. |

Node retry policy belongs to the published graph. A retryable failure commits
`:retry_scheduled` while graph status remains `running`; durable attempt state,
sibling results, and the future wake survive process recovery. The dispatcher
does not sleep on behalf of retrying work.
Attempt timeout bounds executor callback residency, not exact claim-hold time.
The drain budget is cooperative, orphan TTL recovers crashed hosts, dispatcher
shutdown timeout bounds graceful waiting, and client watchdogs are external.
Killing an attempt cannot retract external effects or unlinked children;
expected long work must use a durable external wait/interrupt until native
detached await support is available.
Without a retry policy, a node gets one attempt and no backoff. A configured
retry policy defaults `max_attempts` to `1` and `backoff_ms` to `0`; raise the
attempt count and choose a positive backoff to enable durable retry parking.
Testing modes start no dispatcher, notifier, vehicle supervisor, or pruner;
their caller-owned drain still uses the production lifecycle transactions.
Manual and inline drains call the same `RunStore.claim_due/3` entrypoint as the
supervised dispatcher. One backend-instance admission phase alternates
demand-one ready/expired preference across supervised, manual, and inline
claims. RunStore dispatches through the instance-resolved ClaimPolicy, and a
public `drain_runs` call cannot override the selected admission
implementation. See the [ClaimPolicy boundary](architecture/docket-claim-policy.md)
for its plan/decoder contract, atomicity requirement, and rollout procedure.

## Checkpoints, events, and notifications

The stored `checkpoint_seq` is the current committed fence. A proposed moment
must carry exactly that value plus one. Event `seq` is an independent monotonic
history identity because one moment may emit multiple
runtime events plus one metadata-only `:checkpoint_committed` event.

`checkpoint_observers` run after commit and may be lost or duplicated around
a crash. They are suitable for cache/UI hints, not an outbox. Retained events
are the durable integration source during their configured retention period,
but persistence is not delivery. Docket exposes tenant-scoped event reads, not
a managed exporter or durable consumer cursor. An authorized application
exporter must page through that API, advance its cursor only after downstream
acceptance, and make downstream handling idempotent by `{run_id, seq}`. Raw
payload and metadata columns are private binary formats. The
removed 0.0.1 host-owned `checkpoint:`
committer belongs only in migration documentation.

Immediate wakes call `pg_notify` inside the recording transaction. PostgreSQL
delivers only after commit and drops notifications on rollback. Listener loss
only adds latency because polling remains correctness. Fence loss discards the
proposal and recovery replans from the last committed run, so its cost is
re-execution, not a partial durable moment.

## Operational inspection

Use `inspect_run` for per-run scheduling, claim age, attempt counts, and poison
health. Use telemetry for dispatcher backlog/claim activity, vehicle outcomes,
observer failures, notifier health, and pruning passes. Database tables and
opaque binary columns are backend implementation details rather than an
application query API.

For application-facing discovery, use `list_runs` with the same tenant scope
as every other run read. It returns indexed summary columns rather than opaque
run state, ordered newest first by `(started_at, run_id)`, and supports status
and graph filters. Use `fetch_run` or `inspect_run` only after selecting a run
that needs its full committed or operational state. `fetch_event` and
`fetch_latest_event` provide scoped retained-event point reads; latest may be
`nil` after complete event pruning.

If an operator must inspect tables directly during an incident, use a trusted
database role, honor the configured schema prefix, and avoid decoding the
private `state`, `payload`, or `metadata` columns. Application-facing tools
should remain on the scoped facade so tenantless or tenant access can never
become system access.

### Claim-partition backfill

Schema version 2 installs the rollout ledger but leaves partition backfill
`not_started`, the exact-cap readiness gate `not_ready`, and admission mode
`legacy`. Before backfill, the host must deploy atomic partition
dual-write to every run writer, drain old binaries and their open
transactions, and record reviewed deployment evidence:

```elixir
alias Docket.Postgres.ClaimPolicy.{Backfill, Readiness}

{:ok, _assertion} =
  Readiness.attest_dual_write(context,
    evidence_fingerprint: :crypto.hash(:sha256, deployment_evidence),
    actor: "rollout-operator",
    source: "deployment",
    event_id: "dual-write-2026-07-16"
  )
```

The assertion is non-expiring because it represents a monotonic deployment
fact, not a process heartbeat. Re-attest after any fleet/binary topology
change before continuing reconciliation. An assertion is invalid if a legacy
writer or a pre-deployment transaction can still commit a run without its
partition.

Advance one bounded transaction at a time. The host decides whether to cancel,
pause for WAL volume, or wait for replicas between calls; the library never
sleeps while holding the gate, advisory runner, or rollout row.

```elixir
Backfill.advance(context,
  batch_size: 1_000,
  lock_timeout_ms: 1_000,
  statement_timeout_ms: 5_000
)
```

Repeat only while the host's cancellation and capacity checks allow it. A
crash after any return is safe: the next call resumes from the committed
internal `docket_runs.id` cursor and its finite target. The running phase
inserts only distinct canonical keys from that page with conflict-do-nothing;
it never changes an Admin-created partition. Reconciliation records an exact
distinct missing-key count. A positive observation remains visible while a
new bounded repair pass runs; only a later zero observation marks the phase
complete. Rechecking complete state may reopen repair only while the gate is
not ready; a positive observation while ready returns `:prefix_ready`. The
later online-readiness operation independently recounts and owns indexes, the
mandatory FK, readiness promotion, and ready-state drift demotion. After
demotion it may persist a positive `missing_partition_count` while leaving the
complete phase, target, cursor, counters, and completion time untouched; the
next backfill advance reopens repair. This operation performs none of those
readiness-owned changes.

Trusted prefix-local inspection queries are:

```sql
SELECT schema_generation, dual_write_assertion_id, backfill_phase,
       backfill_target_id, backfill_cursor, backfill_batches, backfill_rows,
       backfill_retries, missing_partition_count, backfill_completed_at,
       backfill_last_error, updated_at
FROM "YOUR_PREFIX".docket_claim_rollout
WHERE id = 1;

SELECT count(DISTINCT runs.scope_key) AS missing_scope_keys
FROM "YOUR_PREFIX".docket_runs AS runs
WHERE NOT EXISTS (
  SELECT 1
  FROM "YOUR_PREFIX".docket_claim_partitions AS partitions
  WHERE partitions.scope_key = runs.scope_key
);

SELECT count(*) AS partition_rows,
       pg_size_pretty(pg_total_relation_size(
         '"YOUR_PREFIX".docket_claim_partitions'::regclass
       )) AS retained_size
FROM "YOUR_PREFIX".docket_claim_partitions;

SELECT count(*) AS dormant_partition_rows
FROM "YOUR_PREFIX".docket_claim_partitions AS partitions
WHERE NOT EXISTS (
  SELECT 1
  FROM "YOUR_PREFIX".docket_runs AS runs
  WHERE runs.scope_key = partitions.scope_key
);

SELECT application_name, state, sync_state,
       pg_wal_lsn_diff(
         pg_current_wal_lsn(),
         COALESCE(replay_lsn, flush_lsn, write_lsn)
       )::bigint AS retained_wal_bytes
FROM pg_stat_replication
ORDER BY application_name;
```

Rehearse the rollout on a production-shaped copy before operating a populated
prefix:

- include tenantless runs (`scope_key = ''`), ordinary tenants, and every
  custom schema prefix independently;
- use sparse internal run IDs and interrupt after each committed page to prove
  cursor/target restart behavior;
- insert current dual-writing runs above the frozen target while pages run and
  confirm reconciliation remains zero;
- size a high-dormant/high-distinct-cardinality fixture, observe batch WAL with
  `pg_current_wal_lsn()`/`pg_wal_lsn_diff`, and pause between calls when the
  host's WAL budget is exceeded;
- use `pg_stat_replication` replay/flush LSN lag under the host's database role,
  stop invoking `advance/2` at the approved byte/time threshold, then resume
  from the same ledger cursor after replicas recover; and
- cancel the caller between every phase and inject bounded lock and statement
  timeouts, verifying only a failed SQL work unit increments retries and that
  error text contains no tenant or database detail.

Partition rows retain raw historical `scope_key` values even after the last run
and override disappear. Those values can be personal or confidential data.
Restrict table/audit access, include dormant rows in storage forecasts and
erasure inventories, and do not export them through metrics, logs, receipts,
or generic error tooling. This schema-generation-2 release has no partition
GC. Deletion requires a
separately reviewed proof that no run references the key; this backfill never
deletes a partition.

### Online indexes, foreign key, and readiness

The online artifact is deliberately separate from the transactional V02
migration. The Repo must use PostgreSQL advisory migration locking so the lock
covers the gap between autocommit DDL and Ecto's `schema_migrations` record:

```elixir
config :my_app, MyApp.Repo, migration_lock: :pg_advisory_lock
```

Generate and commit one prefix-explicit artifact. Never paste these operations
into the transactional schema migration:

```sh
mix docket.gen.migration --online --prefix YOUR_PREFIX -r MyApp.Repo
```

The generated module has `@disable_ddl_transaction true`. Docket also takes a
prefix session advisory runner across all autocommit phases. Readiness
verification takes the same authority transactionally, so a first promotion
cannot race online down; DCKT-71 activation must participate in the same
authority. Calls from an outer transaction and repos using table migration
locks are refused.

Run only after the dual-write assertion is current and backfill shows
`complete` with zero missing partitions. The helper performs these restartable
checkpoints:

1. create/repair the tenant-leading ready index concurrently;
2. create/repair the tenant-leading live/expired index concurrently;
3. recount current runs, then install the exact FK as `NOT VALID`; and
4. validate the FK separately and mark the online checkpoint `complete`.

`NOT VALID` avoids a historical validation scan during installation, but it
immediately enforces new writes. A returned old writer that omits partition
dual-write can therefore fail run insertion; stop and drain it rather than
removing the constraint. Validation is separately retryable. A canceled
`CREATE INDEX CONCURRENTLY` can leave a same-name invalid artifact. On restart,
Docket proves that object's table OID, keys, include count, access method,
nonconstraint/nonprimary/nonexclusion status, immediate semantics, default
opclasses/collations/order, and exact predicate before dropping and rebuilding
it concurrently. A foreign-definition object—valid or invalid—is
never dropped automatically. Repair is refused while the prefix is ready. A
drift-demoted TenantFair-selected prefix is not ready and may be repaired;
repair never changes admission mode.

Inspect durable progress separately from schema generation:

```sql
SELECT schema_generation, online_phase, online_attempts, online_last_error,
       online_started_at, online_completed_at,
       ready_index_valid, encode(ready_index_ddl_sha256, 'hex') AS ready_hash,
       live_index_valid, encode(live_index_ddl_sha256, 'hex') AS live_hash,
       fk_disposition, missing_partition_count,
       encode(verified_default_fingerprint, 'hex') AS default_hash,
       verified_at
FROM "YOUR_PREFIX".docket_claim_rollout
WHERE id = 1;

SELECT gate.readiness, gate.readiness_epoch, gate.admission_mode,
       gate.mode_epoch, gate.required_function_contract
FROM "YOUR_PREFIX".docket_claim_admission_gate AS gate
WHERE gate.id = 1;

SELECT index_relid::regclass, phase, lockers_total, lockers_done,
       blocks_total, blocks_done, tuples_total, tuples_done
FROM pg_stat_progress_create_index
WHERE relid = '"YOUR_PREFIX".docket_runs'::regclass;

SELECT conname, convalidated, confmatchtype, condeferrable, condeferred,
       confupdtype, confdeltype
FROM pg_constraint
WHERE conrelid = '"YOUR_PREFIX".docket_runs'::regclass
  AND conname = 'docket_runs_scope_key_claim_partition_fkey';
```

Before every invocation, record free tablespace/disk headroom, current WAL LSN,
replica retained bytes/time, and the number of long transactions. Concurrent
index creation can temporarily require roughly another index-sized allocation;
do not begin without the host's measured headroom. Use these observations:

```sql
SELECT pg_size_pretty(pg_table_size('"YOUR_PREFIX".docket_runs'::regclass)),
       pg_size_pretty(pg_indexes_size('"YOUR_PREFIX".docket_runs'::regclass)),
       pg_current_wal_lsn();

SELECT pid, application_name, state, sync_state,
       pg_wal_lsn_diff(pg_current_wal_lsn(),
         COALESCE(replay_lsn, flush_lsn, write_lsn))::bigint AS lag_bytes
FROM pg_stat_replication
ORDER BY application_name;

SELECT pid, now() - xact_start AS transaction_age, state, query
FROM pg_stat_activity
WHERE datname = current_database() AND xact_start IS NOT NULL
ORDER BY xact_start;
```

The helper uses a lock timeout no greater than one second and a bounded server
statement timeout (at least one second, five minutes by default, and at most one
hour). Its client query deadline is longer than the server deadline. On normal
and handled-error exits, checked cleanup first disables that deadline, proves
the advisory unlock, and restores prior session settings. A cleanup-query or
unlock mismatch raises instead of reporting success; a disconnected PostgreSQL
session releases its lock. To cancel, use the deployment job's normal
cancellation first; a database operator may use `pg_cancel_backend(pid)` only
after matching the exact prefix DDL in `pg_stat_activity`. Never use
`pg_terminate_backend` as a routine throttle. After cancellation, wait for
cleanup, inspect
`pg_stat_progress_create_index`, `pg_index.indisvalid`, the FK catalog, and the
ledger, then rerun the same generated migration. Do not issue hand-written
`DROP INDEX`, mark ledger flags manually, or use `IF NOT EXISTS` as proof.

Pause retries when free disk falls below the host threshold, WAL growth exceeds
budget, any required replica exceeds the approved byte/time lag, a long
transaction prevents an old snapshot from clearing, or cancellation remains
in progress. The library never queries replicas or sleeps while holding its
runner. Resume only after the external condition clears. Attempts/errors are
closed ledger codes; database and tenant text is intentionally absent.

After online completion and explicit default bootstrap, compute the approved
prefix-neutral hashes from `Docket.Postgres.ClaimPolicy.OnlineDDL` and verify:

```elixir
alias Docket.Postgres.ClaimPolicy.{OnlineDDL, Readiness}

hashes = OnlineDDL.index_fingerprints(context.prefix)

Readiness.verify(context,
  expected_readiness_epoch: current_epoch,
  ready_index_ddl_sha256: hashes.ready,
  live_index_ddl_sha256: hashes.live,
  actor: "rollout-operator",
  source: "deployment",
  event_id: "online-ready-2026-07-16"
)
```

Verification rechecks the complete table/catalog set, current zero-missing
count, dual-write/backfill facts, exact indexes/FK, function-contract metadata,
and initialized default under gate -> rollout -> default locks. A wrong supplied
hash deliberately fails closed. A changed default or later index/FK/partition
drift demotes a ready prefix and increments its readiness epoch. After repair,
a fresh event at the new epoch records the current default fingerprint and may
re-promote. A successful unchanged verification leaves the epoch and gate state
alone but refreshes rollout `verified_at`/`updated_at`, so `verified_at` is the
latest successful or demoting proof time. Demotion receipts retain their exact
sorted reasons after audit pruning.

Readiness does not activate anything. DCKT-72 leaves `admission_mode = legacy`,
does not install/select the TenantFair engine or claim function, and does not
change mode/epoch. DCKT-71 owns activation. Ordinary online down is allowed
only before any readiness or activation history and while the prefix is
not-ready/Legacy; otherwise use the explicit stopped-fleet destructive teardown
contract. The intermediate DCKT-64..67 generation-2 schemas were never
released: recreate those developer databases or apply the reviewed stacked
branch patch manually. The online runner does not alter a prior draft V02.

Rehearse on a production-shaped copy with populated v1 tenantless and tenant
runs, current concurrent dual-writing inserts, default and every custom prefix,
and replicas attached. Inject interruption after each committed checkpoint,
cancel both concurrent index creation and FK validation, hold conflicting table
locks to prove bounded timeout, and verify a rerun preserves already-correct
object OIDs while repairing only exact invalid artifacts. Record server major,
settings, DDL fingerprints, ledger transitions, WAL/disk/lag observations, and
the final zero/count/catalog queries. PostgreSQL 13 and 17 both remain required
release gates.

## Tenant-fair claim benchmark

The repository includes a source-checkout-only PostgreSQL benchmark for the
tenant-fair candidate-discovery work. It compares the known ranking-window and
`DISTINCT ON` failure baselines with bounded partition-hint/cursor and recursive
loose-scan reconciliation candidates. This is an exploratory query and plan
benchmark suite, not a public API or a selectable ClaimPolicy implementation.
DCKT-72 makes its ready/live predicates and prefix-neutral index DDL hashes
identical to the online migration, but `runtime_query_parity` remains false;
DCKT-68 owns the eventual admission SQL/function fingerprint.

The runner requires PostgreSQL 13 or newer. CI exercises PostgreSQL 13 as the
minimum and PostgreSQL 17 as the reference version. Supply an explicitly owned
benchmark database URL and invoke the bounded profile with:

```sh
DOCKET_BENCH_DATABASE_URL=postgres://localhost/docket_bench \
  mix run bench/postgres/tenant_fair_claim.exs -- --profile smoke --check
```

The runner creates and owns a scratch schema in that database. Inside it, the
fixture installs Docket's real migration tables and provisional benchmark-only
policy, partition-hint, and tenant-leading index DDL. It never modifies the
runtime hot path, but the database role must still be allowed to create and
drop the scratch schema. Do not point the benchmark at a database unless that
scratch-schema lifecycle and its seeded data are safe. The provisional objects
are query prototypes only; future runtime migrations and TenantFair SQL remain
the authority, and candidates promoted to runtime must be rechecked for exact
ClaimPolicy, cap, locking, decoding, and telemetry parity.

### Profiles and repeatability

`smoke` is deliberately small and exists for deterministic result, artifact,
and broad plan-shape regression checks. Larger profiles exercise independently
configurable queued-row, queued-tenant, dormant-tenant, hot-tenant, capped-
tenant, and ready/expired cardinalities. Each candidate run records its resolved
page size, oversampling factor, and reconciliation work budget. Run an explicit
matrix of those options when selecting thresholds; one run is evidence for only
the values in its manifest. The reconciliation budget must be an exact multiple
of page size times oversampling, so the runner never rounds a requested budget
up to a larger page.

Each candidate/profile combination uses a fixed timestamp and integer seed,
seeds and analyzes the resolved fixture, completes its configured warmup, then
reseeds the same fixture for measurement. Saved `EXPLAIN` operations are rolled
back so data-modifying plans do not consume the measured claim fixture. Use the
same PostgreSQL major version, resolved scenario, seed, database settings, and
machine class when comparing runs. Cache state, autovacuum, concurrent database
traffic, and storage still affect timings even when fixture contents are
deterministic.

Artifacts default to:

```text
tmp/bench/postgres/tenant_fair_claim/<run-id>/
```

The versioned machine-readable artifact set records the resolved scenarios and
candidate thresholds, seed and fixed time, git/runtime/database versions,
relevant database settings, aggregate measurements, and relative paths to raw
samples and saved `EXPLAIN (ANALYZE, BUFFERS, WAL, SETTINGS, FORMAT JSON)`
plans. Environment and source identity live in the manifest; aggregates live in
the summary. Preserve the whole run directory when comparing or publishing
results; a summary without its raw samples and plans is incomplete evidence.

### Metric interpretation

- Checkout time is measured from the caller's checkout request until it owns a
  connection. Ecto query `queue_time` may be absent for queries executed on an
  already checked-out connection and is not substituted for this measurement.
- Query time is caller-observed bounded claim-path time on an owned connection;
  the cursor candidate may execute multiple recorded page statements inside one
  transaction, while the recursive candidate spends its full work budget in one
  loose scan. Commit throughput counts completed claim
  commits over the measured wall-clock window; rollback warmup and plan capture
  are excluded. Sustainable claim/requeue-cycle throughput, when present, is
  labeled separately.
- p50, p95, and p99 use the artifact's documented quantile method and sample
  population. A percentile from the smoke profile is a serialization and
  plumbing check, not statistically useful performance evidence.
- Physical scan work is derived from base-relation and bitmap-index scan nodes
  in the saved plan and is kept separate from any future runtime logical
  candidate counters. Rows scanned per lease is undefined when the statement
  returns no outcome and must not be coerced to zero.
- Root plan buffer counters are the statement totals. Summing parent and child
  buffer counters double-counts work. `EXPLAIN ANALYZE` executes data-modifying
  CTEs, so the runner always captures plans inside an explicit rollback.
- `SKIP LOCKED` normally skips rather than waits. Skipped partitions and the
  benchmark's explicit blocker/control audit are the contention evidence. A
  separate audit holds the deep hot tenant's partition lock and requires full
  progress from other tenants; the recursive candidate also has to progress
  beyond a fully locked first scan slice. Checkout or whole-query duration is
  not a row-lock wait measurement.
- Fewer outcomes than demand is a partial batch, not automatically an
  avoidable under-claim. The under-claim flag requires the bounded control
  audit to prove that eligible, lockable work remained. A candidate that fails
  that correctness audit is ineligible for latency or throughput comparisons,
  even if returning less work makes it appear faster.

CI runs only `--profile smoke --check`. The check may assert bounded work,
deterministic under-claim controls, concurrent cap safety, required artifact
fields, and coarse plan structure. Measured partial batches are audited against
the same cap-aware eligibility policy before they are classified as avoidable
under-claim. The check intentionally has no p95/p99, latency,
throughput, exact cost, or planner-node-count gate; shared-runner timing and
minor PostgreSQL planner changes are not release regressions by themselves.

### TenantFair query regression budget

The published `QUERY-1` budget compares a candidate with an approved prior
artifact for the same candidate and only when both non-smoke artifacts have
the same `normalized_workload_fingerprint`, PostgreSQL major/settings
fingerprint, and an operator-assigned `benchmark_environment_id` identifying
the same machine class. The comparison record also fingerprints both
artifacts' `runtime`, `postgres`, and `config` objects. The normalized workload is the complete
resolved `config` after removing only artifact/control keys `output`, `check`,
and `keep_schema`; both normalized objects must be byte-identical. Query and
DDL hashes must match, or that
record must name and approve the intentional hash transition. A result is
comparable only when both artifacts independently have
the same named `benchmark_candidate`, that candidate's
`performance_eligibility.eligible = true`, `measurements.error_count = 0`, and
at least 1,000 samples. Select exactly one row where
`candidates[].candidate == benchmark_candidate`, then read:

```text
candidates[].measurements.query_us.p95
candidates[].measurements.query_us.p99
candidates[].measurements.sample_count
candidates[].performance_eligibility.eligible
candidates[].performance_eligibility.checks
candidates[].query_sha256
candidates[].plan_path
samples_path
manifest_path
```

from `summary.json`, and read `source.sha256` and `provisional_ddl_sha256` from
`manifest.json`. `benchmark_environment_id`, the two artifact paths, reference
approval, fingerprints, and any approved hash transition are the bounded
`tenant_fair_report_input/v1` comparison fields; the runner does not infer
hardware equivalence. Each artifact path names an exact immutable artifact root
or `summary.json`, whose relative manifest, samples, and selected plan files
must all exist. The target is candidate/reference p95 `<= 1.20` and p99
`<= 1.30`. Checkout and commit distributions remain separately named; plan
execution milliseconds are one rolled-back `EXPLAIN ANALYZE` sample and are not
substituted for the query distribution. A failed correctness audit makes the latency comparison
ineligible rather than fast.

The current manifest says `runtime_parity: false` and
`exploratory_pre_runtime_prototype`; consequently this budget supports only
prototype-to-prototype candidate regression decisions. It becomes runtime
qualification evidence only after exact runtime query/DDL hashes replace the
prototype hashes. It never becomes a production latency SLO without a separate
production event population and target. See the full
[fairness SLO and regression-budget contract](architecture/docket-tenant-claim-fairness-design.md#fairness-slo-and-regression-budget-contract).

## Migration from 0.0.1

Nodes, graphs, schemas, reducers, interrupts, executors, and processless inline
semantic tests carry forward. Durable run serialization does not: host-defined
0.0.1 persistence has no universal shape, so Docket cannot supply an automatic
row conversion.

Drain or terminate old runs, stop old writers, remove the host checkpoint
handler and persistence, install the Docket migration, configure `repo:` and
`backend: Docket.Postgres`, publish each graph to a `Docket.GraphRef`, replace
`run`/`get_run` with `start_run` and `fetch_run`/`inspect_run`, and remove host
`resume` orchestration. The detailed sequence is in the
[migration guide](architecture/migration-0.0.1-to-0.1.0.md).
