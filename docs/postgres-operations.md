# Docket.Postgres Operations and Correctness Guide

Complete the [README quickstart](../README.md) before configuring production,
inspecting the runtime, or recovering failures.

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

When an existing host adds `ecto_sql` and `postgrex` after compiling Docket
without them, rebuild the dependency so its conditional PostgreSQL modules are
included:

```sh
mix deps.clean docket --build
mix deps.get
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
enables LISTEN/NOTIFY. Changing to `tenant_mode: :required` also requires the
WindowedInterleave claim policy, plus the same authorized non-empty binary
tenant ID on every run, read, and signal call.

## Persistence and transaction ownership

`Docket.Postgres` is one fixed `Docket.Backend` bundle. It supplies compatible
transaction, graph, run, event, and supervision capabilities. The focused
store modules are capability and backend-test boundaries, not public
mix-and-match configuration.

`Docket.Runtime.Moment` is a pre-commit proposal containing the next run,
assigned events, checkpoint metadata, and scheduling disposition.
The lifecycle layer is the single transaction composer: it persists the run,
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
| `claim_policy.implementation` | `Docket.Postgres.ClaimPolicy.Legacy` for `tenant_mode: :none` | Required PostgreSQL tenancy must select `Docket.Postgres.ClaimPolicy.WindowedInterleave`; implementations are validated before startup and cannot be selected per call. |
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

## Claim policy schema state

Schema version 2 contains the shared admission substrate: the singleton
`docket_claim_policy` gate row, trigger-maintained `docket_claim_schedule`
membership with exact unfinished-run counts, `docket_claim_partitions`
ownership rows, and the scoped partial indexes that serve per-scope admission
reads. The schedule is an authoritative superset of current eligibility, so
future timers and parked running work stay visible to admission without being
claimable.

Enable tenant-fair admission across the fleet with the windowed engine:

```elixir
claim_policy: [implementation: Docket.Postgres.ClaimPolicy.WindowedInterleave]
```

Each engine normalizes the persisted `admission_mode` to its own value at
startup, so a rebooted single-engine deployment always claims. The change is
last-boot-wins: fleets mixing engines against one database and prefix are
unsupported in v0.1. `Docket.Postgres.ClaimPolicy.WindowedInterleave`
documents admission ordering, sticky cohort residency, and the statistical
fairness boundary; per-tenant `max_active` caps are not part of v0.1.0.

### Existing schema V1 installations

The generated upgrade is an ordinary transactional migration:

```sh
mix docket.gen.migration -r MyApp.Repo --upgrade-from-v1
mix ecto.migrate -r MyApp.Repo
```

Stop dispatchers and all Docket run writers before the upgrade, deploy one
homogeneous binary version, migrate, and restart. The migration locks the runs
table against inserts while it backfills owner partitions and schedule rows.
The current binary requires schema version 2 and checks it before starting
backend children. Rolling back a generated host-schema-V1 upgrade removes the
version 2 admission schema and returns to schema version 1. Online migrations,
readiness ledgers, fleet
attestations, and audited activation are intentionally outside the v0.1.0
contract.

Fresh installations generated without an upgrade flag install V01 and V02 in
one host migration. Use the same explicit prefix in both migration
directions and runtime configuration.

Claim-policy correctness is covered by the checked-in windowed engine suite
and the shared run-store contract matrix. Timing and large benchmarks are
regression diagnostics.

## Operational inspection

Use `inspect_run` for per-run scheduling, claim age, attempt counts, and poison
health. Use the [telemetry guide](telemetry.md) for dispatcher backlog/claim
activity, vehicle outcomes, observer failures, notifier health, and pruning
passes, and the [benchmark guide](benchmarks.md) for regression diagnostics.
Database tables and opaque binary columns are backend implementation details
rather than an application query API.

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
