# Changelog

All notable changes to `docket` are documented in this file. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the
project follows [Semantic Versioning](https://semver.org/).

Each v0.1.0 ticket updates the Unreleased section in its own PR.

## 0.1.0 — Unreleased

The first operational release line: Docket owns the durable graph-run
lifecycle through a self-contained Postgres backend. Work accumulates on the
`v0.1.0` branch. The design source of truth is
`docs/architecture/docket-operational-transition-spec.md` (revision 8) plus
the DCKT-1 issue tree; entries below reflect what has landed so far.

### Added

- `Docket.Backend`: one backend bundle as the public durable backend
  substitution boundary, supplying compatible transaction, graph,
  run-aggregate, event, and supervision capabilities (DCKT-8, #12).
- Substrate-neutral storage ports (DCKT-8, #12):
  - `Docket.Storage` — the shared backend transaction boundary
    (`transaction/2`);
  - `Docket.Storage.Graphs` — immutable, content-addressed canonical graph
    save/fetch;
  - `Docket.Storage.Runs` — the run-row aggregate: insert/fetch/inspect,
    atomic batched due/expired claims with poison outcomes, token-guarded
    heartbeat/release, mandatory token-and-sequence fenced commit, serialized
    mutation, and poison recovery;
  - `Docket.Storage.Events` — append-only persistence of already-assigned
    events.
- Explicit `:system | :tenantless | {:tenant, id}` scope on every run/event
  storage operation; missing tenant input never implies privileged access
  (DCKT-8, #12).
- In-memory conformance backend exercising the full bundle contract,
  including overlapping-transaction publication (test support) (DCKT-8, #12).
- Postgres substrate scaffold behind optional dependencies: versioned
  migrations (`Docket.Postgres.Migration`, v01), `docket_graph_versions` /
  `docket_runs` / `docket_events` schemas, and `mix docket.gen.migration`
  (DCKT-13, #10).
- Postgres `Docket.Postgres.RunStore` atomic, demand-bounded claims over
  separately indexed ready and expired paths, including `SKIP LOCKED`
  dispatcher concurrency, exact claim-attempt poisoning, token-guarded
  heartbeat/release, and the shared internal mandatory-commit token predicate
  (DCKT-15, #20).
- Postgres storage/read foundation: `Docket.Postgres.Storage` supplies the
  shared Repo/prefix transaction context; `Docket.Postgres.GraphStore`
  persists immutable content-addressed graph versions with concurrent conflict
  arbitration; and the private run row codec plus scoped
  `RunStore.insert_run`/`fetch_run`/`inspect_run` reconstruct the exact
  committed run while keeping claim tokens out of operational projections
  (DCKT-14, #26).
- Durable graphs and opaque run/event state use a private versioned
  deterministic ETF codec and PostgreSQL `bytea`. Graph identity hashes the
  exact stored ETF projection; relational columns retain the facts needed for
  claiming, scheduling, inspection, retention, and constraints. The
  persistence-only Run map codec/version were removed with no v0.0.1 decoder
  or dual-write path (DCKT-14, #26).
- `Docket.Event`: metadata-only `:checkpoint_committed` event type and the
  `types/0` helper (DCKT-8, #12).
- `docs/architecture/docket-operational-transition-spec.md` revision 8 and
  the v0.1.0 spec-lock audit (DCKT-32, #13).
- `Docket.Run.Failure`: durable, JSON-safe terminal failure payload
  (`code`, `message`, optional `node_id`/`details`), present exactly when a
  run is `:failed` (`Run.validate_failure/1`, enforced at the wire boundary
  and conformance commits) and populated by every runtime terminal-failure
  path, so a failed run retains its cause with event persistence off
  (DCKT-31, #17).
- `Docket.RunInfo`: token-free operational projection (`run`, `wake_at`,
  `claimed_at`, `claim_attempts`, paired poison facts) returned by
  `Docket.Storage.Runs.inspect_run`, documenting the `inspect_run` contract
  and the poisoned `await_run` typed operational halt (DCKT-31, #17).
- `Docket.Run.durable_statuses/0`, `durable_status?/1`, and
  `valid_transition?/2`: the five durable graph statuses and the locked
  transition matrix, with exhaustive transition/absorbing-state tests
  (DCKT-31, #17).
- Durable retry parking: a retryable node failure commits exactly one sync
  `:retry_scheduled` checkpoint at the boundary — graph status stays
  `:running`, the graph step does not advance, and the run parks until the
  retry deadline instead of sleeping in the dispatcher. Crash-resume
  continues at the persisted attempt with the same task and idempotency
  identity, never reruns committed sibling results, and only
  permanent/exhausted failure becomes `:failed` with `Run.failure`
  (DCKT-30, #18).
- Durable active-superstep state on `Docket.Run`: `active_tasks` (parked
  attempts via extended `Docket.Run.TaskState`: snapshot, source versions,
  accumulated failures, stable identity helpers), `pending_writes`
  (`Docket.Run.PendingWrite` — completed sibling results invisible until
  the barrier), and `timers` (`Docket.Run.TimerState` retry deadlines)
  (DCKT-30, #18).
- `Docket.Runtime.Moment`: the substrate-neutral pre-commit value every
  runtime transition now calculates — proposed `Docket.Run`, assigned
  events, checkpoint type/metadata, and an explicit core-owned disposition
  (`:continue` or `{:park, :immediate | :external | {:at, timestamp} |
  :terminal, reason}`). Calculation performs no storage write, checkpoint
  delivery, or telemetry; `Moment.checkpoint/2`/`context/2` build the
  committed checkpoint value only after the driver's commit succeeds
  (DCKT-10).
- Processless moment entrypoints on the shared runtime loop:
  initialization calculates exactly one `:run_initialized` moment without
  invoking a checkpoint handler, and advancement plans, dispatches, and
  applies exactly one superstep per call — one commit-boundary moment
  (barrier, retry park, or terminal), never a speculative multi-step
  drain. Retry moments ride DCKT-30's durable control state: graph status
  stays `:running` under the `:retry_scheduled` checkpoint type with a
  `{:park, {:at, deadline}, :retry_backoff}` disposition (DCKT-10).
- Runtime checkpoint identity and history allocation: every moment appends
  exactly one metadata-only `:checkpoint_committed` fact after its runtime
  facts, allocating all event identities from `Run.event_seq` independently
  of the `checkpoint_seq` run fence. Checkpoint context and metadata expose
  the committed graph step plus stable multi-task retry/attempt identity,
  and committed checkpoint facts emit `[:docket, :checkpoint, :committed]`
  telemetry (DCKT-11, #22).
- Pure `Docket.Runtime.RunMutation.resolve_interrupt/5` and `cancel_run/2`
  graph mutations produce deterministic pre-commit moments with explicit
  immediate or terminal dispositions. Cancellation adds the sync
  `:run_cancelled` checkpoint/event fact; repeated cancellation returns an
  explicit unchanged result with the stored run and consumes no sequences
  (DCKT-9).
- `Docket.Lifecycle`, the substrate-neutral owner of atomic run/event start,
  claim-fenced moment commit, and serialized signal/event transaction recipes,
  including the complete runtime-disposition to storage-schedule mapping
  (DCKT-12).
- Explicit content-addressed graph publication through `save_graph`, returning
  a `Docket.GraphRef`; `start_run` accepts only that saved reference and never
  writes the graph store (DCKT-12).
- Locked versioning amendment: publication materializes node schema defaults
  into the effective canonical graph before hashing. Runs pin only graph ID and
  hash; later local compilation validates but never injects newly introduced
  defaults. The vehicle contract requires local compilation once per claim and
  reuse for its drain. Compiler ABI and distributed artifacts are deliberately
  not durable run identity; the vehicle shell lands in its dedicated ticket
  (DCKT-12, DCKT-20).
- Unbounded cyclic graphs are valid. `max_supersteps` remains an optional graph
  policy or host/runtime safety limit rather than a publication requirement.
- Durable operational facade functions: `start_run`, `fetch_run`,
  `inspect_run`, `resolve_interrupt`, `cancel_run`, `retry_poisoned_run`, and
  bounded `await_run`, with strict tenantless/required scope resolution and a
  poisoned-run operational halt (DCKT-12).
- `Docket.Checkpoint.Observer`, configured through separate
  `checkpoint_observers:`, for isolated best-effort notification only after a
  durable commit. DCKT-12 temporarily retains the `0.0.1` host-owned
  `checkpoint:` committer until the assembled backend and deterministic modes
  unblock its removal in DCKT-37.

### Changed

- Postgres claim, heartbeat, release, steal, and poison operations no longer
  rewrite promoted `Docket.Run.updated_at`. Dedicated operational timestamps
  now carry those transitions, so `fetch_run` remains the exact last committed
  graph-run document while `inspect_run` reports delivery state (DCKT-14, #26).
- Locked the final v0.1.0 production boundary to one required durable backend.
  The `0.0.1` host-owned `checkpoint:` driver and its `run`, `resume`, and live
  `get_run` facade will be removed by DCKT-37 only after backend assembly and
  deterministic backend testing land. Node/graph/schema/reducer/executor APIs
  and Postgres-free `Docket.Test` helpers remain public; Run persistence is a
  backend-private boundary and the supported adopter path is drain-and-cut-over.
- Renamed the unreleased durable runtime configuration from `storage:` to
  `backend:` because the configured `Docket.Backend` owns the transaction,
  graph, run, event, context, and supervision capabilities rather than naming
  one storage implementation. The pre-release `storage:` option is no longer
  accepted (DCKT-12).
- One `docket` package: `Docket.Postgres.*` compiles only when the host
  supplies optional `ecto_sql`/`postgrex`; the core keeps no hard Postgres
  dependency (DCKT-7, #8/#9).
- Version bumped to `0.1.0-dev`; release work branches from and merges back
  to `v0.1.0` (DCKT-7, #8).
- Module docs restated as API truth: design rationale moved out of module
  docs and comments into the design docs (DCKT-13, #10).
- `Docket.Node` documentation clarifies the four failure-signaling forms and
  their identical normalization (DCKT-8, #12).

- `Docket.Run` carries terminal failure and active-superstep state directly.
  Explicit durable validation rejects the private `:created` sentinel,
  failure/status disagreement, malformed collections, and inconsistent active
  task/timer coverage before private backend encoding and after recovery
  (DCKT-30/DCKT-31, #17/#18; persistence boundary superseded by DCKT-14).
- The dispatcher executes exactly one node attempt per invocation; retry
  waiting moved out of the durable path into shell parking (supervised
  Runtime: timer wake that keeps serving reads during backoff; inline test
  runtime: the injected `:sleeper` serves each committed park's wait).
  Retried attempts' `:node_failed` events now ride the `:retry_scheduled`
  checkpoint that recorded them instead of the eventual barrier checkpoint
  (DCKT-30, #18).
- Postgres v01 schema finalized to spec revision 8 (amended in place —
  0.1.0 is unreleased): `claim_attempts` / `poisoned_at` / `poison_reason`
  replace `attempts` / `operational_status` / `operational_error`, and
  `started_at` is non-null. Lifecycle CHECK constraints make the
  status/schedule/claim/poison tuple
  authoritative even for raw claim SQL; a composite delete-restricted FK
  keeps every retained run's exact graph version, and a delete-cascaded FK
  keeps events from outliving their run. The single `wake_at` and
  `operational_status` indexes become the ready-unclaimed `(wake_at, id)`,
  expired-claim `(claimed_at, id)`, and poison-introspection partial
  indexes behind positive dispatch eligibility (`status = 'running' AND
  poisoned_at IS NULL`) (DCKT-29, #19).
- The runtime loop's checkpoint emission is split into pure moment
  production plus a host-owned sync-committer adapter: the supervised and
  inline shells adapt the same `Docket.Runtime.Moment` a durable driver
  will commit transactionally, with unchanged public behavior — sync veto,
  async pending effects, checkpoint/telemetry ordering, and park/wake
  mechanics are byte-identical (DCKT-10).

### Removed

- `docket_checkpoints` table and its Ecto schema: `docket_runs.checkpoint_seq`
  is the run fence, recovery reads the run row, and retained events provide
  history. Exactly three operational tables remain (DCKT-28, #11).
- Loader acceptance of version-1 run documents and the serialized `created`
  status (DCKT-31, #17).

## 0.0.1 — 2026-07-08

Initial core runtime line (in-process, storage-free): typed graph
construction and compiler, superstep runtime with checkpoints and interrupts,
host-owned checkpoint committer, reducers, local and task executors,
telemetry, and deterministic test helpers.
