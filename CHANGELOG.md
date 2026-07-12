# Changelog

All notable changes to `docket` are documented in this file. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the
project follows [Semantic Versioning](https://semver.org/).

Each v0.1.0 ticket updates the Unreleased section in its own PR.

## 0.1.0 — Unreleased

The developing operational release line. The backend contract, PostgreSQL
stores, migration, lifecycle transactions, dispatcher, claimed-run vehicle,
notifier, pruner, and public `Docket.Postgres` bundle have landed. The
implementation guide and current-state audit live in `docs/architecture/`;
entries below reflect what has landed so far.

### Added

- Finite runtime-owned node attempt deadlines across Local, Task, and custom
  executors. Missing node timeouts inherit the host maximum; larger explicit
  graph limits are rejected before execution and rescheduled without poison,
  backing off exponentially up to a configurable cap. Vehicles no longer
  refresh claims during node execution.

- An operator-facing PostgreSQL correctness guide covering durable status
  rationale, derived queue views, scope, claims and poison, failure recovery,
  configuration defaults, checkpoint/event delivery boundaries, and the
  0.0.1 cutover.
- `Docket.Postgres`: the fixed Postgres backend bundle supplying Storage,
  GraphStore, RunStore, and EventStore while supervising a one-for-all
  dispatcher/vehicle execution subtree, optional LISTEN/NOTIFY fast path, and
  explicit-policy pruner. The host owns its Repo; schema prefixes and all
  operational children derive from one backend context. Dispatcher failure
  terminates untracked vehicles before restart, while notifier/pruner failures
  remain isolated. `notifier: :none` provides poll-only mode, retention has no
  silent deletion defaults, and individual stores cannot be mixed through the
  public configuration (DCKT-25).

- `Docket.Postgres.Pruner`: explicit, periodically supervised retention with
  bounded event and terminal-run batches, transaction-scoped per-schema
  advisory locking, `SKIP LOCKED` candidate selection, event-to-run cascade
  accounting, and low-cardinality pass telemetry. Event retention uses the
  persistence timestamp and cannot exceed run retention; only terminal runs
  expire. Graph cleanup deletes only unreferenced versions older than the ten
  newest publications for the same graph ID, ordered by immutable publication
  time and row ID. Referenced versions and the newest ten revisions survive
  regardless of age (DCKT-21).
- `Docket.Postgres.Notifier`: the LISTEN fast path for immediate wakes. The
  Postgres RunStore announces every committed wake due at or before the
  database clock with `pg_notify` on the `docket_wake` channel (payload:
  schema prefix, empty when unprefixed) inside the recording transaction, so
  PostgreSQL exposes the notification only after commit and drops it on
  rollback. The notifier holds one dedicated LISTEN connection outside the
  Repo pool, reconnects and re-subscribes on its own, and turns each
  prefix-matching notification into one `Docket.Postgres.Dispatcher`
  immediate poll. Polling remains correctness: a lost notification or dead
  listener only costs latency, and omitting the child is poll-only operation
  whose wake latency is bounded by the dispatcher poll interval alone. LISTEN
  needs a session-scoped connection, so behind PgBouncer transaction or
  statement pooling the notifier's `:connection` must target a direct or
  session-pooled endpoint (DCKT-19).
- `Docket.Postgres.Vehicle`: the Task-per-claim execution shell that turns a
  dispatcher lease into runtime progress. A drain fetches the committed run
  under the lease fence, loads and compiles the exact effective graph version
  node-locally exactly once (never injecting post-publication defaults),
  then loops one `Docket.Runtime.Moment` per fenced
  `Docket.Lifecycle.commit_moment/5`, continuing only on `:continue` and
  exiting on every park after the commit released the claim. Deterministic
  pre-execution compile/decode failure abandons the claim per
  `abandon_claim/5`; fence loss or event-append failure discards the moment,
  releases a still-current claim, and stops; everything else crashes into
  claim-expiry recovery. Node execution holds no checked-out database
  connection (DCKT-20).
- `Docket.Postgres.GraphCache`: optional node-local compiled-graph cache
  keyed by store-provided `{graph_id, graph_hash}` and validated per read
  against the local generation (`:docket` module fingerprint plus each node
  implementation module's beam MD5, or an injected release identity), so a
  cached graph never crosses an incompatible local generation and cache loss
  only affects latency. Known-incompatible versions are negative-cached, with
  a bounded TTL when the stored document could not even be decoded (DCKT-20).
- `Docket.Storage.Runs.abandon_claim/5` and its Postgres and conformance
  implementations: the token-and-sequence fenced, non-poisoning disposition
  for a claimed run whose graph the executing node cannot compile
  (deployment incompatibility). A matched abandon hands the acquisition
  attempt back, counts the abandon in the new `docket_runs.claim_abandons`
  column (surfaced through `Docket.RunInfo` and reset by committed progress
  or poison recovery), and reschedules the run at the caller's backoff; once
  the configured abandon maximum is reached it poisons the run with the
  distinct `max_claim_abandons_exceeded` reason instead of retrying
  unboundedly (DCKT-35).

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
    claim refresh/release, mandatory token-and-sequence fenced commit, serialized
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
  claim refresh/release, and the shared internal mandatory-commit token predicate
  (DCKT-15, #20).
- Postgres atomic runtime-moment persistence: `RunStore.commit/3` enforces the
  sequence-and-claim fence while applying schedule state, and
  `Docket.Postgres.EventStore` idempotently appends assigned, versioned event
  facts inside the same lifecycle transaction (DCKT-16).
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
  graph JSON serializer and public `Docket.Graph.to_map/from_map` and
  `Docket.Graph.hash` APIs, along with the persistence-only Run map
  codec/version, were removed with no v0.0.1 decoder or dual-write path.
  Graph identity is now computed privately only from effective graph bytes
  produced by compiler ingest. Graph-specific canonicalization and strict
  recovered collection validation live at the compiler boundary; the generic
  durable codec owns only ETF envelope and durable-term recovery safety
  (DCKT-14, #26).
- `Docket.Event`: metadata-only `:checkpoint_committed` event type and the
  `types/0` helper (DCKT-8, #12).
- The original operational transition spec and v0.1.0 spec-lock audit
  (DCKT-32, #13), since rewritten as a current PostgreSQL guide and
  implementation audit.
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
  delivery, or telemetry; `Moment.checkpoint/1`/`context/2` build the
  committed checkpoint value only after the driver's commit succeeds
  (DCKT-10).
- Processless moment entrypoints on the shared runtime loop:
  initialization calculates exactly one `:run_initialized` moment without
  invoking observers, and advancement plans, dispatches, and
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
  dispositions: cancellation is terminal, and resolution wakes immediately
  unless every active attempt is parked behind a future retry deadline, in
  which case it parks at the earliest deadline so a resolved run is never
  dispatched before any attempt is due (DCKT-9, DCKT-20). Cancellation adds the sync
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
  durable commit. The `0.0.1` host-owned `checkpoint:` committer is no longer
  a production configuration path.

### Changed

- The main README now leads with a complete Docket.Postgres quickstart covering
  dependencies, migration, supervision, retention, publication, and durable
  execution. Release documentation consistently describes the backend-owned
  v0.1.0 lifecycle, processless waiting, required tenant scope, deterministic
  testing modes, and current production vehicles. The Hex package includes its linked
  examples, telemetry guide, architecture guides, and changelog, and the
  historical pre-cutover audit is labeled at each obsolete finding (DCKT-26).
- Runtime dispatch now executes every node selected for a superstep
  concurrently against the same committed snapshot, then collects results in
  deterministic activation order before crossing the existing update barrier.
- Postgres run insertion, moment commit, signal mutation, and poison recovery
  emit `pg_notify` on `docket_wake` within the same transaction whenever the
  recorded wake is due at or before the database clock. Claim release and
  abandonment stay silent so launch-failure retries keep the poll interval as
  their backoff (DCKT-19).
- Postgres claim, refresh, release, steal, and poison operations no longer
  rewrite promoted `Docket.Run.updated_at`. Dedicated operational timestamps
  now carry those transitions, so `fetch_run` remains the exact last committed
  graph-run document while `inspect_run` reports delivery state (DCKT-14, #26).
- Locked the final v0.1.0 production boundary to one required durable backend.
  The `0.0.1` host-owned `checkpoint:` driver and its `run`, `resume`, and live
  `get_run` facade were removed after backend assembly and deterministic
  backend testing landed. Node/graph/schema/reducer/executor APIs
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
- The runtime loop separates pure moment production from commitment. Durable
  backends commit moments transactionally before best-effort observation;
  processless helpers return read-only checkpoint values for assertions.
  Checkpoints never participate in lifecycle decisions (DCKT-10, DCKT-37).

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
