# Changelog

All notable changes to `docket` are documented in this file. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the
project follows [Semantic Versioning](https://semver.org/).

## 0.1.0 — 2026-07-23

### Added

- PostgreSQL schema version 2 adds the shared admission substrate: the
  singleton claim-policy gate row with per-engine admission modes,
  trigger-maintained `docket_claim_schedule` membership with exact
  unfinished-run counts, `docket_claim_partitions` ownership rows, and the
  scoped partial indexes serving per-scope admission reads. Run creation
  atomically materializes its owner partition, and rollback leaves no
  partition or wake behind. The stopped migration backfills scope membership,
  supports custom prefixes and rollback to host schema V1, and makes stale
  schema shapes fail closed. ClaimPolicy implementations receive the quoted
  schema identifiers needed to build prefix-local plans.
- `Docket.Postgres.ClaimPolicy.WindowedInterleave`, a set-based tenant-aware
  claim policy under its own `windowed` admission mode: samples active scopes
  in random order, admits breadth-first across scopes for statistical
  cross-tenant fairness, and keeps admission sticky within a scope so runs
  already in progress are re-acquired and driven to completion before new
  runs start, bounding each scope's in-flight cohort at its share of the
  claim batch with no configured cap. Required PostgreSQL tenancy selects
  this engine. Every engine normalizes the persisted admission mode to its
  own value at startup, writing only on drift; mixed-engine fleets are
  last-boot-wins and unsupported.
- `Docket.BackendTests`, a source-owned shared ExUnit suite under
  `test/support`. One explicit backend matrix generates identical black-box
  cases for the memory and PostgreSQL bundles; external backend projects can
  load the same files from a Docket checkout pinned to their supported release.
  The suite covers callback completeness, transaction and cross-store
  atomicity, scopes, graph/event identity and reads, claim fencing and
  recovery, checkpoint sequencing, and serialized mutation safety.
- A canonical delivery and execution guarantee matrix documenting the exact
  PostgreSQL transaction boundary, replayable node attempts, external-effect
  idempotency requirements, retained-event export boundary, best-effort
  observer/notification semantics, and consistency behavior during partitions.
- Public saved-graph and run-query reads: `Docket.fetch_graph/3` reads the exact
  tenant-owned version addressed by a `Docket.GraphRef`;
  `Docket.fetch_latest_graph_ref/3` returns a graph ID's newest scoped
  reference; and `Docket.list_graph_versions/3` returns newest-first
  `%Docket.GraphVersionPage{}` values containing lightweight
  `%Docket.GraphVersion{}` metadata. `Docket.list_runs/2` returns
  tenant-scoped, newest-first `%Docket.RunPage{}` values containing lightweight
  `%Docket.RunSummary{}` rows, with stable `{started_at, run_id}` keyset
  pagination and graph/status filters; and `Docket.fetch_latest_run/2` returns
  the newest matching summary. Graph and run reads use the same explicit owner
  scope; equal graph references may be saved independently by different
  tenants without granting cross-tenant access.
- `Docket.fetch_event/4` and `Docket.fetch_latest_event/3` retained-event point
  reads. Exact missing or pruned sequences return `:not_found`; latest returns
  `{:ok, nil}` when a visible run has no retained events, preserving the
  distinction from an unknown or wrong-tenant run. Both memory and PostgreSQL
  backends implement the new graph, run, and event behavior callbacks.
- `Docket.list_events/3` and the generated `list_events/2` host wrapper: a
  tenant-scoped, keyset-paged reader over retained durable events, backed by a
  new `Docket.Backend.EventStore.list_events/4` callback with memory and
  Postgres implementations. Events return in ascending sequence order (`:after_seq`
  default `0`, `:limit` default `250` in `1..1000`); a wrong tenant and an
  unknown run both report `{:error, :not_found}`, invalid options are rejected
  before storage, and an undecodable stored row surfaces as a typed
  `%Docket.Error{type: :corrupt_event_row}`. The Postgres reader observes the
  page rows, MIN/MAX retention bounds, and run state from one snapshot.
  Sequence gaps from persistence filtering and pruning are legal; pages are
  never promised contiguous.
- `Docket.EventPage`: the public page struct returned by the retained-event
  reader — `events`, `next_after_seq`, `has_more?`, `oldest_available_seq`,
  `latest_available_seq`, and `latest_seq` (the owning run's latest committed
  event sequence, so a fully pruned history is detectable as `latest_seq > 0`
  with `latest_available_seq == nil`), all observed from one consistent
  snapshot.
- Restored authored graph map interchange: `Docket.Graph.to_map/2`,
  `from_map/2`, and `from_map!/2` through a strict `Docket.Graph.Serializer`.
  It produces JSON-safe, string-keyed maps with an explicit `schema_version: 1`
  and strictly rejects unknown keys/enums/versions and non-portable values;
  `$`-prefixed keys are reserved for tagged expressions. Executable node
  implementations resolve only through an explicit host `implementations:`
  registry (validated for unambiguous reverse mapping), so loading a document
  never creates or reaches implementation atoms; failures are typed
  `Docket.Graph.Error` codes `:invalid_registry`, `:unregistered_implementation`,
  and `:unknown_implementation`. Hosts own JSON encode/decode, so there is no
  Jason dependency. This is the editable AUTHORED graph and carries no
  `Docket.GraphRef` hash; `save_graph` still materializes node defaults and
  privately encodes and hashes the effective graph, so re-saving after defaults
  change may yield a different effective reference. `Docket.Run.to_map/from_map`
  and the public `Docket.Graph.hash` remain removed.
- Finite runtime-owned node attempt deadlines across Local, Task, and custom
  executors. Missing node timeouts inherit the host maximum; larger explicit
  graph limits are rejected before execution and rescheduled without poison,
  backing off exponentially up to a configurable cap. Vehicles no longer
  refresh claims during node execution.

- An operator-facing PostgreSQL correctness guide covering durable status
  rationale, derived queue views, scope, claims and poison, failure recovery,
  configuration defaults, checkpoint/event delivery boundaries, and the
  0.0.1 cutover.
- `Docket.Postgres`: the fixed Postgres backend bundle supplying its transaction
  boundary, GraphStore, RunStore, and EventStore while supervising a one-for-all
  dispatcher/vehicle execution subtree, optional LISTEN/NOTIFY fast path, and
  explicit-policy pruner. The host owns its Repo; schema prefixes and all
  operational children derive from one backend context. Dispatcher failure
  terminates untracked vehicles before restart, while notifier/pruner failures
  remain isolated. `notifier: :none` provides poll-only mode, retention has no
  silent deletion defaults, and individual stores cannot be mixed through the
  public configuration.

- `Docket.Postgres.Pruner`: explicit, periodically supervised retention with
  bounded event and terminal-run batches, transaction-scoped per-schema
  advisory locking, `SKIP LOCKED` candidate selection, event-to-run cascade
  accounting, and low-cardinality pass telemetry. Event retention uses the
  persistence timestamp and cannot exceed run retention; only terminal runs
  expire. Graph cleanup deletes only unreferenced versions older than the ten
  newest publications for the same owner scope and graph ID, ordered by
  immutable publication time and graph hash. Referenced versions and the
  newest ten scoped revisions survive regardless of age.
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
  session-pooled endpoint.
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
  connection.
- `Docket.Postgres.GraphCache`: optional node-local compiled-graph cache
  keyed by storage namespace, owner scope, and store-provided
  `{graph_id, graph_hash}`, and validated per read
  against the local generation (`:docket` module fingerprint plus each node
  implementation module's beam MD5, or an injected release identity), so a
  cached graph never crosses an incompatible local generation and cache loss
  only affects latency. Known-incompatible versions are negative-cached, with
  a bounded TTL when the stored document could not even be decoded.
- `Docket.Backend.RunStore.abandon_claim/5` and its Postgres and shared-test
  implementations: the token-and-sequence fenced, non-poisoning disposition
  for a claimed run whose graph the executing node cannot compile
  (deployment incompatibility). A matched abandon hands the acquisition
  attempt back, counts the abandon in the new `docket_runs.claim_abandons`
  column (surfaced through `Docket.RunInfo` and reset by committed progress
  or poison recovery), and reschedules the run at the caller's backoff; once
  the configured abandon maximum is reached it poisons the run with the
  distinct `max_claim_abandons_exceeded` reason instead of retrying
  unboundedly.

- `Docket.Backend`: the only public durable backend substitution boundary,
  directly owning the opaque context and scope types plus `transaction/2`, and
  supplying compatible graph, run-aggregate, event, and supervision
  capabilities (#12).
- Substrate-neutral storage ports (#12):
  - `Docket.Backend.GraphStore` — explicitly owner-scoped immutable canonical
    graph save, exact fetch, latest-reference fetch, and version listing;
  - `Docket.Backend.RunStore` — the run-row aggregate: insert/fetch/inspect,
    atomic batched due/expired claims with poison outcomes, token-guarded
    claim refresh/release, mandatory token-and-sequence fenced commit, serialized
    mutation, and poison recovery;
  - `Docket.Backend.EventStore` — append-only persistence of already-assigned
    events.
- Explicit owner scope on graph operations and
  `:system | :tenantless | {:tenant, id}` scope on run/event operations;
  missing tenant input never implies privileged access (#12).
- In-memory shared-test backend exercising the full bundle contract,
  including overlapping-transaction publication (test support) (#12).
- Postgres substrate scaffold behind optional dependencies: versioned
  migrations (`Docket.Postgres.Migration`, v01), `docket_graph_versions` /
  `docket_runs` / `docket_events` schemas, and `mix docket.gen.migration`
  (#10).
- Postgres `Docket.Postgres.RunStore` atomic, demand-bounded claims over
  separately indexed ready and expired paths, including `SKIP LOCKED`
  dispatcher concurrency, exact claim-attempt poisoning, token-guarded
  claim refresh/release, and the shared internal mandatory-commit token predicate
  (#20).
- Postgres atomic runtime-moment persistence: `RunStore.commit/3` enforces the
  sequence-and-claim fence while applying schedule state, and
  `Docket.Postgres.EventStore` idempotently appends assigned, versioned event
  facts inside the same lifecycle transaction.
- Postgres storage/read foundation: the private `Docket.Postgres.Storage`
  implementation supplies the shared Repo/prefix transaction context behind
  `Docket.Postgres.transaction/2`; `Docket.Postgres.GraphStore`
  persists immutable content-addressed graph versions with concurrent conflict
  arbitration; and the private run row codec plus scoped
  `RunStore.insert_run`/`fetch_run`/`inspect_run` reconstruct the exact
  committed run while keeping claim tokens out of operational projections
  (#26).
- Durable graphs and opaque run/event state use a private versioned
  deterministic ETF codec and PostgreSQL `bytea`. Graph identity hashes the
  exact stored ETF projection; relational columns retain the facts needed for
  claiming, scheduling, inspection, retention, and constraints. The public
  `Docket.Graph.hash` API and persistence-only Run map codec/version were
  removed with no v0.0.1 decoder or dual-write path.
  Graph identity is now computed privately only from effective graph bytes
  produced by compiler ingest. Graph-specific canonicalization and strict
  recovered collection validation live at the compiler boundary; the generic
  durable codec owns only ETF envelope and durable-term recovery safety
  (#26).
- `Docket.Event`: metadata-only `:checkpoint_committed` event type and the
  `types/0` helper (#12).
- `Docket.Run.Failure`: durable, JSON-safe terminal failure payload
  (`code`, `message`, optional `node_id`/`details`), present exactly when a
  run is `:failed` (`Run.validate_failure/1`, enforced at the wire boundary
  and shared backend commits) and populated by every runtime terminal-failure
  path, so a failed run retains its cause independently of retained event
  history (#17).
- `Docket.RunInfo`: token-free operational projection (`run`, `wake_at`,
  `claimed_at`, `claim_attempts`, paired poison facts) returned by
  `Docket.Backend.RunStore.inspect_run`, documenting the `inspect_run` contract
  and the poisoned `await_run` typed operational halt (#17).
- `Docket.Run.durable_statuses/0`, `durable_status?/1`, and
  `valid_transition?/2`: the five durable graph statuses and the locked
  transition matrix, with exhaustive transition/absorbing-state tests
  (#17).
- Durable retry parking: a retryable node failure commits exactly one sync
  `:retry_scheduled` checkpoint at the boundary — graph status stays
  `:running`, the graph step does not advance, and the run parks until the
  retry deadline instead of sleeping in the dispatcher. Crash-resume
  continues at the persisted attempt with the same task and idempotency
  identity, never reruns committed sibling results, and only
  permanent/exhausted failure becomes `:failed` with `Run.failure`
  (#18).
- Durable active-superstep state on `Docket.Run`: `active_tasks` (parked
  attempts via extended `Docket.Run.TaskState`: snapshot, source versions,
  accumulated failures, stable identity helpers), `pending_writes`
  (`Docket.Run.PendingWrite` — completed sibling results invisible until
  the barrier), and `timers` (`Docket.Run.TimerState` retry deadlines)
  (#18).
- `Docket.Runtime.Moment`: the substrate-neutral pre-commit value every
  runtime transition now calculates — proposed `Docket.Run`, assigned
  events, checkpoint type/metadata, and an explicit core-owned disposition
  (`:continue` or `{:park, :immediate | :external | {:at, timestamp} |
  :terminal, reason}`). Calculation performs no storage write, checkpoint
  delivery, or telemetry; `Moment.checkpoint/1`/`context/2` build the
  committed checkpoint value only after the driver's commit succeeds.
- Processless moment entrypoints on the shared runtime loop:
  initialization calculates exactly one `:run_initialized` moment without
  invoking observers, and advancement plans, dispatches, and
  applies exactly one superstep per call — one commit-boundary moment
  (barrier, retry park, or terminal), never a speculative multi-step
  drain. Retry moments use the durable retry control state: graph status
  stays `:running` under the `:retry_scheduled` checkpoint type with a
  `{:park, {:at, deadline}, :retry_backoff}` disposition.
- Runtime checkpoint identity and history allocation: every moment appends
  exactly one metadata-only `:checkpoint_committed` fact after its runtime
  facts, allocating all event identities from `Run.event_seq` independently
  of the `checkpoint_seq` run fence. Checkpoint context and metadata expose
  the committed graph step plus stable multi-task retry/attempt identity,
  and committed checkpoint facts emit `[:docket, :checkpoint, :committed]`
  telemetry (#22).
- Pure `Docket.Runtime.RunMutation.resolve_interrupt/5` and `cancel_run/2`
  graph mutations produce deterministic pre-commit moments with explicit
  dispositions: cancellation is terminal, and resolution wakes immediately
  unless every active attempt is parked behind a future retry deadline, in
  which case it parks at the earliest deadline so a resolved run is never
  dispatched before any attempt is due. Cancellation adds the sync
  `:run_cancelled` checkpoint/event fact; repeated cancellation returns an
  explicit unchanged result with the stored run and consumes no sequences.
- `Docket.Lifecycle`, the substrate-neutral owner of atomic run/event start,
  claim-fenced moment commit, and serialized signal/event transaction recipes,
  including the complete runtime-disposition to storage-schedule mapping.
- Explicit content-addressed graph publication through `save_graph`, returning
  a `Docket.GraphRef`; `start_run` accepts only that saved reference and never
  writes the graph store.
- Locked versioning amendment: publication materializes node schema defaults
  into the effective canonical graph before hashing. Runs pin only graph ID and
  hash; later local compilation validates but never injects newly introduced
  defaults. The vehicle contract requires local compilation once per claim and
  reuse for its drain. Compiler ABI and distributed artifacts are deliberately
  not durable run identity; the vehicle shell is specified separately.
- Unbounded cyclic graphs are valid. `max_supersteps` remains an optional graph
  policy or host/runtime safety limit rather than a publication requirement.
- Durable operational facade functions: `start_run`, `fetch_run`,
  `inspect_run`, `resolve_interrupt`, `cancel_run`, `retry_poisoned_run`, and
  bounded `await_run`, with strict tenantless/required scope resolution and a
  poisoned-run operational halt.
- `Docket.Checkpoint.Observer`, configured through separate
  `checkpoint_observers:`, for isolated best-effort notification only after a
  durable commit. The `0.0.1` host-owned `checkpoint:` committer is no longer
  a production configuration path.

### Changed

- PostgreSQL wall-clock injection is now a testing-only, top-level,
  instance-owned option shared by facade operations, synchronous claims, and
  vehicles; nested and per-call overrides are rejected or ignored. Runtime
  clocks validate `DateTime` results, and the ClaimPolicy boundary normalizes
  `now` to UTC microsecond precision before every implementation receives it.
- The main README now leads with a complete Docket.Postgres quickstart covering
  dependencies, migration, supervision, retention, publication, and durable
  execution. Release documentation consistently describes the backend-owned
  v0.1.0 lifecycle, processless waiting, required tenant scope, deterministic
  testing modes, and current production vehicles. The Hex package and ExDoc
  output include the linked operational, architecture, roadmap, and example
  guides.
- Runtime dispatch now executes every node selected for a superstep
  concurrently against the same committed snapshot, then collects results in
  deterministic activation order before crossing the existing update barrier.
- Postgres run insertion, moment commit, signal mutation, and poison recovery
  emit `pg_notify` on `docket_wake` within the same transaction whenever the
  recorded wake is due at or before the database clock. Claim release and
  abandonment stay silent so launch-failure retries keep the poll interval as
  their backoff.
- Postgres claim, refresh, release, steal, and poison operations no longer
  rewrite promoted `Docket.Run.updated_at`. Dedicated operational timestamps
  now carry those transitions, so `fetch_run` remains the exact last committed
  graph-run document while `inspect_run` reports delivery state (#26).
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
  accepted.
- One `docket` package: `Docket.Postgres.*` compiles only when the host
  supplies optional `ecto_sql`/`postgrex`; the core keeps no hard Postgres
  dependency (#8/#9).
- Version bumped to `0.1.0-dev` (#8).
- `Docket.Node` documentation clarifies the four failure-signaling forms and
  their identical normalization (#12).

- `Docket.Run` carries terminal failure and active-superstep state directly.
  Explicit durable validation rejects the private `:created` sentinel,
  failure/status disagreement, malformed collections, and inconsistent active
  task/timer coverage before private backend encoding and after recovery
  (#17/#18); the persistence boundary was later replaced by the private
  backend codec.
- The dispatcher executes exactly one node attempt per invocation; retry
  waiting moved out of the durable path into shell parking (supervised
  Runtime: timer wake that keeps serving reads during backoff; inline test
  runtime: the injected `:sleeper` serves each committed park's wait).
  Retried attempts' `:node_failed` events now ride the `:retry_scheduled`
  checkpoint that recorded them instead of the eventual barrier checkpoint
  (#18).
- Postgres v01 schema finalized to spec revision 8 (amended in place
  before the 0.1.0 release): `claim_attempts` / `poisoned_at` / `poison_reason`
  replace `attempts` / `operational_status` / `operational_error`, and
  `started_at` is non-null. Lifecycle CHECK constraints make the
  status/schedule/claim/poison tuple
  authoritative even for raw claim SQL; a composite delete-restricted FK
  keeps every retained run's exact graph version, and a delete-cascaded FK
  keeps events from outliving their run. The single `wake_at` and
  `operational_status` indexes become the ready-unclaimed `(wake_at, id)`,
  expired-claim `(claimed_at, id)`, and poison-introspection partial
  indexes behind positive dispatch eligibility (`status = 'running' AND
  poisoned_at IS NULL`) (#19).
- The Postgres v01 schema makes graph versions tenant-owned without
  adding a publication table: generated non-null scope keys bind runs to
  `(scope_key, graph_id, graph_hash)` through a delete-restricted composite
  foreign key.
- The runtime loop separates pure moment production from commitment. Durable
  backends commit moments transactionally before best-effort observation;
  processless helpers return read-only checkpoint values for assertions.
  Checkpoints never participate in lifecycle decisions.

### Removed

- `docket_checkpoints` table and its Ecto schema: `docket_runs.checkpoint_seq`
  is the run fence, recovery reads the run row, and retained events provide
  history (#11).
- Loader acceptance of version-1 run documents and the serialized `created`
  status (#17).

## 0.0.1 — 2026-07-08

Initial core runtime line (in-process, storage-free): typed graph
construction and compiler, superstep runtime with checkpoints and interrupts,
host-owned checkpoint committer, reducers, local and task executors,
telemetry, and deterministic test helpers.
