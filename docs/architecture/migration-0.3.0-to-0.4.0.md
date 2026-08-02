# Migrating backends from Docket 0.3.0 to 0.4.0

Docket 0.4.0 raises the PostgreSQL schema to version 3: a new
`docket_detached_tasks` claim-index table (one row per pending or claimed
detached task, maintained inside the same commit as every run mutation) and
a relaxed `docket_runs` running-schedule constraint that admits a running
run holding neither a wake nor a claim — the shape of a run parked on
detached work with no live deadline. The durable run encoding also changes:
`Docket.Run.TaskState` gained the `scheduled_at` field, so a run persisted
by 0.3.x while parked inside an active superstep (retry backoff) fails to
decode under 0.4.0. The supported adopter path is drain-and-cut-over: let
in-flight runs finish or cancel them, upgrade, then start new runs.

## Schema upgrade

The upgrade is an ordinary hand-written transactional migration pinning both
directions to the version it adds:

```elixir
defmodule MyApp.Repo.Migrations.UpgradeDocketToV3 do
  use Ecto.Migration

  def up, do: Docket.Postgres.Migration.up(version: 3)
  def down, do: Docket.Postgres.Migration.down(version: 3)
end
```

Stop dispatchers and all Docket run writers before the upgrade, deploy one
homogeneous binary version, migrate, and restart; the binary requires schema
version 3 and checks it before starting backend children. Rolling back
returns the host to schema version 2, dropping the claim index and restoring
the exact-one running-schedule constraint. `mix docket.gen.migration`
remains fresh-install-only and now installs V01 through V03 in one host
migration.

## New runtime behavior

A declared detached node parks at plan time: the superstep partitions it
into the parked set as a pending task — the dispatcher never spawns a worker
for it — and the park commit writes the task's pending row into
`docket_detached_tasks` atomically with the run. The run stays `:running`
and waits externally with no wake (`wake_at` is null; retry siblings still
contribute their backoff wakes). A bounded schedule-to-start wait is
recorded durably on the task and its timer but does not wake the run in
this release — deadline wakes and expiry dispositions land together in a
later release, because a wake nothing can act on only churns the claim
machinery. Cancelling the run removes its index rows in the same terminal
commit, and the pruner is unchanged: terminal runs have no live rows, and
the table's `run_id` foreign key cascades for crash-window stragglers.

Claiming and completing detached tasks land in subsequent releases; until
they do, a parked detached task never becomes dispatchable and its run can
only be cancelled.
