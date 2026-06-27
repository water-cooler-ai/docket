# Parent App Integration Example

This example shows how a host application can connect Docket runs to its own
users, accounts, workflows, and database rows.

The core idea is simple: the parent app passes app-owned identity in
`Docket.Run.metadata` when it starts a run. Docket includes that metadata in the
initial `:run_initialized` checkpoint, and the app checkpoint handler persists
the run under the right user/account before node execution begins.

## Flow

1. The parent app receives a request from an authenticated user.
2. The app chooses or creates an app-owned run ID.
3. The app starts Docket with workflow input and app-owned metadata.
4. Docket builds the initial `Docket.Run`.
5. Docket emits a synchronous `:run_initialized` checkpoint.
6. The app checkpoint handler upserts the parent app row by `Docket.Run.id`.
7. Docket starts node execution only after the checkpoint handler returns `:ok`.

That first checkpoint is the durable ownership handoff. If it fails, execution
has not started and no node code has run.

## Runtime Module

A parent app normally configures Docket once:

```elixir
defmodule MyApp.Docket do
  use Docket,
    checkpoint: MyApp.DocketCheckpoint,
    executor: Docket.Executor.Local
end
```

`MyApp.DocketCheckpoint` becomes the app-owned persistence boundary for run
snapshots, events, and projections.

## Starting A Run

The parent app should keep workflow business input separate from app ownership
metadata:

```elixir
defmodule MyApp.Workflows do
  alias MyApp.Repo
  alias MyApp.Workflows.WorkflowRun

  def run_workflow(user_id, workflow_id, attrs) do
    user = MyApp.Accounts.get_user!(user_id)
    workflow = get_workflow!(workflow_id)
    graph = load_published_graph!(workflow)
    run_id = Ecto.UUID.generate()
    input = Map.new(attrs)

    metadata = %{
      user_id: user.id,
      account_id: user.account_id,
      workflow_id: workflow.id,
      app_run_id: run_id
    }

    with {:ok, run} <- MyApp.Docket.run(graph, input, id: run_id, metadata: metadata) do
      Repo.get_by!(WorkflowRun, docket_run_id: run.id)
    end
  end
end
```

`input` belongs to the workflow graph. It is the data nodes read and transform.

`metadata` belongs to the parent app. It is durable identifying context for
authorization, tenancy, database relationships, projections, and support tools.
Docket stores and re-emits it, but does not interpret it.

## Persisting Checkpoints

The checkpoint handler should be idempotent. It should create the row if the
first checkpoint arrives before any parent app row exists, and update the row if
the same checkpoint is delivered again.

```elixir
defmodule MyApp.DocketCheckpoint do
  @behaviour Docket.Checkpoint

  alias MyApp.Repo
  alias MyApp.Workflows.WorkflowRun

  def handle(%Docket.Checkpoint{run: run} = checkpoint, _context) do
    metadata = run.metadata || %{}

    with {:ok, dumped_run} <- Docket.Run.dump(run),
         {:ok, user_id} <- fetch_metadata(metadata, :user_id),
         {:ok, account_id} <- fetch_metadata(metadata, :account_id),
         {:ok, workflow_id} <- fetch_metadata(metadata, :workflow_id) do
      attrs = %{
        docket_run_id: run.id,
        user_id: user_id,
        account_id: account_id,
        workflow_id: workflow_id,
        status: run.status,
        docket_run: dumped_run,
        latest_checkpoint_seq: checkpoint.seq,
        latest_checkpoint_type: checkpoint.type
      }

      Repo.insert!(
        WorkflowRun.changeset(%WorkflowRun{}, attrs),
        on_conflict:
          {:replace,
           [
             :status,
             :docket_run,
             :latest_checkpoint_seq,
             :latest_checkpoint_type,
             :updated_at
           ]},
        conflict_target: :docket_run_id
      )

      :ok
    end
  end

  defp fetch_metadata(metadata, key) do
    case Map.fetch(metadata, key) do
      {:ok, value} -> {:ok, value}
      :error -> {:error, {:missing_run_metadata, key}}
    end
  end
end
```

For apps that need replay, audit, or UI timelines, the same callback can append
`checkpoint.events` in the same database transaction or enqueue them through an
outbox.

## Example Schema

The parent app owns its table shape. A minimal Ecto schema might look like this:

```elixir
defmodule MyApp.Workflows.WorkflowRun do
  use Ecto.Schema
  import Ecto.Changeset

  @primary_key {:id, :binary_id, autogenerate: true}
  schema "workflow_runs" do
    field :docket_run_id, :binary_id
    field :user_id, :binary_id
    field :account_id, :binary_id
    field :workflow_id, :binary_id
    field :status, Ecto.Enum, values: [:running, :waiting, :done, :failed, :cancelled]
    field :docket_run, :map
    field :latest_checkpoint_seq, :integer
    field :latest_checkpoint_type, Ecto.Enum,
      values: [
        :run_initialized,
        :step_committed,
        :interrupt_requested,
        :interrupt_resolved,
        :run_completed,
        :run_failed
      ]

    timestamps()
  end

  def changeset(run, attrs) do
    run
    |> cast(attrs, [
      :docket_run_id,
      :user_id,
      :account_id,
      :workflow_id,
      :status,
      :docket_run,
      :latest_checkpoint_seq,
      :latest_checkpoint_type
    ])
    |> validate_required([
      :docket_run_id,
      :user_id,
      :account_id,
      :workflow_id,
      :status,
      :docket_run,
      :latest_checkpoint_seq,
      :latest_checkpoint_type
    ])
    |> unique_constraint(:docket_run_id)
  end
end
```

The corresponding table should enforce a unique index on `docket_run_id`.
Apps that use multi-tenant storage will usually also add indexes such as
`[:account_id, :user_id]`, `[:account_id, :workflow_id]`, and
`[:account_id, :status]`.

## Resuming A Run

On restart or crash recovery, the app loads its durable row, restores the
public `Docket.Run`, and passes both the graph and run back to Docket:

```elixir
defmodule MyApp.Workflows do
  def resume_run!(%WorkflowRun{} = workflow_run) do
    graph = load_published_graph!(workflow_run.workflow_id)

    {:ok, run} = Docket.Run.load(workflow_run.docket_run)

    MyApp.Docket.resume(graph, run)
  end
end
```

The restored run still contains the original metadata, so future checkpoints
continue to carry the same parent app identity.

## Important Boundaries

- Store authorization and ownership data in `metadata`, not in workflow `input`.
- Treat `run.id` as the Docket run identity and the natural checkpoint upsert
  key.
- Treat `run.metadata` as app-owned opaque data that Docket preserves.
- Persist `checkpoint.run` as the resume source of truth.
- Make checkpoint handling idempotent by `run.id` and `checkpoint.seq`.
- Do not require a parent app row to exist before the first checkpoint.
