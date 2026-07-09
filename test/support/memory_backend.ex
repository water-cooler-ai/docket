defmodule Docket.Test.MemoryBackend do
  @moduledoc """
  Agent-backed reference implementation of `Docket.Storage` and
  `Docket.Coordinator`.

  Tests start one agent per test and pass its pid as the backend context:

      {:ok, backend} = MemoryBackend.start_link()
      {:ok, run} = MemoryBackend.insert_run(backend, run, [])
      {:ok, run} = MemoryBackend.fetch_run(backend, run.id, [])

  The claim TTL that governs steals is controllable with the `:orphan_ttl_ms`
  start option (default generous) so a steal test need not sleep long.
  """

  @behaviour Docket.Storage
  @behaviour Docket.Coordinator

  @default_orphan_ttl_ms 60_000

  defstruct runs: %{}, graphs: %{}, orphan_ttl_ms: @default_orphan_ttl_ms

  # Per-run record held in the agent state.
  defp new_record(run, tenant_id) do
    %{
      run: run,
      tenant_id: tenant_id,
      claim_token: nil,
      claimed_at: nil,
      wake_at: nil,
      events: []
    }
  end

  def start_link(opts \\ []) do
    ttl = Keyword.get(opts, :orphan_ttl_ms, @default_orphan_ttl_ms)
    Agent.start_link(fn -> %__MODULE__{orphan_ttl_ms: ttl} end)
  end

  # ---------------------------------------------------------------------------
  # Docket.Storage
  # ---------------------------------------------------------------------------

  @impl Docket.Storage
  def insert_run(backend, run, opts) do
    tenant_id = Keyword.get(opts, :tenant_id)

    Agent.get_and_update(backend, fn state ->
      if Map.has_key?(state.runs, run.id) do
        {{:error, :already_exists}, state}
      else
        record = new_record(run, tenant_id)
        {{:ok, run}, put_in(state.runs[run.id], record)}
      end
    end)
  end

  @impl Docket.Storage
  def fetch_run(backend, run_id, opts) do
    tenant_id = Keyword.get(opts, :tenant_id, :_any)

    Agent.get(backend, fn state ->
      with {:ok, record} <- fetch_record(state, run_id),
           true <- tenant_matches?(record, tenant_id) do
        {:ok, record.run}
      else
        _ -> {:error, :not_found}
      end
    end)
  end

  @impl Docket.Storage
  def update_run(backend, run, fence, _opts) do
    Agent.get_and_update(backend, fn state ->
      case fetch_record(state, run.id) do
        {:ok, record} ->
          if fence_ok?(record, fence) do
            record = %{record | run: run}
            {{:ok, run}, put_in(state.runs[run.id], record)}
          else
            {{:error, :stale_fence}, state}
          end

        :error ->
          {{:error, :not_found}, state}
      end
    end)
  end

  @impl Docket.Storage
  def put_graph(backend, graph_id, graph_hash, document, _opts) do
    Agent.update(backend, fn state ->
      put_in(state.graphs[{graph_id, graph_hash}], document)
    end)
  end

  @impl Docket.Storage
  def fetch_graph(backend, graph_id, graph_hash, _opts) do
    Agent.get(backend, fn state ->
      case Map.fetch(state.graphs, {graph_id, graph_hash}) do
        {:ok, document} -> {:ok, document}
        :error -> {:error, :not_found}
      end
    end)
  end

  @impl Docket.Storage
  def persist_events(backend, run_id, events, _opts) do
    Agent.get_and_update(backend, fn state ->
      case fetch_record(state, run_id) do
        {:ok, record} ->
          record = %{record | events: record.events ++ events}
          {:ok, put_in(state.runs[run_id], record)}

        :error ->
          {{:error, :not_found}, state}
      end
    end)
  end

  # ---------------------------------------------------------------------------
  # Docket.Coordinator
  # ---------------------------------------------------------------------------

  @impl Docket.Coordinator
  def claim_run(backend, run_id, claim_token, _opts) do
    now = DateTime.utc_now()

    Agent.get_and_update(backend, fn state ->
      case fetch_record(state, run_id) do
        {:ok, record} ->
          if claimable?(record, state, now) do
            record = %{record | claim_token: claim_token, claimed_at: now}
            {{:ok, record.run}, put_in(state.runs[run_id], record)}
          else
            {{:error, :claim_held}, state}
          end

        :error ->
          {{:error, :not_found}, state}
      end
    end)
  end

  @impl Docket.Coordinator
  def refresh_claim(backend, run_id, claim_token, _opts) do
    now = DateTime.utc_now()

    Agent.get_and_update(backend, fn state ->
      case fetch_record(state, run_id) do
        {:ok, %{claim_token: ^claim_token} = record} ->
          {:ok, put_in(state.runs[run_id], %{record | claimed_at: now})}

        _ ->
          {{:error, :claim_lost}, state}
      end
    end)
  end

  @impl Docket.Coordinator
  def release_claim(backend, run_id, claim_token, _opts) do
    Agent.update(backend, fn state ->
      case fetch_record(state, run_id) do
        {:ok, %{claim_token: ^claim_token} = record} ->
          put_in(state.runs[run_id], %{record | claim_token: nil, claimed_at: nil})

        _ ->
          state
      end
    end)
  end

  @impl Docket.Coordinator
  def commit(backend, checkpoint, claim_token, disposition, _opts) do
    run = checkpoint.run
    expected_seq = checkpoint.seq - 1
    fence = %{expected_seq: expected_seq, claim_token: claim_token}
    now = DateTime.utc_now()

    Agent.get_and_update(backend, fn state ->
      case fetch_record(state, run.id) do
        {:ok, record} ->
          if fence_ok?(record, fence) do
            record =
              record
              |> Map.put(:run, run)
              |> Map.update!(:events, &(&1 ++ checkpoint.events))
              |> apply_disposition(disposition, claim_token, now)

            {{:ok, run}, put_in(state.runs[run.id], record)}
          else
            {{:error, :stale_fence}, state}
          end

        :error ->
          {{:error, :not_found}, state}
      end
    end)
  end

  # ---------------------------------------------------------------------------
  # Read helpers for assertions
  # ---------------------------------------------------------------------------

  def events(backend, run_id) do
    Agent.get(backend, fn state ->
      case fetch_record(state, run_id) do
        {:ok, record} -> record.events
        :error -> nil
      end
    end)
  end

  def claim(backend, run_id) do
    Agent.get(backend, fn state ->
      case fetch_record(state, run_id) do
        {:ok, record} -> record.claim_token
        :error -> nil
      end
    end)
  end

  def wake_at(backend, run_id) do
    Agent.get(backend, fn state ->
      case fetch_record(state, run_id) do
        {:ok, record} -> record.wake_at
        :error -> nil
      end
    end)
  end

  # ---------------------------------------------------------------------------
  # Internals
  # ---------------------------------------------------------------------------

  defp fetch_record(state, run_id), do: Map.fetch(state.runs, run_id)

  defp tenant_matches?(_record, :_any), do: true
  defp tenant_matches?(%{tenant_id: tenant_id}, tenant_id), do: true
  defp tenant_matches?(_record, _tenant_id), do: false

  defp fence_ok?(record, fence) do
    record.run.checkpoint_seq == fence.expected_seq and
      token_ok?(record, fence.claim_token)
  end

  # A nil fence token fences on seq alone. A non-nil token must equal the
  # current claim; expiry only makes a claim stealable, it never fails the
  # fence.
  defp token_ok?(_record, nil), do: true
  defp token_ok?(record, token), do: record.claim_token == token

  defp claimable?(record, state, now) do
    is_nil(record.claim_token) or expired?(record, state, now)
  end

  defp expired?(%{claimed_at: nil}, _state, _now), do: true

  defp expired?(%{claimed_at: claimed_at}, state, now) do
    DateTime.diff(now, claimed_at, :millisecond) >= state.orphan_ttl_ms
  end

  defp apply_disposition(record, :continue, nil, _now), do: record

  defp apply_disposition(record, :continue, _claim_token, now) do
    %{record | claimed_at: now}
  end

  defp apply_disposition(record, {:park, wake_at}, _claim_token, _now) do
    %{record | claim_token: nil, claimed_at: nil, wake_at: wake_at}
  end
end
