defmodule Docket.Test.MemoryBackend do
  @moduledoc """
  Agent-backed implementation used by Docket's shared backend tests.

  An outer transaction owns a backend-wide lock, works against an isolated
  Agent snapshot, and publishes the snapshot only after `{:ok, value}`. Every
  direct root mutation takes the same lock, so an overlapping transaction can
  neither overwrite nor be overwritten by another committed write.
  """

  @behaviour Docket.Backend
  @behaviour Docket.Backend.GraphStore
  @behaviour Docket.Backend.RunStore
  @behaviour Docket.Backend.EventStore

  defmodule Transaction do
    @moduledoc false
    @enforce_keys [:root, :agent, :rollback]
    defstruct [:root, :agent, :rollback]
  end

  @type scope :: :system | :tenantless | {:tenant, String.t()}

  defstruct runs: %{},
            graphs: %{},
            clock: nil,
            token_generator: nil

  @impl Docket.Backend
  def graphs, do: __MODULE__

  @impl Docket.Backend
  def runs, do: __MODULE__

  @impl Docket.Backend
  def events, do: __MODULE__

  @impl Docket.Backend
  def context(opts), do: Keyword.fetch!(opts, :name)

  @impl Docket.Backend
  def child_spec(opts, _context) do
    %{
      id: Keyword.get(opts, :id, __MODULE__),
      start: {__MODULE__, :start_link, [opts]},
      type: :worker
    }
  end

  def start_link(opts \\ []) do
    clock = Keyword.get(opts, :clock, &DateTime.utc_now/0)
    token_generator = Keyword.get(opts, :token_generator, &generate_claim_token/0)

    Agent.start_link(
      fn -> %__MODULE__{clock: clock, token_generator: token_generator} end,
      Keyword.take(opts, [:name])
    )
  end

  @impl Docket.Backend
  def transaction(%Transaction{rollback: rollback} = transaction, fun)
      when is_function(fun, 1) do
    try do
      case validate_transaction_result(fun.(transaction)) do
        {:ok, _value} = result ->
          result

        {:error, _reason} = error ->
          mark_rollback_only(rollback)
          error
      end
    catch
      kind, reason ->
        mark_rollback_only(rollback)
        :erlang.raise(kind, reason, __STACKTRACE__)
    end
  end

  def transaction(backend, fun) when is_function(fun, 1) do
    with_root_lock(backend, fn ->
      snapshot = Agent.get(backend, & &1)
      {:ok, transaction_agent} = Agent.start_link(fn -> snapshot end)
      {:ok, rollback_agent} = Agent.start_link(fn -> false end)

      transaction = %Transaction{
        root: backend,
        agent: transaction_agent,
        rollback: rollback_agent
      }

      process_key = transaction_process_key(backend)
      previous = Process.put(process_key, true)

      try do
        case validate_transaction_result(fun.(transaction)) do
          {:ok, _value} = result ->
            if Agent.get(rollback_agent, & &1) do
              {:error, :rollback}
            else
              committed = Agent.get(transaction_agent, & &1)
              Agent.update(backend, fn _state -> committed end)
              result
            end

          {:error, _reason} = error ->
            error
        end
      after
        restore_process_value(process_key, previous)

        if Process.alive?(transaction_agent) do
          Agent.stop(transaction_agent)
        end

        if Process.alive?(rollback_agent) do
          Agent.stop(rollback_agent)
        end
      end
    end)
  end

  @impl Docket.Backend.GraphStore
  def save_graph(
        backend,
        owner_scope,
        graph_id,
        graph_hash,
        %Docket.Graph{id: id, diagnostics: []} = graph
      )
      when id == graph_id do
    validate_owner_scope!(owner_scope)
    bytes = Docket.DurableCodec.encode!(:graph, graph)
    actual_hash = Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)

    if actual_hash == graph_hash do
      state_get_and_update(backend, fn state ->
        key = {owner_scope, graph_id, graph_hash}

        case Map.fetch(state.graphs, key) do
          :error ->
            version = %{graph: graph, published_at: current_time(state)}
            state = put_in(state.graphs[key], version)
            {:ok, state}

          {:ok, %{graph: ^graph}} ->
            {:ok, state}

          {:ok, _other} ->
            {{:error, :graph_content_conflict}, state}
        end
      end)
    else
      {:error, :invalid_graph_hash}
    end
  rescue
    _error in Docket.Error -> {:error, :invalid_graph_document}
  end

  def save_graph(_backend, _owner_scope, _graph_id, _graph_hash, _graph),
    do: {:error, :invalid_graph_document}

  @impl Docket.Backend.GraphStore
  def fetch_graph(backend, owner_scope, graph_id, graph_hash) do
    validate_owner_scope!(owner_scope)

    state_get(backend, fn state ->
      case Map.fetch(state.graphs, {owner_scope, graph_id, graph_hash}) do
        {:ok, %{graph: document}} -> {:ok, document}
        :error -> {:error, :not_found}
      end
    end)
  end

  @impl Docket.Backend.GraphStore
  def fetch_latest_graph_ref(backend, owner_scope, graph_id) do
    validate_owner_scope!(owner_scope)

    state_get(backend, fn state ->
      case graph_versions(state, owner_scope, graph_id) do
        [{graph_hash, _version} | _rest] ->
          {:ok, %Docket.GraphRef{graph_id: graph_id, graph_hash: graph_hash}}

        [] ->
          {:error, :not_found}
      end
    end)
  end

  @impl Docket.Backend.GraphStore
  def list_graph_versions(backend, owner_scope, graph_id, %{limit: limit, before: before}) do
    validate_owner_scope!(owner_scope)

    unless is_integer(limit) and limit > 0 do
      raise ArgumentError, "graph version list limit must be positive"
    end

    state_get(backend, fn state ->
      versions =
        state
        |> graph_versions(owner_scope, graph_id)
        |> Enum.filter(fn {graph_hash, version} ->
          graph_version_before?(version.published_at, graph_hash, before)
        end)
        |> Enum.take(limit + 1)
        |> Enum.map(fn {graph_hash, version} ->
          %Docket.GraphVersion{
            ref: %Docket.GraphRef{graph_id: graph_id, graph_hash: graph_hash},
            published_at: version.published_at
          }
        end)

      {:ok, Docket.GraphVersionPage.new(versions, before, limit)}
    end)
  end

  @impl Docket.Backend.RunStore
  def insert_run(backend, owner_scope, run, checkpoint_type, wake_at) do
    tenant_id = owner_tenant_id!(owner_scope)

    state_get_and_update(backend, fn state ->
      cond do
        not valid_initialized_run?(run, checkpoint_type, wake_at) ->
          {{:error, :invalid_run}, state}

        Map.has_key?(state.runs, run.id) ->
          {{:error, :already_exists}, state}

        not Map.has_key?(state.graphs, {owner_scope, run.graph_id, run.graph_hash}) ->
          {{:error, :not_found}, state}

        true ->
          record = new_record(run, tenant_id, checkpoint_type, wake_at)
          {{:ok, run}, put_in(state.runs[run.id], record)}
      end
    end)
  end

  @impl Docket.Backend.RunStore
  def fetch_run(backend, scope, run_id) do
    validate_scope!(scope)

    state_get(backend, fn state ->
      with {:ok, record} <- fetch_scoped_record(state, scope, run_id) do
        {:ok, record.run}
      end
    end)
  end

  @impl Docket.Backend.RunStore
  def inspect_run(backend, scope, run_id) do
    validate_scope!(scope)

    state_get(backend, fn state ->
      with {:ok, record} <- fetch_scoped_record(state, scope, run_id) do
        {:ok, run_info(record)}
      end
    end)
  end

  @impl Docket.Backend.RunStore
  def list_runs(
        backend,
        scope,
        %{
          limit: limit,
          before: before,
          graph_id: graph_id,
          graph_hash: graph_hash,
          statuses: statuses
        }
      ) do
    validate_scope!(scope)

    unless is_integer(limit) and limit > 0 do
      raise ArgumentError, "run list limit must be positive"
    end

    state_get(backend, fn state ->
      candidates =
        state.runs
        |> Map.values()
        |> Enum.filter(&scope_matches?(&1, scope))
        |> Enum.filter(&run_record_matches?(&1, graph_id, graph_hash, statuses))
        |> Enum.filter(&run_record_before?(&1, before))
        |> Enum.sort(&run_record_newer?/2)
        |> Enum.map(&run_summary/1)
        |> Enum.take(limit + 1)

      {:ok, Docket.RunPage.new(candidates, before, limit)}
    end)
  end

  @impl Docket.Backend.RunStore
  def claim_due(backend, :system, policy) do
    validate_claim_policy!(policy)

    state_get_and_update(backend, fn state ->
      {state, leases, poisoned} = claim_due_records(state, policy)
      {{:ok, %{leases: leases, poisoned: poisoned}}, state}
    end)
  end

  def claim_due(_backend, scope, _policy) do
    raise ArgumentError, "claim_due scope must be :system, got: #{inspect(scope)}"
  end

  @impl Docket.Backend.RunStore
  def refresh_claim(backend, :system, run_id, claim_token, now) do
    state_get_and_update(backend, fn state ->
      case fetch_record(state, run_id) do
        {:ok, %{claim_token: ^claim_token} = record}
        when is_binary(claim_token) and byte_size(claim_token) > 0 ->
          {:ok,
           put_in(state.runs[run_id], %{record | claimed_at: latest(record.claimed_at, now)})}

        _ ->
          {{:error, :claim_lost}, state}
      end
    end)
  end

  def refresh_claim(_backend, scope, _run_id, _claim_token, _now) do
    raise ArgumentError, "refresh_claim scope must be :system, got: #{inspect(scope)}"
  end

  defp latest(%DateTime{} = stored, %DateTime{} = now) do
    if DateTime.compare(now, stored) == :lt, do: stored, else: now
  end

  defp latest(_stored, now), do: now

  @impl Docket.Backend.RunStore
  def release_claim(backend, :system, run_id, claim_token, now) do
    state_update(backend, fn state ->
      case fetch_record(state, run_id) do
        {:ok, %{claim_token: ^claim_token} = record}
        when is_binary(claim_token) and byte_size(claim_token) > 0 ->
          put_in(
            state.runs[run_id],
            %{record | claim_token: nil, claimed_at: nil, wake_at: now}
          )

        _ ->
          state
      end
    end)
  end

  def release_claim(_backend, scope, _run_id, _claim_token, _now) do
    raise ArgumentError, "release_claim scope must be :system, got: #{inspect(scope)}"
  end

  @impl Docket.Backend.RunStore
  def abandon_claim(backend, :system, run_id, claim_token, policy) do
    validate_abandon_policy!(policy)

    state_get_and_update(backend, fn state ->
      case fetch_record(state, run_id) do
        {:ok, %{claim_token: ^claim_token} = record}
        when is_binary(claim_token) and byte_size(claim_token) > 0 ->
          apply_abandon(state, run_id, record, policy)

        _ ->
          {{:ok, :stale}, state}
      end
    end)
  end

  def abandon_claim(_backend, scope, _run_id, _claim_token, _policy) do
    raise ArgumentError, "abandon_claim scope must be :system, got: #{inspect(scope)}"
  end

  @impl Docket.Backend.RunStore
  def commit(backend, scope, proposal) do
    validate_scope!(scope)

    state_get_and_update(backend, fn state ->
      with :ok <- validate_advance_commit(proposal),
           :ok <- validate_next_sequence(proposal),
           {:ok, record} <- fetch_scoped_record(state, scope, proposal.run.id),
           :ok <- validate_immutable_binding(record.run, proposal.run),
           :ok <- validate_fence(record, proposal) do
        now = current_time(state)

        record =
          record
          |> Map.put(:run, proposal.run)
          |> Map.put(:latest_checkpoint_type, proposal.checkpoint_type)
          |> reset_operational_health()
          |> apply_schedule(proposal.schedule, now)

        {{:ok, proposal.run}, put_in(state.runs[proposal.run.id], record)}
      else
        {:error, reason} -> {{:error, reason}, state}
      end
    end)
  end

  @impl Docket.Backend.RunStore
  def mutate_run(backend, scope, run_id, mutation) when is_function(mutation, 1) do
    validate_scope!(scope)

    state_mutate(backend, fn state ->
      case fetch_scoped_record(state, scope, run_id) do
        {:ok, record} -> apply_mutation(state, run_id, record, mutation.(record.run))
        {:error, :not_found} -> {{:error, :not_found}, state}
      end
    end)
  end

  @impl Docket.Backend.RunStore
  def retry_poisoned_run(backend, scope, run_id, now) do
    validate_scope!(scope)

    state_get_and_update(backend, fn state ->
      case fetch_scoped_record(state, scope, run_id) do
        {:ok, %{run: run}} when run.status in [:done, :failed, :cancelled] ->
          {{:error, :inactive_run}, state}

        {:ok, %{poisoned_at: nil} = record} ->
          {{:ok, record.run}, state}

        {:ok, record} ->
          record = %{
            record
            | claim_token: nil,
              claimed_at: nil,
              wake_at: now,
              claim_attempts: 0,
              claim_abandons: 0,
              poisoned_at: nil,
              poison_reason: nil
          }

          {{:ok, record.run}, put_in(state.runs[run_id], record)}

        {:error, :not_found} ->
          {{:error, :not_found}, state}
      end
    end)
  end

  @impl Docket.Backend.EventStore
  def append_events(_backend, scope, _run_id, []) do
    validate_scope!(scope)
    :ok
  end

  def append_events(backend, scope, run_id, events) do
    validate_scope!(scope)

    state_get_and_update(backend, fn state ->
      with {:ok, record} <- fetch_scoped_record(state, scope, run_id),
           {:ok, merged} <- merge_events(record.events, run_id, events) do
        record = %{record | events: merged}
        {:ok, put_in(state.runs[run_id], record)}
      else
        {:error, reason} -> {{:error, reason}, state}
      end
    end)
  end

  @impl Docket.Backend.EventStore
  def fetch_event(backend, scope, run_id, seq) do
    validate_scope!(scope)

    unless is_integer(seq) and seq > 0 do
      raise ArgumentError, "event sequence must be a positive integer"
    end

    state_get(backend, fn state ->
      with {:ok, record} <- fetch_scoped_record(state, scope, run_id),
           {:ok, event} <- Map.fetch(record.events, seq) do
        {:ok, event}
      else
        _missing -> {:error, :not_found}
      end
    end)
  end

  @impl Docket.Backend.EventStore
  def fetch_latest_event(backend, scope, run_id) do
    validate_scope!(scope)

    state_get(backend, fn state ->
      with {:ok, record} <- fetch_scoped_record(state, scope, run_id) do
        latest =
          case Map.keys(record.events) do
            [] -> nil
            seqs -> Map.fetch!(record.events, Enum.max(seqs))
          end

        {:ok, latest}
      end
    end)
  end

  @impl Docket.Backend.EventStore
  def list_events(backend, scope, run_id, %{after_seq: after_seq, limit: limit}) do
    validate_scope!(scope)

    unless is_integer(after_seq) and after_seq >= 0 and is_integer(limit) and limit > 0 do
      raise ArgumentError, "event cursor must be non-negative and limit must be positive"
    end

    state_get(backend, fn state ->
      with {:ok, record} <- fetch_scoped_record(state, scope, run_id) do
        {:ok, build_event_page(record, after_seq, limit)}
      end
    end)
  end

  defp build_event_page(record, after_seq, limit) do
    seqs = Map.keys(record.events)

    {oldest, latest} =
      case seqs do
        [] -> {nil, nil}
        _ -> {Enum.min(seqs), Enum.max(seqs)}
      end

    events =
      record.events
      |> Enum.filter(fn {seq, _event} -> seq > after_seq end)
      |> Enum.sort_by(&elem(&1, 0))
      |> Enum.take(limit)
      |> Enum.map(&elem(&1, 1))

    Docket.EventPage.new(events, after_seq, oldest, latest, record.run.event_seq)
  end

  # Shared-test inspection helpers. These deliberately bypass public
  # tenant policy and never participate in application-facing code.

  def events(backend, run_id) do
    case state_get(backend, &fetch_record(&1, run_id)) do
      {:ok, record} -> ordered_events(record.events)
      :error -> nil
    end
  end

  def events(backend, scope, run_id) do
    case list_events(backend, scope, run_id, %{after_seq: 0, limit: 1_000_000}) do
      {:ok, %Docket.EventPage{events: events}} -> events
      {:error, :not_found} -> nil
    end
  end

  def claim(backend, run_id) do
    case record(backend, run_id) do
      nil -> nil
      record -> record.claim_token
    end
  end

  def wake_at(backend, run_id) do
    case record(backend, run_id) do
      nil -> nil
      record -> record.wake_at
    end
  end

  def record(backend, run_id) do
    case state_get(backend, &fetch_record(&1, run_id)) do
      {:ok, record} -> record
      :error -> nil
    end
  end

  def poison(backend, run_id, reason \\ "test") do
    state_update(backend, fn state ->
      update_in(state.runs[run_id], fn
        nil ->
          nil

        record ->
          %{
            record
            | claim_token: nil,
              claimed_at: nil,
              wake_at: nil,
              poisoned_at: current_time(state),
              poison_reason: reason
          }
      end)
    end)
  end

  defp new_record(run, tenant_id, checkpoint_type, wake_at) do
    %{
      run: run,
      tenant_id: tenant_id,
      wake_at: wake_at,
      claim_token: nil,
      claimed_at: nil,
      claim_attempts: 0,
      claim_abandons: 0,
      poisoned_at: nil,
      poison_reason: nil,
      latest_checkpoint_type: checkpoint_type,
      events: %{}
    }
  end

  defp run_info(record) do
    Docket.RunInfo.new!(
      run: record.run,
      wake_at: record.wake_at,
      claimed_at: record.claimed_at,
      claim_attempts: record.claim_attempts,
      claim_abandons: record.claim_abandons,
      poisoned_at: record.poisoned_at,
      poison_reason: record.poison_reason
    )
  end

  defp run_summary(record) do
    run = record.run

    Docket.RunSummary.new!(
      id: run.id,
      tenant_id: record.tenant_id,
      graph_id: run.graph_id,
      graph_hash: run.graph_hash,
      status: run.status,
      step: run.step,
      checkpoint_seq: run.checkpoint_seq,
      started_at: run.started_at,
      updated_at: run.updated_at,
      finished_at: run.finished_at
    )
  end

  defp run_record_matches?(record, graph_id, graph_hash, statuses) do
    run = record.run

    (is_nil(graph_id) or run.graph_id == graph_id) and
      (is_nil(graph_hash) or run.graph_hash == graph_hash) and
      (is_nil(statuses) or run.status in statuses)
  end

  defp run_record_before?(_record, nil), do: true

  defp run_record_before?(%{run: run}, {%DateTime{} = started_at, run_id}) do
    case DateTime.compare(run.started_at, started_at) do
      :lt -> true
      :gt -> false
      :eq -> run.id < run_id
    end
  end

  defp run_record_newer?(left, right) do
    case DateTime.compare(left.run.started_at, right.run.started_at) do
      :gt -> true
      :lt -> false
      :eq -> left.run.id >= right.run.id
    end
  end

  defp graph_versions(state, owner_scope, graph_id) do
    state.graphs
    |> Enum.flat_map(fn
      {{^owner_scope, ^graph_id, graph_hash}, version} -> [{graph_hash, version}]
      {_key, _version} -> []
    end)
    |> Enum.sort(fn {left_hash, left}, {right_hash, right} ->
      case DateTime.compare(left.published_at, right.published_at) do
        :gt -> true
        :lt -> false
        :eq -> left_hash >= right_hash
      end
    end)
  end

  defp graph_version_before?(_published_at, _graph_hash, nil), do: true

  defp graph_version_before?(published_at, graph_hash, {%DateTime{} = before_at, before_hash}) do
    case DateTime.compare(published_at, before_at) do
      :lt -> true
      :gt -> false
      :eq -> graph_hash < before_hash
    end
  end

  defp apply_abandon(state, run_id, record, policy) do
    cond do
      record.run.checkpoint_seq != policy.expected_checkpoint_seq ->
        {{:ok, :stale}, state}

      Map.get(policy, :non_poisoning, false) ->
        record = %{
          record
          | claim_token: nil,
            claimed_at: nil,
            claim_attempts: max(record.claim_attempts - 1, 0),
            claim_abandons: record.claim_abandons + 1,
            wake_at: non_poisoning_wake(policy, record.claim_abandons)
        }

        {{:ok, :rescheduled}, put_in(state.runs[run_id], record)}

      record.claim_abandons < policy.max_claim_abandons ->
        record = %{
          record
          | claim_token: nil,
            claimed_at: nil,
            claim_attempts: max(record.claim_attempts - 1, 0),
            claim_abandons: record.claim_abandons + 1,
            wake_at: policy.retry_at
        }

        {{:ok, :rescheduled}, put_in(state.runs[run_id], record)}

      true ->
        record = %{
          record
          | claim_token: nil,
            claimed_at: nil,
            claim_attempts: max(record.claim_attempts - 1, 0),
            wake_at: nil,
            poisoned_at: policy.now,
            poison_reason: "max_claim_abandons_exceeded"
        }

        {{:ok, :poisoned}, put_in(state.runs[run_id], record)}
    end
  end

  defp non_poisoning_wake(%{backoff: %{base_ms: base, cap_ms: cap}, now: now}, abandons) do
    DateTime.add(now, min(base * Integer.pow(2, min(abandons, 30)), cap), :millisecond)
  end

  defp non_poisoning_wake(policy, _abandons), do: policy.retry_at

  defp validate_abandon_policy!(%{
         expected_checkpoint_seq: seq,
         now: %DateTime{} = now,
         retry_at: %DateTime{} = retry_at,
         max_claim_abandons: max
       })
       when is_integer(seq) and seq >= 0 and is_integer(max) and max > 0 do
    if DateTime.compare(retry_at, now) == :lt do
      raise ArgumentError, "abandon policy retry_at must not precede now"
    end

    :ok
  end

  defp validate_abandon_policy!(policy) do
    raise ArgumentError, "invalid abandon policy: #{inspect(policy)}"
  end

  defp claim_due_records(state, policy) do
    candidates =
      state.runs
      |> Enum.filter(fn {_run_id, record} -> claim_candidate?(record, policy) end)
      |> Enum.sort_by(fn {run_id, record} -> claim_sort_key(run_id, record) end)
      |> select_candidates(policy.limit, Map.get(policy, :preference))

    {state, leases, poisoned} =
      Enum.reduce(candidates, {state, [], []}, fn {run_id, _record}, {state, leases, poisoned} ->
        record = Map.fetch!(state.runs, run_id)

        if record.claim_attempts < policy.max_claim_attempts do
          claim_attempt = record.claim_attempts + 1
          claim_token = state.token_generator.()

          unless is_binary(claim_token) and byte_size(claim_token) > 0 do
            raise ArgumentError, "claim token generator must return a non-empty binary"
          end

          record = %{
            record
            | wake_at: nil,
              claim_token: claim_token,
              claimed_at: policy.now,
              claim_attempts: claim_attempt
          }

          lease = %{
            run_id: run_id,
            owner_scope: record_owner_scope(record),
            graph_id: record.run.graph_id,
            graph_hash: record.run.graph_hash,
            checkpoint_seq: record.run.checkpoint_seq,
            claim_token: claim_token,
            claimed_at: policy.now,
            claim_attempt: claim_attempt,
            orphan_ttl_ms: policy.orphan_ttl_ms
          }

          {put_in(state.runs[run_id], record), [lease | leases], poisoned}
        else
          reason = "max_claim_attempts_exceeded"

          record = %{
            record
            | wake_at: nil,
              claim_token: nil,
              claimed_at: nil,
              poisoned_at: policy.now,
              poison_reason: reason
          }

          poison = %{run_id: run_id, poisoned_at: policy.now, poison_reason: reason}
          {put_in(state.runs[run_id], record), leases, [poison | poisoned]}
        end
      end)

    {state, Enum.reverse(leases), Enum.reverse(poisoned)}
  end

  # Mirrors the Postgres class-progress policy: with demand of at least two
  # and both classes present, the oldest candidate of each class is selected
  # before the remaining demand takes the sorted (oldest-eligible) order;
  # with demand of exactly one an optional preferred class is served first,
  # falling through when empty. No preference means class-neutral order.
  defp select_candidates(sorted, limit, preference) do
    cond do
      limit >= 2 ->
        heads =
          [:ready, :expired]
          |> Enum.map(fn class -> Enum.find(sorted, &(candidate_class(&1) == class)) end)
          |> Enum.reject(&is_nil/1)

        heads ++ Enum.take(Enum.reject(sorted, &(&1 in heads)), limit - length(heads))

      limit == 1 and preference in [:ready, :expired] ->
        case Enum.find(sorted, &(candidate_class(&1) == preference)) do
          nil -> Enum.take(sorted, 1)
          candidate -> [candidate]
        end

      true ->
        Enum.take(sorted, limit)
    end
  end

  defp candidate_class({_run_id, %{claim_token: nil}}), do: :ready
  defp candidate_class({_run_id, _claimed}), do: :expired

  defp claim_candidate?(record, policy) do
    record.run.status == :running and is_nil(record.poisoned_at) and
      (ready?(record, policy.now) or expired?(record, policy.now, policy.orphan_ttl_ms))
  end

  defp ready?(%{claim_token: nil, wake_at: %DateTime{} = wake_at}, now) do
    DateTime.compare(wake_at, now) in [:lt, :eq]
  end

  defp ready?(_record, _now), do: false

  defp expired?(%{claim_token: token, claimed_at: %DateTime{} = claimed_at}, now, ttl)
       when is_binary(token) do
    cutoff = DateTime.add(now, -ttl, :millisecond)
    DateTime.compare(claimed_at, cutoff) == :lt
  end

  defp expired?(_record, _now, _ttl), do: false

  defp claim_sort_key(run_id, %{claim_token: nil, wake_at: wake_at}) do
    {DateTime.to_unix(wake_at, :microsecond), 0, run_id}
  end

  defp claim_sort_key(run_id, %{claimed_at: claimed_at}) do
    {DateTime.to_unix(claimed_at, :microsecond), 1, run_id}
  end

  defp validate_advance_commit(%{
         run: run,
         expected_checkpoint_seq: expected_seq,
         claim_token: claim_token,
         checkpoint_type: checkpoint_type,
         schedule: schedule
       }) do
    cond do
      not is_struct(run, Docket.Run) -> {:error, :invalid_commit}
      not is_integer(expected_seq) or expected_seq < 0 -> {:error, :invalid_commit}
      not is_binary(claim_token) or byte_size(claim_token) == 0 -> {:error, :invalid_commit}
      checkpoint_type not in Docket.Checkpoint.types() -> {:error, :invalid_commit}
      not valid_schedule?(schedule) -> {:error, :invalid_commit}
      not schedule_matches_status?(schedule, run.status) -> {:error, :invalid_commit}
      Docket.Run.validate_failure(run) != :ok -> {:error, :invalid_commit}
      true -> :ok
    end
  end

  defp validate_advance_commit(_proposal), do: {:error, :invalid_commit}

  defp validate_fence(record, proposal) do
    if record.run.checkpoint_seq == proposal.expected_checkpoint_seq and
         record.claim_token == proposal.claim_token do
      :ok
    else
      {:error, :stale_fence}
    end
  end

  defp validate_next_sequence(proposal) do
    if proposal.run.checkpoint_seq == proposal.expected_checkpoint_seq + 1 do
      :ok
    else
      {:error, :invalid_commit}
    end
  end

  defp validate_immutable_binding(stored_run, proposed_run) do
    if stored_run.id == proposed_run.id and stored_run.graph_id == proposed_run.graph_id and
         stored_run.graph_hash == proposed_run.graph_hash and
         stored_run.started_at == proposed_run.started_at do
      :ok
    else
      {:error, :invalid_commit}
    end
  end

  defp valid_schedule?(:retain_claim), do: true
  defp valid_schedule?({:release_claim, :immediate}), do: true
  defp valid_schedule?({:release_claim, :external}), do: true
  defp valid_schedule?({:release_claim, :terminal}), do: true
  defp valid_schedule?({:release_claim, {:at, %DateTime{}}}), do: true
  defp valid_schedule?(_schedule), do: false

  defp schedule_matches_status?(:retain_claim, :running), do: true
  defp schedule_matches_status?({:release_claim, :immediate}, :running), do: true
  defp schedule_matches_status?({:release_claim, {:at, %DateTime{}}}, :running), do: true
  defp schedule_matches_status?({:release_claim, :external}, :waiting), do: true

  defp schedule_matches_status?({:release_claim, :terminal}, status)
       when status in [:done, :failed, :cancelled],
       do: true

  defp schedule_matches_status?(_schedule, _status), do: false

  defp apply_schedule(record, :retain_claim, now) do
    %{record | wake_at: nil, claimed_at: now}
  end

  defp apply_schedule(record, {:release_claim, :immediate}, now) do
    %{record | wake_at: now, claim_token: nil, claimed_at: nil}
  end

  defp apply_schedule(record, {:release_claim, {:at, wake_at}}, _now) do
    %{record | wake_at: wake_at, claim_token: nil, claimed_at: nil}
  end

  defp apply_schedule(record, {:release_claim, reason}, _now)
       when reason in [:external, :terminal] do
    %{record | wake_at: nil, claim_token: nil, claimed_at: nil}
  end

  defp apply_mutation(
         state,
         run_id,
         record,
         {:commit, proposed_run, checkpoint_type, schedule, opaque}
       ) do
    if valid_mutation?(record, run_id, proposed_run, checkpoint_type, schedule) do
      record =
        record
        |> Map.put(:run, proposed_run)
        |> Map.put(:latest_checkpoint_type, checkpoint_type)
        |> reset_operational_health()
        |> apply_schedule(schedule, current_time(state))

      {{:ok, {:committed, opaque}}, put_in(state.runs[run_id], record)}
    else
      {{:error, :invalid_mutation}, state}
    end
  end

  defp apply_mutation(state, _run_id, _record, {:no_change, opaque}) do
    {{:ok, {:unchanged, opaque}}, state}
  end

  defp apply_mutation(state, _run_id, _record, {:error, reason}) do
    {{:error, reason}, state}
  end

  defp apply_mutation(state, _run_id, _record, _invalid) do
    {{:error, :invalid_mutation}, state}
  end

  defp valid_mutation?(record, run_id, proposed_run, checkpoint_type, schedule) do
    is_struct(proposed_run, Docket.Run) and proposed_run.id == run_id and
      proposed_run.id == record.run.id and
      proposed_run.graph_id == record.run.graph_id and
      proposed_run.graph_hash == record.run.graph_hash and
      proposed_run.started_at == record.run.started_at and
      proposed_run.checkpoint_seq == record.run.checkpoint_seq + 1 and
      checkpoint_type in Docket.Checkpoint.types() and schedule != :retain_claim and
      valid_schedule?(schedule) and schedule_matches_status?(schedule, proposed_run.status) and
      Docket.Run.validate_failure(proposed_run) == :ok
  end

  defp valid_initialized_run?(run, checkpoint_type, wake_at) do
    is_struct(run, Docket.Run) and run.status == :running and nonempty_binary?(run.id) and
      nonempty_binary?(run.graph_id) and nonempty_binary?(run.graph_hash) and
      is_integer(run.checkpoint_seq) and run.checkpoint_seq >= 1 and
      checkpoint_type == :run_initialized and
      is_struct(run.started_at, DateTime) and is_struct(run.updated_at, DateTime) and
      is_struct(wake_at, DateTime) and Docket.Run.validate_durable(run) == :ok
  end

  defp nonempty_binary?(value), do: is_binary(value) and byte_size(value) > 0

  defp reset_operational_health(record) do
    %{
      record
      | claim_attempts: 0,
        claim_abandons: 0,
        poisoned_at: nil,
        poison_reason: nil
    }
  end

  defp merge_events(existing, run_id, incoming) when is_list(incoming) do
    Enum.reduce_while(incoming, {:ok, existing}, fn event, {:ok, accumulated} ->
      cond do
        event.run_id != run_id ->
          {:halt, {:error, :event_run_mismatch}}

        not is_integer(event.seq) or event.seq <= 0 ->
          {:halt, {:error, :invalid_event_sequence}}

        previous = Map.get(accumulated, event.seq) ->
          if previous == event,
            do: {:cont, {:ok, accumulated}},
            else: {:halt, {:error, :event_conflict}}

        true ->
          {:cont, {:ok, Map.put(accumulated, event.seq, event)}}
      end
    end)
  end

  defp merge_events(_existing, _run_id, _incoming), do: {:error, :invalid_events}

  defp ordered_events(events) do
    events
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.map(&elem(&1, 1))
  end

  defp fetch_record(state, run_id), do: Map.fetch(state.runs, run_id)

  defp fetch_scoped_record(state, scope, run_id) do
    with {:ok, record} <- fetch_record(state, run_id),
         true <- scope_matches?(record, scope) do
      {:ok, record}
    else
      _ -> {:error, :not_found}
    end
  end

  defp scope_matches?(_record, :system), do: true
  defp scope_matches?(%{tenant_id: nil}, :tenantless), do: true
  defp scope_matches?(%{tenant_id: tenant_id}, {:tenant, tenant_id}), do: true
  defp scope_matches?(_record, _scope), do: false

  defp owner_tenant_id!(:tenantless), do: nil

  defp owner_tenant_id!({:tenant, tenant_id})
       when is_binary(tenant_id) and byte_size(tenant_id) > 0 do
    tenant_id
  end

  defp owner_tenant_id!(scope) do
    raise ArgumentError,
          "run owner scope must be :tenantless or {:tenant, tenant_id}, got: #{inspect(scope)}"
  end

  defp validate_owner_scope!(scope) do
    _tenant_id = owner_tenant_id!(scope)
    :ok
  end

  defp record_owner_scope(%{tenant_id: nil}), do: :tenantless
  defp record_owner_scope(%{tenant_id: tenant_id}), do: {:tenant, tenant_id}

  defp validate_scope!(:system), do: :ok
  defp validate_scope!(:tenantless), do: :ok
  defp validate_scope!({:tenant, tenant_id}) when is_binary(tenant_id), do: :ok

  defp validate_scope!(scope) do
    raise ArgumentError,
          "scope must be :system, :tenantless, or {:tenant, tenant_id}, got: #{inspect(scope)}"
  end

  defp validate_claim_policy!(%{
         now: %DateTime{},
         limit: limit,
         orphan_ttl_ms: orphan_ttl_ms,
         max_claim_attempts: max_claim_attempts
       })
       when is_integer(limit) and limit > 0 and is_integer(orphan_ttl_ms) and
              orphan_ttl_ms >= 0 and is_integer(max_claim_attempts) and
              max_claim_attempts > 0,
       do: :ok

  defp validate_claim_policy!(policy) do
    raise ArgumentError, "invalid claim policy: #{inspect(policy)}"
  end

  defp current_time(state), do: state.clock.()

  defp generate_claim_token do
    18
    |> :crypto.strong_rand_bytes()
    |> Base.url_encode64(padding: false)
  end

  defp validate_transaction_result({:ok, _value} = result), do: result
  defp validate_transaction_result({:error, _reason} = result), do: result

  defp validate_transaction_result(other) do
    raise ArgumentError,
          "backend transaction must return {:ok, value} or {:error, reason}, got: #{inspect(other)}"
  end

  defp mark_rollback_only(agent), do: Agent.update(agent, fn _rollback_only? -> true end)

  defp state_get(%Transaction{agent: agent}, fun), do: Agent.get(agent, fun)

  defp state_get(backend, fun) do
    ensure_root_context!(backend)
    Agent.get(backend, fun)
  end

  defp state_get_and_update(%Transaction{agent: agent}, fun) do
    with_agent_lock(agent, fn -> Agent.get_and_update(agent, fun) end)
  end

  defp state_get_and_update(backend, fun) do
    with_root_lock(backend, fn -> Agent.get_and_update(backend, fun) end)
  end

  defp state_update(%Transaction{agent: agent}, fun) do
    with_agent_lock(agent, fn -> Agent.update(agent, fun) end)
  end

  defp state_update(backend, fun) do
    with_root_lock(backend, fn -> Agent.update(backend, fun) end)
  end

  # Runs caller-provided mutation code in the caller, not inside the Agent.
  # That keeps exceptions from crashing the backend process.
  defp state_mutate(%Transaction{agent: agent}, fun) do
    with_agent_lock(agent, fn -> mutate_agent(agent, fun) end)
  end

  defp state_mutate(backend, fun) do
    with_root_lock(backend, fn -> mutate_agent(backend, fun) end)
  end

  defp mutate_agent(agent, fun) do
    state = Agent.get(agent, & &1)
    {reply, next_state} = fun.(state)
    Agent.update(agent, fn _state -> next_state end)
    reply
  end

  defp with_root_lock(backend, fun) do
    ensure_root_context!(backend)
    with_lock({__MODULE__, backend}, fun)
  end

  defp with_agent_lock(agent, fun), do: with_lock({__MODULE__, agent}, fun)

  defp with_lock(resource, fun) do
    case :global.trans({resource, self()}, fun, [node()]) do
      :aborted -> raise "memory backend lock acquisition aborted"
      result -> result
    end
  end

  defp ensure_root_context!(backend) do
    if Process.get(transaction_process_key(backend)) do
      raise ArgumentError,
            "root backend context used inside a transaction; pass the transaction context instead"
    end
  end

  defp transaction_process_key(backend), do: {__MODULE__, :transaction, backend}

  defp restore_process_value(key, nil), do: Process.delete(key)
  defp restore_process_value(key, value), do: Process.put(key, value)
end
