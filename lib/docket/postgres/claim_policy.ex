if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.ClaimPolicy do
    @moduledoc """
    Internal construction and admission boundary for PostgreSQL claims.

    A runtime resolves one configured implementation during backend setup. The
    boundary constructs the effective poll policy from that instance-owned
    configuration and the caller's clock, demand, and ready/expired preference,
    then delegates one atomic `RunStore.claim_due/3` operation.

    Implementations must preserve the focused run-store input and output
    contract. They may enrich the effective policy for a different PostgreSQL
    admission engine, but must not split admission into application-side reads
    followed by writes.
    """

    @default_implementation Docket.Postgres.ClaimPolicy.Legacy

    @enforce_keys [:implementation, :implementation_state]
    defstruct @enforce_keys

    @type runtime_input :: %{
            required(:now) => DateTime.t(),
            required(:limit) => pos_integer(),
            required(:orphan_ttl_ms) => non_neg_integer(),
            required(:max_claim_attempts) => pos_integer(),
            optional(:preference) => :ready | :expired | nil
          }

    @opaque t :: %__MODULE__{
              implementation: module(),
              implementation_state: term()
            }

    @callback init(keyword()) :: {:ok, state :: term()} | {:error, term()}

    @callback claim_due(
                run_store :: module(),
                Docket.Backend.ctx(),
                :system,
                Docket.Backend.RunStore.claim_policy(),
                state :: term()
              ) ::
                {:ok, Docket.Backend.RunStore.claim_batch()} | {:error, term()}

    @doc false
    @spec new(keyword()) :: t()
    def new(config \\ []) do
      unless Keyword.keyword?(config) do
        raise ArgumentError, ":claim_policy must be a keyword list, got: #{inspect(config)}"
      end

      {implementation, implementation_opts} =
        Keyword.pop(config, :implementation, @default_implementation)

      validate_implementation!(implementation)

      implementation_state =
        case implementation.init(implementation_opts) do
          {:ok, state} ->
            state

          {:error, reason} ->
            raise ArgumentError,
                  ":claim_policy implementation #{inspect(implementation)} rejected its " <>
                    "configuration: #{inspect(reason)}"

          other ->
            raise ArgumentError,
                  ":claim_policy implementation #{inspect(implementation)} init/1 must return " <>
                    "{:ok, state} or {:error, reason}, got: #{inspect(other)}"
        end

      %__MODULE__{
        implementation: implementation,
        implementation_state: implementation_state
      }
    end

    @doc false
    @spec implementation(t()) :: module()
    def implementation(%__MODULE__{implementation: implementation}), do: implementation

    @doc false
    @spec claim_due(t(), module(), Docket.Backend.ctx(), runtime_input()) ::
            {:ok, Docket.Backend.RunStore.claim_batch()} | {:error, term()}
    def claim_due(%__MODULE__{} = claim_policy, run_store, context, runtime_input)
        when is_atom(run_store) do
      effective = effective_policy!(claim_policy, runtime_input)
      started = System.monotonic_time()

      result =
        claim_policy.implementation.claim_due(
          run_store,
          context,
          :system,
          effective,
          claim_policy.implementation_state
        )

      emit_admission(claim_policy.implementation, effective.limit, started, result)
      result
    end

    def claim_due(%__MODULE__{}, run_store, _context, _runtime_input) do
      raise ArgumentError, "ClaimPolicy run store must be a module, got: #{inspect(run_store)}"
    end

    def claim_due(claim_policy, _run_store, _context, _runtime_input) do
      raise ArgumentError,
            "expected a resolved Docket.Postgres.ClaimPolicy, got: #{inspect(claim_policy)}"
    end

    defp effective_policy!(
           _claim_policy,
           %{
             now: %DateTime{} = now,
             limit: limit,
             orphan_ttl_ms: orphan_ttl_ms,
             max_claim_attempts: max_claim_attempts
           } = runtime_input
         )
         when is_integer(limit) and limit > 0 and is_integer(orphan_ttl_ms) and
                orphan_ttl_ms >= 0 and is_integer(max_claim_attempts) and
                max_claim_attempts > 0 do
      preference = Map.get(runtime_input, :preference)

      unless preference in [nil, :ready, :expired] do
        raise ArgumentError,
              "ClaimPolicy preference must be :ready or :expired, got: " <>
                inspect(preference)
      end

      %{
        now: now,
        limit: limit,
        orphan_ttl_ms: orphan_ttl_ms,
        max_claim_attempts: max_claim_attempts,
        preference: preference
      }
    end

    defp effective_policy!(_claim_policy, runtime_input) do
      raise ArgumentError,
            "ClaimPolicy runtime input requires DateTime now, positive limit/max_claim_attempts, " <>
              "non-negative orphan_ttl_ms, and optional preference of :ready or :expired, got: " <>
              inspect(runtime_input)
    end

    defp validate_implementation!(implementation) when is_atom(implementation) do
      loaded? = Code.ensure_loaded?(implementation)

      missing =
        for {name, arity} <- [init: 1, claim_due: 5],
            not (loaded? and function_exported?(implementation, name, arity)),
            do: "#{name}/#{arity}"

      if missing != [] do
        raise ArgumentError,
              ":claim_policy implementation #{inspect(implementation)} does not implement " <>
                "Docket.Postgres.ClaimPolicy; missing #{Enum.join(missing, ", ")}"
      end
    end

    defp validate_implementation!(implementation) do
      raise ArgumentError,
            ":claim_policy implementation must be a module, got: #{inspect(implementation)}"
    end

    defp emit_admission(implementation, demand, started, result) do
      {leases, poisoned} =
        case result do
          {:ok, %{leases: leases, poisoned: poisoned}}
          when is_list(leases) and is_list(poisoned) ->
            {length(leases), length(poisoned)}

          _ ->
            {0, 0}
        end

      :telemetry.execute(
        [:docket, :postgres, :claim_policy, :admission],
        %{
          duration: System.monotonic_time() - started,
          demand: demand,
          leases: leases,
          poisoned: poisoned
        },
        %{
          implementation: implementation,
          result: Docket.Telemetry.result_kind(result)
        }
      )
    end
  end

  defmodule Docket.Postgres.ClaimPolicy.Legacy do
    @moduledoc false

    @behaviour Docket.Postgres.ClaimPolicy

    @impl true
    def init([]), do: {:ok, nil}
    def init(options), do: {:error, {:unknown_options, Keyword.keys(options)}}

    @impl true
    def claim_due(run_store, context, :system, policy, nil) do
      run_store.claim_due(context, :system, policy)
    end
  end
end
