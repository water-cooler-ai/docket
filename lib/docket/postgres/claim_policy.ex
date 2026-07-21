if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.ClaimPolicy do
    @moduledoc """
    Instance-resolved PostgreSQL admission-plan boundary.

    `Docket.Postgres.RunStore.claim_due/3` is the sole admission entrypoint and
    the sole executor of policy plans. A selected implementation receives
    normalized policy values and pre-quoted identifiers, builds one data-only
    SQL plan, decodes that statement's rows, and owns its bounded admission
    observations. A policy may also implement one narrow startup configuration
    callback. Plan construction never receives a run-store module or query
    callback.
    """

    alias Docket.Postgres.Storage
    alias Docket.Runtime.Clock

    @default_implementation Docket.Postgres.ClaimPolicy.Legacy
    @max_observation_keys 32

    defmodule Plan do
      @moduledoc """
      One implementation-owned, data-only PostgreSQL admission operation.

      The selected implementation remains out-of-band; a plan cannot switch
      the decoder that `ClaimPolicy` invokes after execution.
      """

      @enforce_keys [:statement, :params, :decoder, :observation]
      defstruct @enforce_keys ++ [demand: nil]

      @type t :: %__MODULE__{
              statement: String.t(),
              params: [term()],
              decoder: term(),
              observation: map(),
              demand: pos_integer() | nil
            }
    end

    @type runtime_input :: %{
            required(:now) => DateTime.t(),
            required(:limit) => pos_integer(),
            required(:orphan_ttl_ms) => non_neg_integer(),
            required(:max_claim_attempts) => pos_integer(),
            optional(:preference) => :ready | :expired | nil
          }

    @type init_context :: %{
            required(:prefix) => String.t() | nil,
            required(:identifiers) => %{
              required(:runs) => String.t(),
              required(:claim_policy) => String.t(),
              required(:claim_partitions) => String.t()
            }
          }

    @type plan_context :: %{
            required(:prefix) => String.t() | nil,
            required(:identifiers) => %{
              required(:runs) => String.t(),
              required(:claim_policy) => String.t(),
              required(:claim_partitions) => String.t()
            }
          }

    @type claim_batch :: %{required(:leases) => [map()], required(:poisoned) => [map()]}
    @type claim_result :: {:ok, claim_batch()} | {:error, term()}

    @opaque t :: %__MODULE__{
              implementation: module(),
              implementation_state: term()
            }

    @enforce_keys [:implementation, :implementation_state]
    defstruct @enforce_keys

    @callback init(keyword(), init_context()) :: {:ok, state :: term()} | {:error, term()}
    @callback configure(
                Docket.Backend.ctx(),
                state :: term(),
                query :: (String.t(), [term()] -> {:ok, term()} | {:error, term()})
              ) :: :ok | {:error, term()}
    @callback build_plan(plan_context(), runtime_input(), state :: term()) :: Plan.t()

    @callback decode(rows :: [list()], decoder :: term(), state :: term()) ::
                {:ok, claim_batch(), observation :: map()}
                | {:error, reason :: term(), observation :: map()}

    @callback observe(
                plan_observation :: map(),
                decoded_observation :: map() | nil,
                claim_result(),
                duration :: integer(),
                state :: term()
              ) :: :ok

    @optional_callbacks configure: 3

    @doc false
    @spec new(keyword(), Docket.Backend.ctx()) :: t()
    def new(config, context) do
      unless Keyword.keyword?(config) do
        raise ArgumentError, ":claim_policy must be a keyword list, got: #{inspect(config)}"
      end

      if Enum.count(config, fn {key, _value} -> key == :implementation end) > 1 do
        raise ArgumentError, ":claim_policy contains duplicate keys: [:implementation]"
      end

      {implementation, implementation_opts} =
        Keyword.pop(config, :implementation, @default_implementation)

      validate_implementation!(implementation)

      implementation_state =
        case implementation.init(implementation_opts, init_context!(context)) do
          {:ok, state} ->
            state

          {:error, reason} ->
            raise ArgumentError,
                  ":claim_policy implementation #{inspect(implementation)} rejected its " <>
                    "configuration: #{inspect(reason)}"

          other ->
            raise ArgumentError,
                  ":claim_policy implementation #{inspect(implementation)} init/2 must return " <>
                    "{:ok, state} or {:error, reason}, got: #{inspect(other)}"
        end

      %__MODULE__{
        implementation: implementation,
        implementation_state: implementation_state
      }
    end

    @doc false
    @spec resolve(Docket.Backend.ctx()) :: t()
    def resolve(%{claim_policy: %__MODULE__{} = claim_policy}), do: claim_policy

    def resolve(%{claim_policy: invalid}) do
      raise ArgumentError,
            "Postgres context contains an invalid resolved ClaimPolicy: #{inspect(invalid)}"
    end

    def resolve(context) do
      _ = Storage.context!(context)

      raise ArgumentError,
            "Postgres claim admission requires a resolved ClaimPolicy; " <>
              "build the context with Docket.Postgres.context/1"
    end

    @doc false
    @spec implementation(t()) :: module()
    def implementation(%__MODULE__{implementation: implementation}), do: implementation

    @doc false
    @spec configures_on_startup?(t()) :: boolean()
    def configures_on_startup?(%__MODULE__{implementation: implementation}) do
      function_exported?(implementation, :configure, 3)
    end

    @doc false
    @spec configure(t(), Docket.Backend.ctx(), (String.t(), [term()] -> term())) ::
            :ok | {:error, term()}
    def configure(%__MODULE__{} = claim_policy, context, query) when is_function(query, 2) do
      if configures_on_startup?(claim_policy) do
        claim_policy.implementation.configure(
          context,
          claim_policy.implementation_state,
          query
        )
      else
        :ok
      end
    end

    @doc false
    @spec effective_policy!(runtime_input()) :: runtime_input()
    def effective_policy!(
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
        now: Clock.normalize!(now),
        limit: limit,
        orphan_ttl_ms: orphan_ttl_ms,
        max_claim_attempts: max_claim_attempts,
        preference: preference
      }
    end

    def effective_policy!(runtime_input) do
      raise ArgumentError,
            "ClaimPolicy runtime input requires DateTime now, positive limit/max_claim_attempts, " <>
              "non-negative orphan_ttl_ms, and optional preference of :ready or :expired, got: " <>
              inspect(runtime_input)
    end

    @doc false
    @spec build_plan(t(), Docket.Backend.ctx(), runtime_input()) :: Plan.t()
    def build_plan(%__MODULE__{} = claim_policy, context, effective_policy) do
      plan =
        claim_policy.implementation.build_plan(
          plan_context!(context),
          effective_policy,
          claim_policy.implementation_state
        )

      plan = validate_plan!(plan, claim_policy.implementation)
      %{plan | demand: effective_policy.limit}
    end

    @doc false
    @spec decode(t(), Plan.t(), [list()]) ::
            {:ok, claim_batch(), map()}
            | {:error, term(), map()}
            | {:error, {:claim_policy_decode_failed, term()}}
    def decode(%__MODULE__{} = claim_policy, %Plan{} = plan, rows) when is_list(rows) do
      try do
        case claim_policy.implementation.decode(
               rows,
               plan.decoder,
               claim_policy.implementation_state
             ) do
          {:ok, %{leases: leases, poisoned: poisoned} = batch, observation}
          when is_list(leases) and is_list(poisoned) and is_map(observation) ->
            validate_observation!(observation, :decoded, claim_policy.implementation)
            {:ok, batch, observation}

          {:error, reason, observation} when is_map(observation) ->
            validate_observation!(observation, :decoded, claim_policy.implementation)
            {:error, reason, observation}

          other ->
            {:error, {:claim_policy_decode_failed, {:invalid_return, other}}}
        end
      rescue
        exception ->
          {:error, {:claim_policy_decode_failed, {:raised, exception, __STACKTRACE__}}}
      catch
        kind, reason ->
          {:error, {:claim_policy_decode_failed, {kind, reason, __STACKTRACE__}}}
      end
    end

    @doc false
    @spec observe(t(), Plan.t(), map() | nil, claim_result(), integer()) :: :ok
    def observe(
          %__MODULE__{} = claim_policy,
          %Plan{} = plan,
          decoded_observation,
          result,
          started
        ) do
      duration = System.monotonic_time() - started

      _ =
        try do
          claim_policy.implementation.observe(
            plan.observation,
            decoded_observation,
            result,
            duration,
            claim_policy.implementation_state
          )
        rescue
          _exception -> :ok
        catch
          _kind, _reason -> :ok
        end

      emit_admission(
        claim_policy.implementation,
        plan,
        decoded_observation,
        duration,
        result
      )

      :ok
    end

    @doc false
    @spec plan_context!(Docket.Backend.ctx()) :: plan_context()
    def plan_context!(context) do
      {_repo, prefix} = Storage.context!(context)
      policy_context(prefix)
    end

    defp init_context!(context) do
      {_repo, prefix} = Storage.context!(context)
      policy_context(prefix)
    end

    defp policy_context(prefix) do
      %{
        prefix: prefix,
        identifiers: %{
          runs: Storage.qualified_table(prefix, "docket_runs"),
          claim_policy: Storage.qualified_table(prefix, "docket_claim_policy"),
          claim_partitions: Storage.qualified_table(prefix, "docket_claim_partitions")
        }
      }
    end

    defp validate_implementation!(implementation) when is_atom(implementation) do
      loaded? = Code.ensure_loaded?(implementation)

      missing =
        for {name, arity} <- [init: 2, build_plan: 3, decode: 3, observe: 5],
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

    defp validate_plan!(%Plan{} = plan, implementation) do
      cond do
        not (is_binary(plan.statement) and String.trim(plan.statement) != "") ->
          invalid_plan!(implementation, "a non-empty SQL statement")

        String.contains?(plan.statement, ";") ->
          invalid_plan!(implementation, "one SQL statement without a statement separator")

        not is_list(plan.params) or not data_only?(plan.params) ->
          invalid_plan!(implementation, "data-only SQL parameters")

        not data_only?(plan.decoder) ->
          invalid_plan!(implementation, "a data-only decoder contract")

        true ->
          validate_observation!(plan.observation, :plan, implementation)
          plan
      end
    end

    defp validate_plan!(other, implementation) do
      raise ArgumentError,
            "ClaimPolicy implementation #{inspect(implementation)} build_plan/3 must return " <>
              "a Docket.Postgres.ClaimPolicy.Plan, got: #{inspect(other)}"
    end

    defp invalid_plan!(implementation, requirement) do
      raise ArgumentError,
            "ClaimPolicy implementation #{inspect(implementation)} plan requires #{requirement}"
    end

    defp validate_observation!(observation, stage, implementation)
         when is_map(observation) and map_size(observation) <= @max_observation_keys do
      if data_only?(observation) do
        :ok
      else
        invalid_observation!(stage, implementation)
      end
    end

    defp validate_observation!(_observation, stage, implementation),
      do: invalid_observation!(stage, implementation)

    defp invalid_observation!(stage, implementation) do
      raise ArgumentError,
            "ClaimPolicy implementation #{inspect(implementation)} #{stage} observation must be " <>
              "a data-only map with at most #{@max_observation_keys} keys"
    end

    defp data_only?(value)
         when is_atom(value) or is_binary(value) or is_number(value) or is_boolean(value) or
                is_nil(value),
         do: true

    defp data_only?(value) when is_list(value), do: Enum.all?(value, &data_only?/1)
    defp data_only?(value) when is_tuple(value), do: value |> Tuple.to_list() |> data_only?()
    defp data_only?(%_{} = value), do: value |> Map.from_struct() |> data_only?()

    defp data_only?(value) when is_map(value) do
      Enum.all?(value, fn {key, nested} -> data_only?(key) and data_only?(nested) end)
    end

    defp data_only?(_value), do: false

    defp emit_admission(implementation, %Plan{} = plan, decoded_observation, duration, result) do
      {leases, poisoned} =
        case result do
          {:ok, %{leases: leases, poisoned: poisoned}}
          when is_list(leases) and is_list(poisoned) ->
            {length(leases), length(poisoned)}

          _ ->
            {0, 0}
        end

      contention_phase = contention_phase(implementation, decoded_observation, result)

      :telemetry.execute(
        [:docket, :postgres, :claim_policy, :admission],
        %{
          duration: duration,
          demand: plan.demand,
          leases: leases,
          poisoned: poisoned,
          contentions: if(contention_phase == :none, do: 0, else: 1)
        },
        %{
          implementation: implementation,
          result: Docket.Telemetry.result_kind(result),
          contention_phase: contention_phase
        }
      )
    end

    defp contention_phase(
           Docket.Postgres.ClaimPolicy.TenantFair,
           %{contention_phase: :policy_cursor},
           {:error, {:claim_policy_unavailable, :lock_contention}}
         ),
         do: :policy_cursor

    defp contention_phase(_implementation, _decoded_observation, _result), do: :none
  end
end
