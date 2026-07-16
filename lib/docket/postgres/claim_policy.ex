if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.ClaimPolicy do
    @moduledoc """
    Instance-resolved PostgreSQL admission-plan boundary.

    `Docket.Postgres.RunStore.claim_due/3` is the sole admission entrypoint and
    the sole executor of policy plans. A selected implementation receives
    normalized policy values and pre-quoted identifiers, builds one data-only
    SQL plan, decodes that statement's rows, and owns its bounded admission
    observations. It never receives a run-store module or query callback.
    """

    alias Docket.Postgres.Storage
    alias Docket.Postgres.ClaimPolicy.TenantFair.Observation
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
            required(:prefix) => String.t(),
            required(:identifiers) => identifiers()
          }

    @type plan_context :: %{
            required(:prefix) => String.t(),
            required(:identifiers) => identifiers()
          }

    @type identifiers :: %{
            required(:runs) => String.t(),
            required(:claim_policy) => String.t(),
            required(:claim_partitions) => String.t(),
            required(:claim_policy_receipts) => String.t(),
            required(:claim_policy_events) => String.t(),
            required(:claim_policy_holds) => String.t(),
            required(:claim_audit_exports) => String.t(),
            required(:claim_assertions) => String.t(),
            required(:claim_rollout) => String.t(),
            required(:claim_admission_gate) => String.t(),
            required(:claim_capabilities) => String.t()
          }

    @identifier_tables %{
      runs: "docket_runs",
      claim_policy: "docket_claim_policy",
      claim_partitions: "docket_claim_partitions",
      claim_policy_receipts: "docket_claim_policy_receipts",
      claim_policy_events: "docket_claim_policy_events",
      claim_policy_holds: "docket_claim_policy_holds",
      claim_audit_exports: "docket_claim_audit_exports",
      claim_assertions: "docket_claim_assertions",
      claim_rollout: "docket_claim_rollout",
      claim_admission_gate: "docket_claim_admission_gate",
      claim_capabilities: "docket_claim_capabilities"
    }

    @type claim_batch :: %{required(:leases) => [map()], required(:poisoned) => [map()]}
    @type claim_result :: {:ok, claim_batch()} | {:error, term()}

    @opaque t :: %__MODULE__{
              implementation: module(),
              implementation_state: term(),
              policy_context: plan_context()
            }

    @enforce_keys [:implementation, :implementation_state, :policy_context]
    defstruct @enforce_keys

    @callback init(keyword(), init_context()) :: {:ok, state :: term()} | {:error, term()}
    @callback build_plan(plan_context(), runtime_input(), state :: term()) :: Plan.t()

    @callback decode(rows :: [list()], decoder :: term(), state :: term()) ::
                {:ok, claim_batch(), observation :: map()}

    @callback observe(
                plan_observation :: map(),
                decoded_observation :: map() | nil,
                claim_result(),
                duration :: integer(),
                state :: term()
              ) :: :ok

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

      policy_context = init_context!(context)

      implementation_state =
        case implementation.init(implementation_opts, policy_context) do
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
        implementation_state: implementation_state,
        policy_context: policy_context
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
      _ = Storage.context!(context)

      plan =
        claim_policy.implementation.build_plan(
          claim_policy.policy_context,
          effective_policy,
          claim_policy.implementation_state
        )

      plan = validate_plan!(plan, claim_policy.implementation)
      %{plan | demand: effective_policy.limit}
    end

    @doc false
    @spec decode(t(), Plan.t(), [list()]) ::
            {:ok, claim_batch(), map()} | {:error, {:claim_policy_decode_failed, term()}}
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
            validate_admission_observation!(plan, observation, batch, claim_policy.implementation)
            {:ok, batch, observation}

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

      emit_admission_observation(
        claim_policy.implementation,
        plan,
        decoded_observation,
        duration,
        result
      )

      emit_admission(claim_policy.implementation, plan, duration, result)
      :ok
    end

    @doc false
    @spec plan_context!(Docket.Backend.ctx()) :: plan_context()
    def plan_context!(context) do
      {repo, prefix} = Storage.context!(context)
      policy_context(Storage.physical_prefix!(repo, prefix))
    end

    defp init_context!(context) do
      {repo, prefix} = Storage.context!(context)
      policy_context(Storage.physical_prefix!(repo, prefix))
    end

    defp policy_context(prefix) do
      %{
        prefix: prefix,
        identifiers:
          Map.new(@identifier_tables, fn {key, table} ->
            {key, Storage.qualified_table(prefix, table)}
          end)
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
        validate_reserved_admission_observation!(observation, stage, implementation)
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

    defp validate_reserved_admission_observation!(observation, stage, implementation) do
      case Map.fetch(observation, :admission_observation) do
        :error ->
          :ok

        {:ok, reserved} ->
          try do
            case stage do
              :plan -> Observation.validate_plan!(reserved)
              :decoded -> Observation.validate!(reserved)
            end

            :ok
          rescue
            exception in ArgumentError ->
              raise ArgumentError,
                    "ClaimPolicy implementation #{inspect(implementation)} has an invalid " <>
                      "#{stage} admission observation: #{Exception.message(exception)}"
          end
      end
    end

    defp validate_admission_observation!(%Plan{} = plan, decoded, batch, implementation) do
      plan_contract = Map.get(plan.observation, :admission_observation)
      decoded_contract = Map.get(decoded, :admission_observation)

      cond do
        is_nil(plan_contract) and is_nil(decoded_contract) ->
          :ok

        is_nil(plan_contract) ->
          invalid_admission_contract!(
            implementation,
            "decoded observation opted in without a plan declaration"
          )

        is_nil(decoded_contract) ->
          invalid_admission_contract!(
            implementation,
            "successful decode omitted the declared TenantFair observation"
          )

        true ->
          try do
            Observation.validate_plan!(plan_contract)
            Observation.validate_batch!(decoded_contract, batch, plan.demand)
            :ok
          rescue
            exception in ArgumentError ->
              invalid_admission_contract!(implementation, Exception.message(exception))
          end
      end
    end

    defp invalid_admission_contract!(implementation, requirement) do
      raise ArgumentError,
            "ClaimPolicy implementation #{inspect(implementation)} admission observation " <>
              "contract failed: #{requirement}"
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

    defp emit_admission_observation(
           implementation,
           %Plan{} = plan,
           decoded_observation,
           duration,
           result
         ) do
      case Map.get(plan.observation, :admission_observation) do
        %Observation.Plan{} ->
          emit_declared_admission_observation(
            implementation,
            plan,
            decoded_observation,
            duration,
            result
          )

        _other ->
          :ok
      end
    end

    defp emit_declared_admission_observation(
           implementation,
           %Plan{} = plan,
           %{admission_observation: %Observation{} = observation},
           duration,
           {:ok, %{leases: leases, poisoned: poisoned}}
         ) do
      outcomes = length(leases) + length(poisoned)

      measurements =
        observation
        |> Observation.measurements()
        |> Map.merge(%{
          duration: duration,
          demand: plan.demand,
          leases: length(leases),
          poisoned: length(poisoned),
          outcomes: outcomes,
          unfilled_demand: max(plan.demand - outcomes, 0),
          steals: observation.expired_leases
        })

      metadata = %{
        implementation: implementation,
        schema: Observation.schema(),
        result: :ok,
        observation_status: :available,
        admission_class:
          bounded_class(
            observation.preferred_admissions,
            observation.borrowed_admissions,
            :preferred,
            :borrowed
          ),
        work_class:
          bounded_class(
            observation.ready_leases + observation.ready_poisoned,
            observation.expired_leases + observation.expired_poisoned,
            :ready,
            :expired
          ),
        batch_shape: batch_shape(observation, outcomes, plan.demand),
        policy_source:
          bounded_class(
            observation.default_policy_partitions,
            observation.override_policy_partitions,
            :default,
            :override
          ),
        admin_state: admin_state(observation)
      }

      :telemetry.execute(
        [:docket, :postgres, :claim_policy, :admission, :observation],
        measurements,
        metadata
      )
    end

    defp emit_declared_admission_observation(
           implementation,
           %Plan{} = plan,
           _decoded_observation,
           duration,
           result
         ) do
      :telemetry.execute(
        [:docket, :postgres, :claim_policy, :admission, :observation],
        %{duration: duration, demand: plan.demand},
        %{
          implementation: implementation,
          schema: Observation.schema(),
          result: Docket.Telemetry.result_kind(result),
          observation_status: :unavailable,
          admission_class: :none,
          work_class: :none,
          batch_shape: :error,
          policy_source: :none,
          admin_state: :none
        }
      )
    end

    defp bounded_class(0, 0, _left, _right), do: :none
    defp bounded_class(left_count, 0, left, _right) when left_count > 0, do: left
    defp bounded_class(0, right_count, _left, right) when right_count > 0, do: right

    defp bounded_class(left_count, right_count, _left, _right)
         when left_count > 0 and right_count > 0,
         do: :mixed

    defp admin_state(%Observation{} = observation) do
      active =
        [
          {:running, observation.running_partitions},
          {:hold_new, observation.hold_new_partitions},
          {:drain, observation.drain_partitions}
        ]
        |> Enum.filter(fn {_state, count} -> count > 0 end)

      case active do
        [] -> :none
        [{state, _count}] -> state
        _mixed -> :mixed
      end
    end

    defp batch_shape(%Observation{under_claimed: 1}, _outcomes, _demand), do: :under_claim
    defp batch_shape(_observation, 0, _demand), do: :no_op
    defp batch_shape(_observation, demand, demand), do: :full
    defp batch_shape(_observation, _outcomes, _demand), do: :partial

    defp emit_admission(implementation, %Plan{} = plan, duration, result) do
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
          duration: duration,
          demand: plan.demand,
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
end
