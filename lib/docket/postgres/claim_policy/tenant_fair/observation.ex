if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.ClaimPolicy.TenantFair.Observation do
    @moduledoc """
    Closed, identity-free aggregate observation contract for TenantFair admission.

    The TenantFair plan declares this schema with `plan/0`. A successful
    decoder returns one complete `t:t/0` summary, including for a no-op. The
    ClaimPolicy facade validates the summary against the decoded batch before
    emitting it, so implementations cannot add telemetry keys or metadata.
    """

    @schema :tenant_fair_v1

    @count_fields [
      :eligible_partitions,
      :locked_partitions,
      :skipped_partitions,
      :cap_denied_partitions,
      :below_preferred_partitions,
      :default_policy_partitions,
      :override_policy_partitions,
      :running_partitions,
      :hold_new_partitions,
      :drain_partitions,
      :preferred_admissions,
      :borrowed_admissions,
      :ready_leases,
      :ready_poisoned,
      :expired_leases,
      :expired_poisoned,
      :candidate_rows_examined,
      :under_claimed,
      :ready_claim_wait_ms_count,
      :ready_claim_wait_ms_sum,
      :ready_claim_wait_ms_max,
      :expired_recovery_wait_ms_count,
      :expired_recovery_wait_ms_sum,
      :expired_recovery_wait_ms_max
    ]

    @optional_count_fields [
      :partition_lock_skip_delay_ms_count,
      :partition_lock_skip_delay_ms_sum,
      :partition_lock_skip_delay_ms_max
    ]

    @measurement_fields @count_fields ++ @optional_count_fields

    defmodule Plan do
      @moduledoc false
      @enforce_keys [:schema]
      defstruct @enforce_keys

      @type t :: %__MODULE__{schema: :tenant_fair_v1}
    end

    defstruct Enum.map(@count_fields, &{&1, 0}) ++
                Enum.map(@optional_count_fields, &{&1, nil})

    @type t :: %__MODULE__{}

    @doc false
    @spec schema() :: :tenant_fair_v1
    def schema, do: @schema

    @doc false
    @spec plan() :: Plan.t()
    def plan, do: %Plan{schema: @schema}

    @doc false
    @spec new!(keyword() | map()) :: t()
    def new!(attributes \\ %{}) do
      attributes = if is_list(attributes), do: Map.new(attributes), else: attributes

      unless is_map(attributes) do
        raise ArgumentError,
              "TenantFair admission observation requires a keyword list or map, got: " <>
                inspect(attributes)
      end

      unknown = Map.keys(attributes) -- @measurement_fields

      if unknown != [] do
        raise ArgumentError,
              "TenantFair admission observation contains unknown fields: #{inspect(unknown)}"
      end

      __MODULE__
      |> struct!(attributes)
      |> validate!()
    end

    @doc false
    @spec validate_plan!(term()) :: Plan.t()
    def validate_plan!(%Plan{schema: @schema} = plan), do: plan

    def validate_plan!(other) do
      raise ArgumentError,
            "TenantFair plan observation must declare schema #{@schema}, got: #{inspect(other)}"
    end

    @doc false
    @spec validate!(term()) :: t()
    def validate!(%__MODULE__{} = observation) do
      Enum.each(@count_fields, fn field ->
        value = Map.fetch!(observation, field)

        unless is_integer(value) and value >= 0 do
          invalid!("#{field} must be a non-negative integer", observation)
        end
      end)

      if observation.under_claimed not in [0, 1] do
        invalid!("under_claimed must be 0 or 1", observation)
      end

      validate_wait!(
        observation,
        :ready_claim_wait_ms_count,
        :ready_claim_wait_ms_sum,
        :ready_claim_wait_ms_max
      )

      validate_wait!(
        observation,
        :expired_recovery_wait_ms_count,
        :expired_recovery_wait_ms_sum,
        :expired_recovery_wait_ms_max
      )

      validate_optional_wait!(
        observation,
        :partition_lock_skip_delay_ms_count,
        :partition_lock_skip_delay_ms_sum,
        :partition_lock_skip_delay_ms_max
      )

      if observation.locked_partitions + observation.skipped_partitions >
           observation.eligible_partitions do
        invalid!(
          "locked_partitions + skipped_partitions exceeds eligible_partitions",
          observation
        )
      end

      if observation.cap_denied_partitions > observation.locked_partitions do
        invalid!("cap_denied_partitions exceeds locked_partitions", observation)
      end

      if observation.below_preferred_partitions > observation.locked_partitions do
        invalid!("below_preferred_partitions exceeds locked_partitions", observation)
      end

      if observation.cap_denied_partitions + observation.below_preferred_partitions >
           observation.locked_partitions do
        invalid!(
          "cap-denied and below-preferred partition counts overlap or exceed locked_partitions",
          observation
        )
      end

      if observation.default_policy_partitions + observation.override_policy_partitions !=
           observation.locked_partitions do
        invalid!("policy-source partition counts must equal locked_partitions", observation)
      end

      if observation.running_partitions + observation.hold_new_partitions +
           observation.drain_partitions != observation.locked_partitions do
        invalid!(
          "administrative-state partition counts must equal locked_partitions",
          observation
        )
      end

      observation
    end

    def validate!(other) do
      raise ArgumentError,
            "TenantFair decoded observation must use the closed #{@schema} schema, got: " <>
              inspect(other)
    end

    @doc false
    @spec validate_batch!(t(), map(), pos_integer()) :: t()
    def validate_batch!(
          %__MODULE__{} = observation,
          %{leases: leases, poisoned: poisoned},
          demand
        )
        when is_list(leases) and is_list(poisoned) and is_integer(demand) and demand > 0 do
      observation = validate!(observation)
      lease_count = observation.ready_leases + observation.expired_leases
      poison_count = observation.ready_poisoned + observation.expired_poisoned
      outcome_count = lease_count + poison_count

      if lease_count != length(leases) do
        invalid!("ready_leases + expired_leases must equal the decoded lease count", observation)
      end

      if poison_count != length(poisoned) do
        invalid!(
          "ready_poisoned + expired_poisoned must equal the decoded poison count",
          observation
        )
      end

      if outcome_count > demand do
        invalid!("portable outcomes exceed admission demand", observation)
      end

      if observation.preferred_admissions + observation.borrowed_admissions !=
           observation.ready_leases do
        invalid!(
          "preferred_admissions + borrowed_admissions must equal ready_leases",
          observation
        )
      end

      if observation.ready_claim_wait_ms_count != observation.ready_leases do
        invalid!("ready claim-wait count must equal ready_leases", observation)
      end

      expired_outcomes = observation.expired_leases + observation.expired_poisoned

      if observation.expired_recovery_wait_ms_count != expired_outcomes do
        invalid!("expired recovery-wait count must equal expired outcomes", observation)
      end

      if observation.candidate_rows_examined < outcome_count do
        invalid!("candidate_rows_examined cannot be smaller than outcomes", observation)
      end

      if observation.under_claimed == 1 and outcome_count >= demand do
        invalid!("under_claimed requires unfilled demand", observation)
      end

      observation
    end

    @doc false
    @spec measurements(t()) :: map()
    def measurements(%__MODULE__{} = observation) do
      observation
      |> Map.from_struct()
      |> Enum.reject(fn {_key, value} -> is_nil(value) end)
      |> Map.new()
    end

    defp validate_wait!(observation, count_field, sum_field, max_field) do
      count = Map.fetch!(observation, count_field)
      sum = Map.fetch!(observation, sum_field)
      maximum = Map.fetch!(observation, max_field)

      if count == 0 and (sum != 0 or maximum != 0) do
        invalid!("#{count_field} is zero but its sum/max is non-zero", observation)
      end

      if count > 0 and sum < maximum do
        invalid!("#{sum_field} must be at least #{max_field}", observation)
      end

      if count > 0 and sum > count * maximum do
        invalid!("#{sum_field} cannot exceed #{count_field} * #{max_field}", observation)
      end
    end

    defp validate_optional_wait!(observation, count_field, sum_field, max_field) do
      values = Enum.map([count_field, sum_field, max_field], &Map.fetch!(observation, &1))

      cond do
        Enum.all?(values, &is_nil/1) ->
          :ok

        Enum.all?(values, &(is_integer(&1) and &1 >= 0)) ->
          validate_wait!(observation, count_field, sum_field, max_field)

          if Map.fetch!(observation, count_field) > observation.skipped_partitions do
            invalid!("#{count_field} exceeds skipped_partitions", observation)
          end

        true ->
          invalid!(
            "optional partition lock skip-delay count/sum/max must be all set or all nil",
            observation
          )
      end
    end

    defp invalid!(requirement, observation) do
      raise ArgumentError,
            "invalid TenantFair admission observation: #{requirement}; got: " <>
              inspect(observation)
    end
  end
end
