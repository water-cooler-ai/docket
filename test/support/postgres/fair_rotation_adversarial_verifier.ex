if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Test.FairRotationAdversarialVerifier do
    @moduledoc false

    alias Docket.Test.ConcurrentAdmissionHarness.FairRotationOracle

    @doc false
    def assert_trace!(trace, opts) when is_list(trace) and is_list(opts) do
      first_sequence = Keyword.fetch!(opts, :first_database_call_sequence)
      committed_sequences = opts |> Keyword.fetch!(:committed_call_sequences) |> MapSet.new()
      durable_outcomes = opts |> Keyword.fetch!(:durable_outcome_ids) |> MapSet.new()

      call_sequences =
        trace |> Enum.map(&fetch_positive!(&1, :database_call_sequence)) |> Enum.uniq()

      expected_sequences =
        case call_sequences do
          [] -> fail!("adversarial trace must contain at least one call")
          sequences -> Enum.to_list(first_sequence..List.last(sequences))
        end

      unless call_sequences == expected_sequences do
        fail!(
          "database-authored call sequence is reordered or incomplete: " <>
            "observed #{inspect(call_sequences)}, expected #{inspect(expected_sequences)}"
        )
      end

      unless committed_sequences == MapSet.new(call_sequences) do
        fail!("database commit evidence does not exactly cover the trace calls")
      end

      traced_outcomes =
        Enum.reduce(trace, MapSet.new(), fn event, seen ->
          outcome_ids = Map.fetch!(event, :outcome_ids)

          unless is_list(outcome_ids) and length(outcome_ids) == Map.fetch!(event, :outcomes) and
                   Enum.all?(outcome_ids, &is_binary/1) and Enum.uniq(outcome_ids) == outcome_ids do
            fail!("trace outcome identities do not match the reported outcome count")
          end

          Enum.reduce(outcome_ids, seen, fn outcome_id, accumulated ->
            if MapSet.member?(accumulated, outcome_id) do
              fail!("trace repeats one outcome identity across the qualification window")
            end

            MapSet.put(accumulated, outcome_id)
          end)
        end)

      unless traced_outcomes == durable_outcomes do
        fail!("trace outcomes do not exactly match independent durable outcome evidence")
      end

      target = Keyword.fetch!(opts, :target)

      target_index =
        Enum.find_index(trace, fn event ->
          event.partition == target and event.disposition == :grant
        end) || fail!("adversarial trace contains no committed target grant")

      fairness_prefix = Enum.take(trace, target_index + 1)

      minimal_cohort =
        fairness_prefix
        |> Enum.filter(&(&1.disposition == :grant))
        |> Enum.map(& &1.partition)
        |> MapSet.new()
        |> MapSet.put(target)

      declared_cohort = opts |> Keyword.fetch!(:cohort) |> MapSet.new()

      unless declared_cohort == minimal_cohort do
        fail!(
          "declared cohort inflates or omits the target-plus-grant population: " <>
            "declared #{inspect(declared_cohort)}, observed #{inspect(minimal_cohort)}"
        )
      end

      normalized =
        Enum.map(trace, fn event ->
          sequence = Map.fetch!(event, :database_call_sequence)

          event
          |> Map.put(:call, sequence - first_sequence + 1)
          |> Map.drop([:database_call_sequence, :outcome_ids])
        end)

      oracle_opts =
        Keyword.drop(opts, [
          :first_database_call_sequence,
          :committed_call_sequences,
          :durable_outcome_ids
        ])

      FairRotationOracle.assert_trace!(normalized, oracle_opts)
    end

    defp fetch_positive!(event, key) do
      case Map.fetch!(event, key) do
        value when is_integer(value) and value > 0 -> value
        value -> fail!("#{key} must be positive, got: #{inspect(value)}")
      end
    end

    defp fail!(message), do: raise(ArgumentError, message)
  end
end
