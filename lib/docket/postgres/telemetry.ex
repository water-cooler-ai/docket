if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.Telemetry do
    @moduledoc """
    Bounded metric-label projections for PostgreSQL backend telemetry.

    The event catalog and measurements are documented in the telemetry guide.
    """

    @metric_metadata %{
      [:docket, :postgres, :dispatcher, :poll] => [:claim_policy, :result, :source],
      [:docket, :postgres, :dispatcher, :launch] => [:result],
      [:docket, :postgres, :dispatcher, :shutdown] => [:result],
      [:docket, :postgres, :notification] => [:result],
      [:docket, :postgres, :run_store, :claim] => [:preference, :fallback, :result],
      [:docket, :postgres, :claim_policy, :admission] => [
        :implementation,
        :result,
        :contention_phase
      ],
      [:docket, :postgres, :claim, :operation] => [:operation, :result],
      [:docket, :postgres, :claim, :attempt] => [:result],
      [:docket, :postgres, :claim, :poisoned] => [:reason],
      [:docket, :postgres, :admission, :release] => [:reason],
      [:docket, :postgres, :claim, :fence_lost] => [:stage, :result],
      [:docket, :postgres, :graph_cache, :fetch] => [:result],
      [:docket, :postgres, :graph, :fetch, :stop] => [:result],
      [:docket, :postgres, :graph, :fetch, :exception] => [:result],
      [:docket, :postgres, :graph, :compile, :stop] => [:result],
      [:docket, :postgres, :graph, :compile, :exception] => [:result],
      [:docket, :postgres, :vehicle, :stop] => [:result],
      [:docket, :postgres, :vehicle, :exception] => [:result],
      [:docket, :postgres, :vehicle, :discard] => [:stage, :result],
      [:docket, :postgres, :vehicle, :crash] => [:result],
      [:docket, :postgres, :run_codec] => [:operation, :result],
      [:docket, :postgres, :store] => [:operation, :result],
      [:docket, :postgres, :vehicle, :drain] => [:outcome, :budget],
      [:docket, :postgres, :pruner, :pass] => [:result]
    }

    @doc "Returns the bounded metadata projection safe to use as metric labels."
    @spec metric_metadata([atom()], map()) :: map()
    def metric_metadata([:docket, :postgres | _rest] = event, metadata) do
      Map.take(metadata, Map.get(@metric_metadata, event, []))
    end

    def metric_metadata(event, metadata), do: Docket.Telemetry.metric_metadata(event, metadata)
  end
end
