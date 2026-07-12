if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.RunCodec do
    @moduledoc """
    Maps one durable `Docket.Run` to its Postgres row.

    Postgres columns are the sole copy of fields needed for identity,
    claiming, scheduling, inspection, and constraints. Every other run field
    is one opaque ETF value in `state`.
    """

    alias Docket.{DurableCodec, Run}

    @column_fields [
      run_id: :id,
      graph_id: :graph_id,
      graph_hash: :graph_hash,
      status: :status,
      step: :step,
      checkpoint_seq: :checkpoint_seq,
      started_at: :started_at,
      updated_at: :updated_at,
      finished_at: :finished_at
    ]

    @state_fields [
      :input,
      :output,
      :failure,
      :channels,
      :changed_channels,
      :pending_nodes,
      :active_tasks,
      :pending_writes,
      :interrupts,
      :timers,
      :event_seq,
      :metadata
    ]

    @run_fields %Run{} |> Map.from_struct() |> Map.keys()
    @partition_fields Keyword.values(@column_fields) ++ @state_fields

    unless Enum.sort(@partition_fields) == Enum.sort(@run_fields) and
             length(@partition_fields) == length(Enum.uniq(@partition_fields)) do
      raise "RunCodec fields must partition every Docket.Run field exactly once"
    end

    @type row_attrs :: %{
            required(:run_id) => String.t(),
            required(:graph_id) => String.t(),
            required(:graph_hash) => String.t() | nil,
            required(:status) => Run.durable_status(),
            required(:step) => non_neg_integer(),
            required(:state) => binary(),
            required(:checkpoint_seq) => non_neg_integer(),
            required(:started_at) => DateTime.t() | nil,
            required(:updated_at) => DateTime.t() | nil,
            required(:finished_at) => DateTime.t() | nil
          }

    @spec dump(Run.t()) :: {:ok, row_attrs()} | {:error, Docket.Error.t()}
    def dump(%Run{} = run) do
      started = System.monotonic_time()

      result = do_dump(run)

      bytes =
        case result do
          {:ok, %{state: state}} -> byte_size(state)
          _ -> 0
        end

      emit_codec(:dump, started, result, bytes)
      result
    end

    defp do_dump(%Run{} = run) do
      with :ok <- Run.validate_durable(run) do
        validate_timestamps!(run)

        attrs =
          Map.new(@column_fields, fn {column, field} ->
            {column, Map.fetch!(run, field)}
          end)

        state = Map.take(run, @state_fields)
        {:ok, Map.put(attrs, :state, DurableCodec.encode!(:run, state))}
      end
    rescue
      error in Docket.Error -> {:error, error}
    end

    @spec load(Docket.Postgres.Schemas.Run.t() | map()) ::
            {:ok, Run.t()} | {:error, Docket.Error.t()}
    def load(row) when is_map(row) do
      started = System.monotonic_time()

      bytes =
        case Map.get(row, :state) do
          state when is_binary(state) -> byte_size(state)
          _ -> 0
        end

      result = do_load(row)
      emit_codec(:load, started, result, bytes)
      result
    end

    def load(other),
      do: {:error, corruption("Postgres run row must be a map, got: #{inspect(other)}")}

    defp do_load(row) do
      state = row |> fetch!(:state) |> DurableCodec.decode!(:run)

      unless is_map(state) and not is_struct(state) and
               Enum.sort(Map.keys(state)) == Enum.sort(@state_fields) do
        raise corruption("Postgres run state has the wrong fields")
      end

      run_fields =
        Enum.reduce(@column_fields, state, fn {column, field}, fields ->
          Map.put(fields, field, fetch!(row, column))
        end)

      run = struct!(Run, run_fields)
      validate_timestamps!(run)

      case Run.validate_durable(run) do
        :ok -> {:ok, run}
        {:error, error} -> raise error
      end
    rescue
      error in Docket.Error -> {:error, as_corruption(error)}
    end

    defp emit_codec(operation, started, result, bytes) do
      :telemetry.execute(
        [:docket, :postgres, :run_codec],
        %{duration: System.monotonic_time() - started, bytes: bytes},
        %{operation: operation, result: Docket.Telemetry.result_kind(result)}
      )
    end

    @spec load!(Docket.Postgres.Schemas.Run.t() | map()) :: Run.t()
    def load!(row) do
      case load(row) do
        {:ok, run} -> run
        {:error, error} -> raise error
      end
    end

    defp fetch!(row, field) do
      case Map.fetch(row, field) do
        {:ok, value} ->
          value

        :error ->
          raise corruption("Postgres run row is missing #{inspect(field)}", %{field: field})
      end
    end

    defp validate_timestamps!(run) do
      for field <- [:started_at, :updated_at, :finished_at],
          timestamp = Map.fetch!(run, field),
          not database_timestamp?(timestamp) do
        raise Docket.Error,
          type: :invalid_run,
          message: "run #{field} must be nil or a six-digit UTC DateTime"
      end
    end

    defp database_timestamp?(nil), do: true

    defp database_timestamp?(
           %DateTime{
             calendar: Calendar.ISO,
             time_zone: "Etc/UTC",
             zone_abbr: "UTC",
             utc_offset: 0,
             std_offset: 0,
             microsecond: {_value, 6}
           } = datetime
         ),
         do: DurableCodec.valid_datetime?(datetime)

    defp database_timestamp?(_other), do: false

    defp as_corruption(%Docket.Error{type: :corrupt_run_row} = error), do: error

    defp as_corruption(%Docket.Error{} = error) do
      corruption(
        "Postgres run state is corrupt: #{error.message}",
        %{cause_type: error.type},
        error
      )
    end

    defp corruption(message, details \\ %{}, reason \\ nil),
      do: Docket.Error.new(:corrupt_run_row, message, details: details, reason: reason)
  end
end
