if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.RunCodec do
    @moduledoc """
    Pure mapping between a `Docket.Run` and its Postgres row projection.

    Stable, host-inspectable run fields are stored once in typed columns. The
    remaining canonical wire document -- including its wire `version`,
    `event_seq`, and all Docket-owned execution internals -- is stored in the
    opaque `state` column. Canonical run validation and reconstruction remain
    owned by `Docket.Run.to_map/1` and `Docket.Run.from_map/1`.

    `load/1` fails closed with `Docket.Error.type == :corrupt_run_row` when a
    row cannot reconstruct the current canonical run document. In particular,
    a promoted key in `state` is corruption even when its value agrees with
    the corresponding column: accepting two copies would introduce an
    ambiguous source of truth. Version-1 documents and the private `:created`
    status are not migrated or accepted. Promoted run timestamps must already
    be UTC with six-digit microsecond precision so the database cannot change
    the committed struct merely by padding or normalizing a timestamp.
    """

    alias Docket.Run

    @promoted_wire_keys ~w(
      id
      graph_id
      graph_hash
      status
      step
      input
      output
      failure
      metadata
      checkpoint_seq
      started_at
      updated_at
      finished_at
    )

    @type row_attrs :: %{
            required(:run_id) => String.t(),
            required(:graph_id) => String.t(),
            required(:graph_hash) => String.t() | nil,
            required(:status) => Run.durable_status(),
            required(:step) => non_neg_integer(),
            required(:input) => map(),
            required(:output) => map() | nil,
            required(:failure) => map() | nil,
            required(:metadata) => map(),
            required(:state) => map(),
            required(:checkpoint_seq) => non_neg_integer(),
            required(:started_at) => DateTime.t() | nil,
            required(:updated_at) => DateTime.t() | nil,
            required(:finished_at) => DateTime.t() | nil
          }

    @doc """
    Validates and splits a run into atom-keyed Ecto row attributes.

    Invalid in-memory runs return the same typed `Docket.Error` raised by the
    canonical `Docket.Run.to_map/1` boundary.
    """
    @spec dump(Run.t()) :: {:ok, row_attrs()} | {:error, Docket.Error.t()}
    def dump(%Run{} = run) do
      validate_promoted_timestamps!(run)
      document = Run.to_map(run)
      canonical_run = Run.from_map!(document)

      if canonical_run !== run do
        raise Docket.Error,
          type: :invalid_run,
          message:
            "run is not in canonical durable form and would change across a storage " <>
              "round trip"
      end

      attrs = %{
        run_id: Map.fetch!(document, "id"),
        graph_id: Map.fetch!(document, "graph_id"),
        graph_hash: Map.get(document, "graph_hash"),
        status: run.status,
        step: Map.fetch!(document, "step"),
        input: Map.get(document, "input", %{}),
        output: Map.get(document, "output"),
        failure: Map.get(document, "failure"),
        metadata: Map.get(document, "metadata", %{}),
        state: Map.drop(document, @promoted_wire_keys),
        checkpoint_seq: Map.fetch!(document, "checkpoint_seq"),
        started_at: run.started_at,
        updated_at: run.updated_at,
        finished_at: run.finished_at
      }

      {:ok, attrs}
    rescue
      error in Docket.Error -> {:error, error}
    end

    @doc """
    Reconstructs and validates a run from an Ecto schema struct or row map.

    All malformed persisted values are reported as `:corrupt_run_row`, with
    the canonical codec error retained in `error.reason` when reconstruction
    reached the canonical document boundary.
    """
    @spec load(Docket.Postgres.Schemas.Run.t() | map()) ::
            {:ok, Run.t()} | {:error, Docket.Error.t()}
    def load(row) when is_map(row) do
      {:ok, load_row!(row)}
    rescue
      error in Docket.Error -> {:error, as_corruption(error)}
    end

    def load(other) do
      {:error,
       corruption("Postgres run row must be a schema struct or map, got: #{inspect(other)}")}
    end

    @doc """
    Same as `load/1`, but raises `Docket.Error` for a corrupt persisted row.

    Store reads use this form so corruption is never collapsed into a missing
    row or another ordinary storage result.
    """
    @spec load!(Docket.Postgres.Schemas.Run.t() | map()) :: Run.t()
    def load!(row) do
      case load(row) do
        {:ok, %Run{} = run} -> run
        {:error, %Docket.Error{} = error} -> raise error
      end
    end

    defp load_row!(row) do
      state = fetch_row_field!(row, :state)

      unless is_map(state) and not is_struct(state) do
        raise corruption("Postgres run state must be a plain map, got: #{inspect(state)}")
      end

      reject_promoted_state_keys!(state)

      promoted = %{
        "id" => fetch_row_field!(row, :run_id),
        "graph_id" => fetch_row_field!(row, :graph_id),
        "graph_hash" => fetch_row_field!(row, :graph_hash),
        "status" => status_to_wire(fetch_row_field!(row, :status)),
        "step" => fetch_row_field!(row, :step),
        "input" => fetch_row_field!(row, :input),
        "output" => fetch_row_field!(row, :output),
        "failure" => fetch_row_field!(row, :failure),
        "metadata" => fetch_row_field!(row, :metadata),
        "checkpoint_seq" => fetch_row_field!(row, :checkpoint_seq),
        "started_at" => timestamp_to_wire(fetch_row_field!(row, :started_at)),
        "updated_at" => timestamp_to_wire(fetch_row_field!(row, :updated_at)),
        "finished_at" => timestamp_to_wire(fetch_row_field!(row, :finished_at))
      }

      document = Map.merge(state, promoted)

      case Run.from_map(document) do
        {:ok, %Run{} = run} ->
          run

        {:error, %Docket.Error{} = error} ->
          raise as_corruption(error)
      end
    end

    defp fetch_row_field!(row, field) do
      case Map.fetch(row, field) do
        {:ok, value} ->
          value

        :error ->
          raise corruption("Postgres run row is missing field #{inspect(field)}", %{
                  field: field
                })
      end
    end

    defp reject_promoted_state_keys!(state) do
      collisions =
        state
        |> Map.keys()
        |> Enum.filter(&(&1 in @promoted_wire_keys))
        |> Enum.sort()

      if collisions != [] do
        raise corruption(
                "Postgres run state contains promoted column key(s): " <>
                  Enum.map_join(collisions, ", ", &inspect/1),
                %{keys: collisions}
              )
      end
    end

    defp status_to_wire(status) when is_atom(status), do: Atom.to_string(status)
    defp status_to_wire(status), do: status

    defp timestamp_to_wire(nil), do: nil
    defp timestamp_to_wire(%DateTime{} = timestamp), do: DateTime.to_iso8601(timestamp)
    defp timestamp_to_wire(other), do: other

    defp validate_promoted_timestamps!(%Run{} = run) do
      for {field, timestamp} <- [
            started_at: run.started_at,
            updated_at: run.updated_at,
            finished_at: run.finished_at
          ] do
        unless canonical_database_timestamp?(timestamp) do
          raise Docket.Error,
            type: :invalid_run,
            message:
              "run #{field} must be nil or a UTC DateTime with six-digit microsecond " <>
                "precision, got: #{inspect(timestamp)}"
        end
      end

      :ok
    end

    defp canonical_database_timestamp?(nil), do: true

    defp canonical_database_timestamp?(%DateTime{
           time_zone: "Etc/UTC",
           zone_abbr: "UTC",
           utc_offset: 0,
           std_offset: 0,
           microsecond: {_microsecond, 6}
         }),
         do: true

    defp canonical_database_timestamp?(_timestamp), do: false

    defp as_corruption(%Docket.Error{type: :corrupt_run_row} = error), do: error

    defp as_corruption(%Docket.Error{} = error) do
      corruption(
        "Postgres run row cannot reconstruct a canonical run: #{error.message}",
        %{
          cause_type: error.type,
          cause_details: error.details
        },
        error
      )
    end

    defp corruption(message, details \\ %{}, reason \\ nil) do
      Docket.Error.new(:corrupt_run_row, message, details: details, reason: reason)
    end
  end
end
