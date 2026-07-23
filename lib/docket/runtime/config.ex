defmodule Docket.Runtime.Config do
  @moduledoc false

  alias Docket.Runtime.Clock

  @instance_keys [
    :executor,
    :executor_opts,
    :clock,
    :sleeper,
    :id_generator,
    :max_attempt_elapsed_ms,
    :max_supersteps,
    :context,
    :checkpoint_observers
  ]
  @runtime_keys @instance_keys ++
                  [:backend, :backend_options, :backend_context, :tenant_mode, :testing]

  # Resolves loop/dispatcher options into one config map. All nondeterminism
  # enters here: the clock, ID generation, and the sleeper the inline shell
  # uses to serve committed retry-park waits are injectable so inline tests
  # stay deterministic.

  @type t :: %{
          executor: module(),
          executor_opts: keyword(),
          clock: (-> DateTime.t()),
          id_generator: (atom() -> String.t()),
          sleeper: (non_neg_integer() -> :ok),
          max_attempt_elapsed_ms: pos_integer(),
          max_supersteps: pos_integer() | nil,
          context: map()
        }

  @spec resolve(keyword()) :: t()
  def resolve(opts) when is_list(opts), do: build(opts)

  @doc false
  @spec resolve_moment(keyword()) :: t()
  def resolve_moment(opts) when is_list(opts), do: build(opts)

  @doc false
  def instance_keys, do: @instance_keys

  @doc false
  def validate_runtime!(opts) do
    case Keyword.keys(opts) -- @runtime_keys do
      [] -> :ok
      unknown -> raise ArgumentError, "unknown Docket runtime options: #{inspect(unknown)}"
    end

    validate_instance!(opts)
  end

  @doc false
  def validate_instance!(opts) do
    case Keyword.get(opts, :testing) do
      nil -> :ok
      mode when mode in [:manual, :inline] -> :ok
      other -> raise ArgumentError, ":testing must be :manual or :inline, got: #{inspect(other)}"
    end

    validate_callback!(opts, :clock, 0)
    validate_callback!(opts, :sleeper, 1)
    validate_callback!(opts, :id_generator, 1)

    case Keyword.get(opts, :context) do
      nil -> :ok
      context when is_map(context) -> :ok
      other -> raise ArgumentError, ":context must be a map, got: #{inspect(other)}"
    end

    case Keyword.get(opts, :executor) do
      nil ->
        :ok

      executor when is_atom(executor) ->
        unless Code.ensure_loaded?(executor) and function_exported?(executor, :execute, 6) do
          raise ArgumentError, ":executor must implement execute/6"
        end

      _ ->
        raise ArgumentError, ":executor must implement execute/6"
    end

    executor_opts = Keyword.get(opts, :executor_opts, [])

    unless Keyword.keyword?(executor_opts) do
      raise ArgumentError, ":executor_opts must be a keyword list"
    end

    if Keyword.has_key?(executor_opts, :task_supervisor) do
      raise ArgumentError, ":executor_opts :task_supervisor is owned by the runtime"
    end

    validate_positive!(opts, :max_attempt_elapsed_ms)
    validate_positive!(opts, :max_supersteps)
    validate_observers!(Keyword.get(opts, :checkpoint_observers, []))
    :ok
  end

  defp build(opts) do
    max_attempt_elapsed_ms = Keyword.get(opts, :max_attempt_elapsed_ms, 2_000)

    unless is_integer(max_attempt_elapsed_ms) and max_attempt_elapsed_ms > 0 do
      raise ArgumentError, ":max_attempt_elapsed_ms must be a positive finite integer"
    end

    %{
      executor: Keyword.get(opts, :executor, Docket.Executor.Local),
      executor_opts: Keyword.get(opts, :executor_opts, []),
      clock: Clock.wall_clock(opts),
      id_generator: Keyword.get(opts, :id_generator, &default_id/1),
      sleeper: Keyword.get(opts, :sleeper, &sleep/1),
      max_attempt_elapsed_ms: max_attempt_elapsed_ms,
      max_supersteps: Keyword.get(opts, :max_supersteps),
      context: Keyword.get(opts, :context, %{})
    }
  end

  defp validate_callback!(opts, key, arity) do
    case Keyword.get(opts, key) do
      nil -> :ok
      callback when is_function(callback, arity) -> :ok
      _ -> raise ArgumentError, ":#{key} must be a function of arity #{arity}"
    end
  end

  defp validate_positive!(opts, key) do
    case Keyword.get(opts, key) do
      nil -> :ok
      value when is_integer(value) and value > 0 -> :ok
      _ -> raise ArgumentError, ":#{key} must be a positive integer"
    end
  end

  defp validate_observers!(observers) do
    Enum.each(List.wrap(observers), fn observer ->
      unless is_atom(observer) and Code.ensure_loaded?(observer) and
               function_exported?(observer, :observe, 2) do
        raise ArgumentError,
              ":checkpoint_observers must implement observe/2, got: #{inspect(observer)}"
      end
    end)
  end

  defp default_id(kind) do
    "#{kind}_" <> Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)
  end

  defp sleep(0), do: :ok
  defp sleep(ms), do: Process.sleep(ms)
end
