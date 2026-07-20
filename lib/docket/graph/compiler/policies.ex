defmodule Docket.Graph.Compiler.Policies do
  @moduledoc false

  # Graph policy resolution shared by validation and lowering so both passes
  # agree on the effective values.

  alias Docket.Graph

  @max_supersteps_key "max_supersteps"

  @doc """
  Resolves the effective max-supersteps limit.

  A valid graph policy wins over the `opts` runtime default; an explicit nil
  policy counts as unset and falls back to the default. A present policy that
  is not a positive integer is reported as `{:invalid, value}` so validation
  can attach a diagnostic regardless of graph topology.
  """
  @spec max_supersteps(Graph.t(), keyword()) ::
          {:ok, pos_integer() | nil} | {:invalid, term()}
  def max_supersteps(%Graph{} = graph, opts) do
    case Map.get(graph.policies, @max_supersteps_key) do
      nil -> {:ok, Keyword.get(opts, :max_supersteps)}
      limit when is_integer(limit) and limit > 0 -> {:ok, limit}
      invalid -> {:invalid, invalid}
    end
  end

  @spec max_supersteps_key() :: String.t()
  def max_supersteps_key, do: @max_supersteps_key

  @typedoc "Resolved v0.1 node execution policies with defaults applied."
  @type node_policies :: %{
          timeout_ms: pos_integer() | nil,
          retry: %{max_attempts: pos_integer(), backoff_ms: non_neg_integer()}
        }

  @doc """
  Resolves the v0.1 node policy surface: `"timeout_ms"` and
  `"retry" => %{"max_attempts", "backoff_ms"}`. `"on_error"` is reserved for
  post-v0.1 routing and rejected so graphs cannot silently depend on it;
  other unknown keys are ignored as open content.

  Returns every problem, keyed by the offending policy key, so compiler
  validation can attach one diagnostic per key while the runtime joins them
  into a single plan-time error. Both sides share these rules by
  construction.
  """
  @spec node_policies(term()) ::
          {:ok, node_policies()} | {:error, [{String.t(), String.t()}]}
  def node_policies(policies) when is_map(policies) and not is_struct(policies) do
    {timeout_result, timeout_errors} = check_timeout(policies)
    {retry_result, retry_errors} = check_retry(policies)

    case reserved_errors(policies) ++ timeout_errors ++ retry_errors do
      [] -> {:ok, %{timeout_ms: timeout_result, retry: retry_result}}
      errors -> {:error, errors}
    end
  end

  def node_policies(other) do
    {:error, [{nil, "node policies must be a map, got #{inspect(other)}"}]}
  end

  defp reserved_errors(policies) do
    case Map.fetch(policies, "on_error") do
      :error ->
        []

      {:ok, _value} ->
        [{"on_error", "node policy \"on_error\" is reserved and not supported in v0.1"}]
    end
  end

  defp check_timeout(policies) do
    case Map.get(policies, "timeout_ms") do
      nil ->
        {nil, []}

      value when is_integer(value) and value > 0 ->
        {value, []}

      other ->
        {nil,
         [
           {"timeout_ms",
            "node policy \"timeout_ms\" must be a positive integer, got #{inspect(other)}"}
         ]}
    end
  end

  @retry_defaults %{max_attempts: 1, backoff_ms: 0}

  defp check_retry(policies) do
    case Map.get(policies, "retry") do
      nil ->
        {@retry_defaults, []}

      %{} = retry ->
        results = [
          retry_field(retry, "max_attempts", @retry_defaults.max_attempts, &(&1 >= 1)),
          retry_field(retry, "backoff_ms", @retry_defaults.backoff_ms, &(&1 >= 0)),
          retry_known_keys(retry)
        ]

        case Enum.flat_map(results, fn {_value, errors} -> errors end) do
          [] ->
            [{max_attempts, []}, {backoff_ms, []}, _keys] = results
            {%{max_attempts: max_attempts, backoff_ms: backoff_ms}, []}

          errors ->
            {@retry_defaults, errors}
        end

      other ->
        {@retry_defaults,
         [{"retry", "node policy \"retry\" must be a map, got #{inspect(other)}"}]}
    end
  end

  defp retry_field(retry, key, default, valid?) do
    case Map.get(retry, key) do
      nil ->
        {default, []}

      value when is_integer(value) ->
        if valid?.(value) do
          {value, []}
        else
          {default,
           [{"retry", "node retry policy #{inspect(key)} is out of range, got #{value}"}]}
        end

      other ->
        {default,
         [
           {"retry",
            "node retry policy #{inspect(key)} must be an integer, got #{inspect(other)}"}
         ]}
    end
  end

  defp retry_known_keys(retry) do
    case Map.keys(retry) -- ["max_attempts", "backoff_ms"] do
      [] ->
        {nil, []}

      extra ->
        {nil, [{"retry", "node retry policy has unknown keys #{inspect(Enum.sort(extra))}"}]}
    end
  end
end
