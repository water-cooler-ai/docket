defmodule Docket.Runtime.ExecutionPolicy do
  @moduledoc false

  alias Docket.Graph.Compiler.Policies

  @spec validate_graph(Docket.Runtime.Graph.t(), pos_integer()) ::
          :ok | {:error, Docket.Error.t()}
  def validate_graph(rtg, maximum) when is_integer(maximum) and maximum > 0 do
    rtg.nodes
    |> Map.values()
    |> Enum.sort_by(& &1.public_id)
    |> Enum.reduce_while(:ok, fn node, :ok ->
      case Policies.node_policies(node.policies) do
        {:ok, %{timeout_ms: timeout}} when is_integer(timeout) and timeout > maximum ->
          {:halt,
           {:error,
            Docket.Error.new(
              :incompatible_execution_policy,
              "node #{inspect(node.public_id)} timeout exceeds this runtime's attempt maximum",
              node_id: node.public_id,
              phase: :execute,
              details: %{timeout_ms: timeout, max_attempt_elapsed_ms: maximum}
            )}}

        {:ok, _policies} ->
          {:cont, :ok}

        {:error, _errors} ->
          # General policy validation remains owned by the planner/compiler;
          # this check adds only the host-specific timeout ceiling.
          {:cont, :ok}
      end
    end)
  end

  # Entry points reject oversized explicit timeouts via validate_graph/2
  # before execution; clamping keeps the runtime deadline a hard bound even
  # for callers that reach dispatch without that validation.
  @spec effective_timeout(pos_integer() | nil, pos_integer()) :: pos_integer()
  def effective_timeout(nil, maximum), do: maximum
  def effective_timeout(timeout, maximum), do: min(timeout, maximum)
end
