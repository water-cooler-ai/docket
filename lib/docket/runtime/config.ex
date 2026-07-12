defmodule Docket.Runtime.Config do
  @moduledoc false

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
          max_supersteps: pos_integer() | nil,
          context: map()
        }

  @spec resolve(keyword()) :: t()
  def resolve(opts) when is_list(opts), do: build(opts)

  @doc false
  @spec resolve_moment(keyword()) :: t()
  def resolve_moment(opts) when is_list(opts), do: build(opts)

  defp build(opts) do
    %{
      executor: Keyword.get(opts, :executor, Docket.Executor.Local),
      executor_opts: Keyword.get(opts, :executor_opts, []),
      clock: Keyword.get(opts, :clock, &DateTime.utc_now/0),
      id_generator: Keyword.get(opts, :id_generator, &default_id/1),
      sleeper: Keyword.get(opts, :sleeper, &sleep/1),
      max_supersteps: Keyword.get(opts, :max_supersteps),
      context: Keyword.get(opts, :context, %{})
    }
  end

  defp default_id(kind) do
    "#{kind}_" <> Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)
  end

  defp sleep(0), do: :ok
  defp sleep(ms), do: Process.sleep(ms)
end
