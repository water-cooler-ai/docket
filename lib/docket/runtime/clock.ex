defmodule Docket.Runtime.Clock do
  @moduledoc false

  @spec wall_clock(keyword()) :: (-> DateTime.t())
  def wall_clock(opts) when is_list(opts) do
    case Keyword.get(opts, :clock, &DateTime.utc_now/0) do
      clock when is_function(clock, 0) -> fn -> now!(clock) end
      other -> raise ArgumentError, ":clock must be a zero-arity function, got: #{inspect(other)}"
    end
  end

  @spec now!((-> term())) :: DateTime.t()
  def now!(clock) when is_function(clock, 0) do
    case clock.() do
      %DateTime{} = now -> now
      other -> raise ArgumentError, ":clock must return a DateTime, got: #{inspect(other)}"
    end
  end

  @spec normalize!(term()) :: DateTime.t()
  def normalize!(value) do
    case value do
      %DateTime{} = now ->
        now
        |> DateTime.to_unix(:microsecond)
        |> DateTime.from_unix!(:microsecond)

      other -> raise ArgumentError, "expected a DateTime to normalize, got: #{inspect(other)}"
    end
  end
end
