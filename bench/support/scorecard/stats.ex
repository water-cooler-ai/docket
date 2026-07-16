defmodule Docket.Bench.Scorecard.Stats do
  @moduledoc "Nearest-rank percentiles, mean, clamp, and Jain's fairness index."

  def percentiles([]), do: %{p50: nil, p95: nil, p99: nil, min: nil, max: nil}

  def percentiles(values) do
    sorted = Enum.sort(values)

    %{
      p50: nearest_rank(sorted, 0.50),
      p95: nearest_rank(sorted, 0.95),
      p99: nearest_rank(sorted, 0.99),
      min: hd(sorted),
      max: List.last(sorted)
    }
  end

  def nearest_rank([], _percentile), do: nil

  def nearest_rank(sorted, percentile) do
    index = max(ceil(length(sorted) * percentile) - 1, 0)
    Enum.at(sorted, index)
  end

  def mean([]), do: nil

  def mean(values), do: Enum.sum(values) / length(values)

  def clamp(value, low, _high) when value < low, do: low
  def clamp(value, _low, high) when value > high, do: high
  def clamp(value, _low, _high), do: value

  def jain([]), do: nil

  def jain(values) do
    sum = Enum.sum(values)
    sum_squares = Enum.reduce(values, 0, fn value, acc -> acc + value * value end)

    if sum_squares == 0 do
      1.0
    else
      sum * sum / (length(values) * sum_squares)
    end
  end
end
