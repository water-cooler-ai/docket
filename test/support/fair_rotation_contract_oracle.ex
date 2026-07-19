defmodule Docket.Test.FairRotationContractOracle do
  @moduledoc false

  @doc false
  def bounds!(a, h, s, q, l)
      when is_integer(a) and is_integer(h) and is_integer(s) and is_integer(q) and
             is_integer(l) and a >= 1 and h >= a and s >= 1 and q >= 1 and l >= 0 do
    grants = (l + 1) * (a - 1)

    %{
      competing_grants: grants,
      competing_outcomes: q * grants,
      qualifying_calls: (l + 1) * (a - 1 + ceil_div(h - a + 1, s))
    }
  end

  def bounds!(a, h, s, q, l) do
    raise ArgumentError,
          "expected 1 <= A <= H, S >= 1, Q >= 1, and L >= 0; " <>
            "got A=#{inspect(a)}, H=#{inspect(h)}, S=#{inspect(s)}, " <>
            "Q=#{inspect(q)}, L=#{inspect(l)}"
  end

  defp ceil_div(dividend, divisor), do: div(dividend + divisor - 1, divisor)
end
