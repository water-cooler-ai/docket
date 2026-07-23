defmodule Docket.Runtime.ClockTest do
  use ExUnit.Case, async: true

  alias Docket.Runtime.Clock

  test "wraps a valid wall clock" do
    now = ~U[2030-01-02 03:04:05.000000Z]
    clock = Clock.wall_clock(clock: fn -> now end)

    assert clock.() == now
  end

  test "normalizes database-bound timestamps to UTC microsecond precision" do
    non_utc_high_precision = %{
      ~U[2030-01-02 03:04:05.123456Z]
      | hour: 5,
        time_zone: "Etc/GMT-2",
        zone_abbr: "+02",
        utc_offset: 7_200,
        microsecond: {123_456, 9}
    }

    assert Clock.normalize!(non_utc_high_precision) == ~U[2030-01-02 03:04:05.123456Z]
  end

  test "rejects malformed callbacks and return values" do
    assert_raise ArgumentError, ~r/:clock must be a zero-arity function/, fn ->
      Clock.wall_clock(clock: :system)
    end

    clock = Clock.wall_clock(clock: fn -> :not_a_datetime end)

    assert_raise ArgumentError, ~r/:clock must return a DateTime/, fn ->
      clock.()
    end
  end
end
