defmodule Docket.Benchmark.ProgressTest do
  use ExUnit.Case, async: true

  alias Docket.Benchmark.Progress

  defp point(overrides \\ %{}) do
    Map.merge(
      %{scenario: "smoke", concurrency: 2, pool_size: 5, repetition: 1, repetitions: 1},
      overrides
    )
  end

  defp run_trials(progress) do
    Progress.point_started(progress, 1, point())
    Progress.point_finished(progress, 1, true)
    Progress.point_started(progress, 2, point(%{repetition: 2, repetitions: 3}))
    Progress.point_finished(progress, 2, false)
    Progress.stop(progress)
  end

  test "plain mode prints start and completion lines with trial counts" do
    {:ok, device} = StringIO.open("")

    2
    |> Progress.start(mode: :plain, device: device)
    |> run_trials()

    {_input, output} = StringIO.contents(device)

    assert output =~ "trial 1/2 · smoke c=2 pool=5 · started"
    assert output =~ "trial 1/2 · smoke c=2 pool=5 · PASS · "
    assert output =~ "trial 2/2 · smoke c=2 pool=5 rep=2/3 · started"
    assert output =~ "trial 2/2 · smoke c=2 pool=5 rep=2/3 · FAIL · "
    refute output =~ "\r"
  end

  test "animated mode renders a spinner line and persists completion lines" do
    {:ok, device} = StringIO.open("")
    progress = Progress.start(2, mode: :animated, device: device)

    Progress.point_started(progress, 1, point())
    Process.sleep(250)
    Progress.point_finished(progress, 1, true)
    Progress.point_started(progress, 2, point(%{repetition: 2, repetitions: 3}))
    Progress.point_finished(progress, 2, false)
    Progress.stop(progress)

    {_input, output} = StringIO.contents(device)

    assert output =~ "\r\e[K"
    assert output =~ "trial 1/2 · running smoke c=2 pool=5"
    assert output =~ "trial 1/2 · smoke c=2 pool=5 · PASS · "
    assert output =~ "trial 2/2 · running smoke c=2 pool=5 rep=2/3"
    assert output =~ "trial 2/2 · smoke c=2 pool=5 rep=2/3 · FAIL · "
    assert String.ends_with?(output, "\r\e[K")
  end

  test "off mode and empty plans accept every call without output" do
    assert Progress.start(3, mode: :off) == :off
    assert Progress.start(0) == :off
    assert Progress.point_started(:off, 1, point()) == :ok
    assert Progress.point_finished(:off, 1, true) == :ok
    assert Progress.stop(:off) == :ok
  end
end
