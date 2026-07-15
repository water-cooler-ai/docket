if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.AdmissionPhaseTest do
    use ExUnit.Case, async: true

    alias Docket.Postgres.AdmissionPhase

    setup do
      %{phase: start_supervised!(AdmissionPhase)}
    end

    test "successful demand-one admissions alternate and errors do not", %{phase: phase} do
      assert {:ok, :ready} = AdmissionPhase.run(phase, 1, &{:ok, &1})

      assert {:error, :down} =
               AdmissionPhase.run(phase, 1, fn preference ->
                 assert preference == :expired
                 {:error, :down}
               end)

      assert {:ok, :expired} = AdmissionPhase.run(phase, 1, &{:ok, &1})
      assert {:ok, :ready} = AdmissionPhase.run(phase, 1, &{:ok, &1})
    end

    test "demand above one carries but does not advance the phase", %{phase: phase} do
      assert {:ok, :ready} = AdmissionPhase.run(phase, 2, &{:ok, &1})
      assert {:ok, :ready} = AdmissionPhase.run(phase, 1, &{:ok, &1})
    end

    test "claim scans are serialized in their calling processes", %{phase: phase} do
      parent = self()

      first =
        Task.async(fn ->
          AdmissionPhase.run(phase, 1, fn preference ->
            send(parent, {:entered, :first, preference, self()})
            receive do: (:release -> {:ok, :first})
          end)
        end)

      assert_receive {:entered, :first, :ready, first_owner}

      second =
        Task.async(fn ->
          AdmissionPhase.run(phase, 1, fn preference ->
            send(parent, {:entered, :second, preference})
            {:ok, :second}
          end)
        end)

      refute_receive {:entered, :second, _}, 50
      send(first_owner, :release)
      assert {:ok, :first} = Task.await(first)
      assert_receive {:entered, :second, :expired}
      assert {:ok, :second} = Task.await(second)
    end

    test "a dead owner releases the phase without advancing it", %{phase: phase} do
      parent = self()

      owner =
        spawn(fn ->
          AdmissionPhase.run(phase, 1, fn preference ->
            send(parent, {:owner_entered, preference})
            Process.sleep(:infinity)
          end)
        end)

      assert_receive {:owner_entered, :ready}
      Process.exit(owner, :kill)

      assert {:ok, :ready} = AdmissionPhase.run(phase, 1, &{:ok, &1})
    end
  end
end
