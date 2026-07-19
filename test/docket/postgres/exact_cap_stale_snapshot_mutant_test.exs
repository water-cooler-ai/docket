if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.ExactCapStaleSnapshotMutantTest do
    use ExUnit.Case, async: false

    @moduletag :postgres
    @barrier_key 7_900_791

    alias Docket.Postgres.TestRepo

    defmodule MutationRepo do
      use Ecto.Repo, otp_app: :docket, adapter: Ecto.Adapters.Postgres
    end

    setup do
      config = TestRepo.config()
      _ = Ecto.Adapters.Postgres.storage_down(config)
      :ok = Ecto.Adapters.Postgres.storage_up(config)

      Application.put_env(:docket, MutationRepo, Keyword.put(config, :pool_size, 5))
      start_supervised!(MutationRepo)

      MutationRepo.query!("""
      CREATE TABLE adversarial_cap_partition (
        scope_key text PRIMARY KEY,
        max_active integer NOT NULL CHECK (max_active > 0)
      )
      """)

      MutationRepo.query!("""
      CREATE TABLE adversarial_cap_run (
        id integer PRIMARY KEY,
        scope_key text NOT NULL REFERENCES adversarial_cap_partition(scope_key),
        claimed boolean NOT NULL DEFAULT false
      )
      """)

      MutationRepo.query!(
        "INSERT INTO adversarial_cap_partition (scope_key, max_active) VALUES ('tenant', 1)"
      )

      MutationRepo.query!("""
      INSERT INTO adversarial_cap_run (id, scope_key, claimed)
      VALUES (1, 'tenant', false), (2, 'tenant', false)
      """)

      :ok
    end

    test "known-bad pre-lock snapshot over-admits while a fresh post-lock count denies" do
      parent = self()
      gate = make_ref()

      barrier =
        Task.async(fn ->
          MutationRepo.transaction(fn ->
            MutationRepo.query!("SELECT pg_advisory_xact_lock($1)", [@barrier_key])
            send(parent, {gate, :barrier_locked})

            receive do
              {^gate, :release_barrier} -> :ok
            after
              5_000 -> raise "timed out holding stale-snapshot barrier"
            end
          end)
        end)

      assert_receive {^gate, :barrier_locked}, 2_000

      mutant =
        Task.async(fn ->
          MutationRepo.checkout(fn ->
            [[backend_pid]] = MutationRepo.query!("SELECT pg_backend_pid()").rows
            send(parent, {gate, :mutant_backend, backend_pid})

            MutationRepo.query!(known_bad_statement(), [@barrier_key]).rows
          end)
        end)

      assert_receive {^gate, :mutant_backend, mutant_backend}, 2_000
      assert wait_until_waiting(mutant_backend, ["advisory"])

      MutationRepo.transaction(fn ->
        MutationRepo.query!(
          "SELECT scope_key FROM adversarial_cap_partition WHERE scope_key = 'tenant' FOR UPDATE"
        )

        MutationRepo.query!("UPDATE adversarial_cap_run SET claimed = true WHERE id = 1")
      end)

      send(barrier.pid, {gate, :release_barrier})
      assert {:ok, :ok} = Task.await(barrier, 2_000)
      assert [[2]] = Task.await(mutant, 5_000)

      assert claimed_count() == 2
      assert effective_cap() == 1
      assert claimed_count() > effective_cap()

      MutationRepo.query!("UPDATE adversarial_cap_run SET claimed = false")

      authority_holder =
        Task.async(fn ->
          MutationRepo.transaction(fn ->
            MutationRepo.query!(
              "SELECT scope_key FROM adversarial_cap_partition WHERE scope_key = 'tenant' FOR UPDATE"
            )

            send(parent, {gate, :authority_locked})

            receive do
              {^gate, :commit_first_claim} ->
                MutationRepo.query!("UPDATE adversarial_cap_run SET claimed = true WHERE id = 1")
            after
              5_000 -> raise "timed out holding exact-cap authority"
            end
          end)
        end)

      assert_receive {^gate, :authority_locked}, 2_000

      fresh_recheck =
        Task.async(fn ->
          MutationRepo.transaction(fn ->
            [[backend_pid]] = MutationRepo.query!("SELECT pg_backend_pid()").rows
            send(parent, {gate, :fresh_backend, backend_pid})

            MutationRepo.query!(
              "SELECT scope_key FROM adversarial_cap_partition WHERE scope_key = 'tenant' FOR UPDATE"
            )

            [[live_count]] =
              MutationRepo.query!(
                "SELECT count(*) FROM adversarial_cap_run WHERE scope_key = 'tenant' AND claimed"
              ).rows

            [[cap]] =
              MutationRepo.query!(
                "SELECT max_active FROM adversarial_cap_partition WHERE scope_key = 'tenant'"
              ).rows

            if live_count < cap do
              MutationRepo.query!("UPDATE adversarial_cap_run SET claimed = true WHERE id = 2")
              :admitted
            else
              :denied
            end
          end)
        end)

      assert_receive {^gate, :fresh_backend, fresh_backend}, 2_000

      assert wait_until_waiting(fresh_backend, ["transactionid", "tuple"])

      send(authority_holder.pid, {gate, :commit_first_claim})
      assert {:ok, _} = Task.await(authority_holder, 2_000)
      assert {:ok, :denied} = Task.await(fresh_recheck, 5_000)
      assert claimed_count() == effective_cap()
    end

    defp known_bad_statement do
      """
      WITH stale_count AS MATERIALIZED (
        SELECT count(*)::integer AS live_count
        FROM adversarial_cap_run
        WHERE scope_key = 'tenant' AND claimed
      ),
      barrier AS MATERIALIZED (
        SELECT pg_advisory_xact_lock($1) AS acquired
        FROM stale_count
      ),
      authority AS MATERIALIZED (
        SELECT partition.max_active, stale_count.live_count
        FROM adversarial_cap_partition AS partition
        CROSS JOIN stale_count
        CROSS JOIN barrier
        WHERE partition.scope_key = 'tenant'
        FOR UPDATE OF partition
      )
      UPDATE adversarial_cap_run AS run
      SET claimed = true
      FROM authority
      WHERE run.id = 2
        AND authority.live_count < authority.max_active
      RETURNING run.id
      """
    end

    defp wait_until_waiting(backend_pid, wait_events, attempts \\ 200)

    defp wait_until_waiting(backend_pid, wait_events, 0) do
      state =
        MutationRepo.query!(
          "SELECT state, wait_event_type, wait_event, query FROM pg_stat_activity WHERE pid = $1",
          [backend_pid]
        ).rows

      raise "backend did not wait for #{inspect(wait_events)}: #{inspect(state)}"
    end

    defp wait_until_waiting(backend_pid, wait_events, attempts) do
      observed =
        MutationRepo.query!("SELECT wait_event FROM pg_stat_activity WHERE pid = $1", [
          backend_pid
        ]).rows

      waiting? = Enum.any?(wait_events, &([&1] in observed))

      if waiting? do
        true
      else
        Process.sleep(10)
        wait_until_waiting(backend_pid, wait_events, attempts - 1)
      end
    end

    defp claimed_count do
      [[count]] =
        MutationRepo.query!("SELECT count(*) FROM adversarial_cap_run WHERE claimed").rows

      count
    end

    defp effective_cap do
      [[cap]] = MutationRepo.query!("SELECT max_active FROM adversarial_cap_partition").rows
      cap
    end
  end
end
