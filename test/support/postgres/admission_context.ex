defmodule Docket.Postgres.TestAdmissionContext do
  @moduledoc false

  def resolve(context, extra \\ %{}) do
    {repo, prefix} = apply(Docket.Postgres.Storage, :context!, [context])
    root = %{repo: repo, prefix: prefix}

    root
    |> Map.put(:claim_policy, apply(Docket.Postgres.ClaimPolicy, :new, [[], root]))
    |> Map.merge(extra)
  end
end
