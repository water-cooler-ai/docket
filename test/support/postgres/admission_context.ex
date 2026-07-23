defmodule Docket.Postgres.TestAdmissionContext do
  @moduledoc false

  def resolve(context, extra \\ %{}), do: resolve(context, extra, [])

  def resolve(context, extra, claim_policy_config)
      when is_map(extra) and is_list(claim_policy_config) do
    {repo, prefix} = apply(Docket.Postgres.Storage, :context!, [context])
    root = %{repo: repo, prefix: prefix}

    root
    |> Map.put(
      :claim_policy,
      apply(Docket.Postgres.ClaimPolicy, :new, [claim_policy_config, root])
    )
    |> Map.merge(extra)
  end
end
