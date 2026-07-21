defmodule Docket.ClaimPolicy do
  @moduledoc """
  Backend-neutral current value of a max-active-run policy.

  A `nil` `max_active_runs` with version zero represents an uninitialized
  persisted default. Resetting an owner override returns a `nil` cap with its
  incremented partition version.
  """

  @enforce_keys [:max_active_runs, :version, :updated_at]
  defstruct [:max_active_runs, :version, :updated_at]

  @type cap :: 1..2_147_483_647
  @type t :: %__MODULE__{
          max_active_runs: cap() | nil,
          version: non_neg_integer(),
          updated_at: DateTime.t()
        }

  @doc false
  def new(%{max_active_runs: maximum, version: version, updated_at: %DateTime{} = updated_at})
      when (is_nil(maximum) or
              (is_integer(maximum) and maximum > 0 and maximum <= 2_147_483_647)) and
             is_integer(version) and version >= 0 do
    {:ok,
     %__MODULE__{
       max_active_runs: maximum,
       version: version,
       updated_at: updated_at
     }}
  end

  def new(_policy), do: :error
end
