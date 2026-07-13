defmodule Docket.GraphVersion do
  @moduledoc """
  Lightweight metadata for one retained saved graph version.

  The reference is an exact content address within the owner scope used for
  the read. Owner scope is intentionally absent from this value: public graph
  APIs resolve it independently, and a reference is not an authorization
  credential. `published_at` records the first successful publication of this
  distinct version; an idempotent save does not change it.

  Storage backends construct this projection after validating their own rows.
  """

  @enforce_keys [:ref, :published_at]
  defstruct [:ref, :published_at]

  @type t :: %__MODULE__{
          ref: Docket.GraphRef.t(),
          published_at: DateTime.t()
        }
end
