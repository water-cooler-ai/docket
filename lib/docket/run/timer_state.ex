defmodule Docket.Run.TimerState do
  @moduledoc """
  Durable future wake owned by the run document.

  Timers are keyed on `Docket.Run.timers` by the identity they schedule —
  a retry timer is keyed by its task ID. `fires_at` is the earliest instant
  the scheduled work may execute; shells and backends derive the run's wake
  from the earliest timer.

  The only v0.1 kind is `:retry`: the parked next attempt of an active task.
  """

  defstruct [:kind, :fires_at]

  @type t :: %__MODULE__{
          kind: :retry,
          fires_at: DateTime.t()
        }
end
