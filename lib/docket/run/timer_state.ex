defmodule Docket.Run.TimerState do
  @moduledoc """
  Durable future wake owned by the run document.

  Timers are keyed on `Docket.Run.timers` by the identity they schedule —
  a retry timer is keyed by its task ID. `fires_at` is the earliest instant
  the scheduled work may execute; shells and backends derive the run's wake
  from the earliest timer.

  Two kinds exist: `:retry` schedules the parked next attempt of an active
  task, and `:detached_deadline` bounds a detached attempt — `fires_at` is
  the instant the runtime recovers the task if no completion arrived.
  """

  defstruct [:kind, :fires_at]

  @type t :: %__MODULE__{
          kind: :retry | :detached_deadline,
          fires_at: DateTime.t()
        }
end
