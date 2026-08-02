defmodule Docket.Run.TimerState do
  @moduledoc """
  Durable future wake owned by the run document.

  Timers are keyed on `Docket.Run.timers` by the identity they schedule —
  a task timer is keyed by its task ID. `fires_at` is the earliest instant
  the scheduled work may execute; shells and backends derive the run's wake
  from the earliest timer.

  Two kinds exist:

  - `:retry` — the parked next attempt of an active task becomes
    dispatchable when the timer fires.
  - `:schedule_to_start` — a pending detached task's bounded queue-wait
    deadline. The timer never makes the task dispatchable; it exists so the
    run wakes when the deadline passes. An unbounded pending detached task
    has no timer.
  """

  defstruct [:kind, :fires_at]

  @type t :: %__MODULE__{
          kind: :retry | :schedule_to_start,
          fires_at: DateTime.t()
        }
end
