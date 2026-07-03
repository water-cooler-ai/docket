defmodule Docket.Executor do
  @moduledoc """
  Adapter boundary for executing one runtime graph node activation.

  Executors run node code; they must not mutate the run, apply writes, emit
  checkpoints, decide graph termination, or read uncommitted superstep
  writes. The dispatcher normalizes raises, exits, and throws, so executors
  may let node exceptions propagate.

  v1 ships `Docket.Executor.Local`. Queue, remote, and late-completion
  protocols are post-v1.
  """

  @callback execute(
              task :: Docket.Run.TaskState.t(),
              node :: Docket.Runtime.Graph.Node.t(),
              state :: map(),
              config :: map(),
              context :: map(),
              opts :: keyword()
            ) ::
              {:ok, state_update :: map()}
              | {:interrupt, Docket.Interrupt.t()}
              | {:await, term()}
              | {:error, term()}
end
