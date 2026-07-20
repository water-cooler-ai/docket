defmodule Docket.Executor do
  @moduledoc """
  Adapter boundary for executing one runtime graph node activation.

  Executors run node code; they must not mutate the run, apply writes, emit
  checkpoints, decide graph termination, or read uncommitted superstep
  writes. The dispatcher normalizes raises, exits, and throws, so executors
  may let node exceptions propagate.

  v0.1 ships `Docket.Executor.Local`, which executes inside the dispatcher's
  isolated, finite-deadline activation process. Custom executors receive the
  same hard outer boundary. Queue,
  remote, and late-completion protocols are post-v0.1; the `{:await, term()}`
  return is reserved for them and is treated as a permanent node failure in
  v0.1.

  The runtime dispatches all activations in a superstep concurrently. The
  executor callback remains a single-activation boundary; the update barrier
  waits for every callback and applies their results in deterministic
  activation order.
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
