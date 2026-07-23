defmodule Docket.Runtime.Instance do
  @moduledoc false

  use GenServer

  def start_link({name, defaults}) do
    GenServer.start_link(__MODULE__, defaults, name: name(name))
  end

  def defaults(runtime) do
    GenServer.call(name(runtime), :defaults)
  catch
    :exit, _reason -> :error
  end

  defp name(runtime), do: Module.concat(runtime, Instance)

  @impl true
  def init(defaults), do: {:ok, defaults}

  @impl true
  def handle_call(:defaults, _from, defaults), do: {:reply, {:ok, defaults}, defaults}
end
