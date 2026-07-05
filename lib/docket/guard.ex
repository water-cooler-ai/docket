defmodule Docket.Guard do
  @moduledoc """
  Durable guard expression descriptors evaluated on graph edges.

  `changed/1`, `version_at_least/2`, `exists/1`, `equals/2`, `all/1`,
  `any/1`, and `not/1` build boolean expressions. `path/2` (and a bare
  channel ID) is a reference expression usable only inside `exists/1` and
  `equals/2`; it is not a standalone guard.
  """

  import Kernel, except: [not: 1]

  defstruct [:op, args: []]

  @type op :: :all | :any | :changed | :equals | :exists | :not | :path | :version_at_least
  @type t :: %__MODULE__{op: op(), args: [term()]}

  @spec changed(String.t()) :: t()
  def changed(channel), do: expr(:changed, [channel])

  @spec version_at_least(String.t(), non_neg_integer()) :: t()
  def version_at_least(channel, version), do: expr(:version_at_least, [channel, version])

  @spec path(String.t(), [String.t() | atom() | integer()]) :: t()
  def path(channel, path), do: expr(:path, [channel, path])

  @spec exists(term()) :: t()
  def exists(ref), do: expr(:exists, [ref])

  @spec equals(term(), term()) :: t()
  def equals(ref, value), do: expr(:equals, [ref, value])

  @spec all([t()]) :: t()
  def all(expressions), do: expr(:all, expressions)

  @spec any([t()]) :: t()
  def any(expressions), do: expr(:any, expressions)

  @spec unquote(:not)(t()) :: t()
  def unquote(:not)(expression), do: expr(:not, [expression])

  defp expr(op, args), do: %__MODULE__{op: op, args: args}
end
