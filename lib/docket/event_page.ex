defmodule Docket.EventPage do
  @moduledoc """
  One page of retained run events with the retention bounds observed
  alongside it.

  A page carries the committed events whose sequence is greater than the
  requested cursor, in ascending sequence order and bounded by the requested
  limit. It also carries the retention bounds and the owning run's latest
  committed event sequence, all read from one consistent snapshot so a caller
  can reason about progress and pruning without a second query.

  Sequence gaps are legal: persistence filtering and retention pruning both
  leave holes, so consecutive pages and the bounds are not promised
  contiguous.

  `latest_seq` is the run's committed event counter and is present even when
  history is fully pruned. A fully pruned history is therefore detectable as
  `latest_seq > 0` with `latest_available_seq == nil`.
  """

  @typedoc """
  Fields:

    * `events` — retained events with sequence greater than the requested
      cursor, ascending by sequence, at most the requested limit.
    * `next_after_seq` — the sequence to pass as the next cursor: the last
      returned event's sequence, or the supplied cursor when the page is
      empty.
    * `has_more?` — whether a retained event exists beyond `next_after_seq`,
      computed from the same snapshot as `latest_available_seq`.
    * `oldest_available_seq` — the lowest retained sequence, or `nil` when no
      events are retained.
    * `latest_available_seq` — the highest retained sequence, or `nil` when no
      events are retained.
    * `latest_seq` — the owning run's latest committed event sequence,
      independent of retention.
  """
  @type t :: %__MODULE__{
          events: [Docket.Event.t()],
          next_after_seq: non_neg_integer(),
          has_more?: boolean(),
          oldest_available_seq: pos_integer() | nil,
          latest_available_seq: pos_integer() | nil,
          latest_seq: non_neg_integer()
        }

  @enforce_keys [
    :events,
    :next_after_seq,
    :has_more?,
    :oldest_available_seq,
    :latest_available_seq,
    :latest_seq
  ]
  defstruct [
    :events,
    :next_after_seq,
    :has_more?,
    :oldest_available_seq,
    :latest_available_seq,
    :latest_seq
  ]

  @doc """
  Builds a page from a decoded event list and the bounds observed with it.

  Derivation rules, shared by every backend so pagination cannot drift:

    * `next_after_seq` is the last event's sequence, or `after_seq` when the
      page is empty.
    * `has_more?` is true when a retained event exists beyond `next_after_seq`,
      i.e. `latest` is present and greater than `next_after_seq`.

  `oldest`, `latest`, and `latest_seq` are recorded verbatim from the same
  snapshot that produced `events`.
  """
  @spec new(
          [Docket.Event.t()],
          non_neg_integer(),
          pos_integer() | nil,
          pos_integer() | nil,
          non_neg_integer()
        ) :: t()
  def new(events, after_seq, oldest, latest, latest_seq) do
    next_after_seq =
      case events do
        [] -> after_seq
        _ -> List.last(events).seq
      end

    %__MODULE__{
      events: events,
      next_after_seq: next_after_seq,
      has_more?: latest != nil and latest > next_after_seq,
      oldest_available_seq: oldest,
      latest_available_seq: latest,
      latest_seq: latest_seq
    }
  end
end
