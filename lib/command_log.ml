open Core

module Command = struct
  type t = unit [@@deriving bin_io, sexp]
end

module Entry = struct
  type t = { term : int; command : Command.t } [@@deriving fields, bin_io, sexp]
end

type t = Entry.t list [@@deriving bin_io, sexp]

let init () = []
let last_index t = List.length t - 1

let last_log_term t =
  match List.last t with Some v -> Entry.term v | None -> 0

let get_index (t : t) index = List.nth t index
let append l1 l2 = l1 @ l2
let take t index = List.take t index
let length t = List.length t
