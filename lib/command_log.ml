open Core
open! Async

module Command = struct
  type t = Increment | Decrement | AddServer of Host_and_port.t
  [@@deriving bin_io, sexp]
end

module State_machine = struct
  type t = int [@@deriving bin_io, sexp]

  let init () = 0

  let apply t = function
    | Command.Increment -> t + 1
    | Command.Decrement -> t - 1
    | AddServer _ -> t
end

module Entry = struct
  type t = { term : int; command : Command.t } [@@deriving fields, bin_io, sexp]

  let create = Fields.create
end

type t = Entry.t list [@@deriving bin_io, sexp]

let init () = []
let create (a : Entry.t list) : t = a
let create_one (a : Entry.t) : t = [ a ]

let get_unique_peers t =
  let start_set = Set.empty (module Host_and_port) in
  List.fold t ~init:start_set ~f:(fun acc entry ->
      match Entry.command entry with
      | AddServer peer -> Set.add acc peer
      | _ -> acc)
  |> Set.to_list

let last_index t = List.length t

let last_log_term t =
  match List.last t with Some v -> Entry.term v | None -> 0

let get_index (t : t) index = List.nth t (index - 1)
let append l1 l2 = l1 @ l2
let append_one (i : Entry.t) (l1 : t) : t = l1 @ [ i ]
let take t n = List.take t n

(* get log entries starting at nextIndex *)
let entries_from t nextIndex = List.drop t (nextIndex - 1)

let get_term_exn t index =
  match index with
  | 0 -> 0
  | _ -> (
      match get_index t index with
      | Some v -> Entry.term v
      | None -> failwith "get_term_exn")

let get_state t =
  let initial_state = State_machine.init () in
  List.fold t ~init:initial_state ~f:(fun state entry ->
      State_machine.apply state (Entry.command entry))

let length t = List.length t
