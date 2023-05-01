open Core

module Command : sig
  type t = Increment | Decrement | AddServer of Host_and_port.t
  [@@deriving bin_io, sexp]
end

module State_machine : sig
  type t

  val init : unit -> t
  val apply : t -> Command.t -> t
end

module Entry : sig
  type t

  val term : t -> int
  val command : t -> Command.t
  val create : term:int -> command:Command.t -> t
end

type t [@@deriving bin_io, sexp]

val init : unit -> t
val create : Entry.t list -> t
val create_one : Entry.t -> t
val last_index : t -> int
val last_log_term : t -> int
val get_index : t -> int -> Entry.t option
val append : t -> t -> t
val append_one : Entry.t -> t -> t
val take : t -> int -> t
val entries_from : t -> int -> t
val get_term_exn : t -> int -> int
val get_state : t -> int
val get_unique_peers : t -> Host_and_port.t list
val length : t -> int
