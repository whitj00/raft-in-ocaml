module Command : sig
  type t = Increment | Decrement [@@deriving bin_io, sexp]
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
val last_index : t -> int
val last_log_term : t -> int
val get_index : t -> int -> Entry.t option
val append : t -> t -> t
val append_one : t -> Entry.t -> t
val take : t -> int -> t
val entries_from : t -> int -> t
val get_term_exn : t -> int -> int
val get_state : t -> int