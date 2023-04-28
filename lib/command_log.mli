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
end

type t [@@deriving bin_io, sexp]

val init : unit -> t
val last_index : t -> int
val last_log_term : t -> int
val get_index : t -> int -> Entry.t option
val append : t -> t -> t
val take : t -> int -> t
val length : t -> int
