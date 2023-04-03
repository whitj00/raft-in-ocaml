module Command : sig
  type t [@@deriving bin_io]
end

type t [@@deriving bin_io, sexp]

val command : t -> Command.t
val term : t -> int
