open Core

module Command = struct
  type t = unit [@@deriving bin_io, sexp]
end

type t = { term : int; command : Command.t } [@@deriving fields, bin_io, sexp]
