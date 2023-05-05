open Core

module T : sig
  type t [@@deriving sexp, compare]
end

include T

val conn : t -> Persistent_connection.Rpc.t option
val port : t -> int
val host : t -> string
val to_host_and_port : t -> Host_and_port.t
val equal : t -> t -> bool
val equal_host_port : t -> Host_and_port.t -> bool
val to_string : t -> string
val compare : t -> t -> int
val create : host_and_port:Host_and_port.t -> t

type comparator_witness = Comparable.Make(T).comparator_witness

val comparator : (t, comparator_witness) Base.Comparator.t
