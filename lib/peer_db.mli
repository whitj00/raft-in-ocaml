open Core

type 'a t

val init : Peer.t list -> 'a -> 'a t
val update_value : 'a t -> Host_and_port.t -> 'a -> 'a t
val add_peer : 'a t -> Host_and_port.t -> 'a -> 'a t
val remove_peer : 'a t -> Host_and_port.t -> 'a t
val get_value : 'a t -> Peer.t -> 'a option
val get_value_exn : 'a t -> Peer.t -> 'a
val majority_have_at_least_n : int t -> int -> bool
