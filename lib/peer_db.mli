type 'a t

val init : Peer.t list -> 'a -> 'a t
val update_value : 'a t -> Peer.t -> 'a -> 'a t
val get_value : 'a t -> Peer.t -> 'a option
val get_value_exn : 'a t -> Peer.t -> 'a

