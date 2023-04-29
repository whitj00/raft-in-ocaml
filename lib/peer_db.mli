type 'a t

val init : Peer.t list -> 'a -> 'a t
val update_value : 'a t -> Peer.t -> 'a -> 'a t
