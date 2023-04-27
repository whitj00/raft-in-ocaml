module State : sig
  type t

  val votes : t -> Peer.t list
  val init : Peer.t -> t
  val add_vote : t -> Peer.t -> int -> t
  val count_votes : t -> int
end
