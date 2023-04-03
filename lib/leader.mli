module State : sig
  type t = { next_index : int Peer_db.t; match_index : int Peer_db.t }

  val match_index : t -> int Peer_db.t
  val next_index : t -> int Peer_db.t
  val init : Peer.t list -> t
  val update_next_index : t -> Peer.t -> int -> t
  val update_match_index : t -> Peer.t -> int -> t
end
