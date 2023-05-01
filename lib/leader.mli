open Core

module State : sig
  type t = { next_index : int Peer_db.t; match_index : int Peer_db.t }

  val match_index : t -> int Peer_db.t
  val next_index : t -> int Peer_db.t
  val init : peers:Peer.t list -> last_log_index:int -> t
  val add_peer : t -> Host_and_port.t -> last_log_index:int -> t
  val update_next_index : t -> Host_and_port.t -> int -> t
  val update_match_index : t -> Host_and_port.t -> int -> t
  val get_next_index_exn : t -> Peer.t -> int
  val get_match_index_exn : t -> Peer.t -> int
end
