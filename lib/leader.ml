open Core

module State = struct
  type t = { next_index : int Peer_db.t; match_index : int Peer_db.t }
  [@@deriving fields]

  let init peers =
    { next_index = Peer_db.init peers; match_index = Peer_db.init peers }

  let update_next_index t peer index =
    { t with next_index = Peer_db.update_value t.next_index peer index }

  let update_match_index t peer index =
    { t with match_index = Peer_db.update_value t.match_index peer index }
end
