open Core

module State = struct
  type t = { next_index : int Peer_db.t; match_index : int Peer_db.t }
  [@@deriving fields]

  let init ~peers ~last_log_index =
    { next_index = Peer_db.init peers (last_log_index + 1); match_index = Peer_db.init peers 0}

  let update_next_index t peer index =
    { t with next_index = Peer_db.update_value t.next_index peer index }

  let get_next_index_exn t peer =
    Peer_db.get_value_exn t.next_index peer

  let update_match_index t peer index =
    { t with match_index = Peer_db.update_value t.match_index peer index }

  let get_match_index_exn t peer =
    Peer_db.get_value_exn t.match_index peer  
end
