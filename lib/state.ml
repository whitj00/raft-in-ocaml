open! Core
open! Async

module Term = struct
  type t = int

  let init () : t = 0
  let next t = t + 1
end

module Peer = struct
  type t = { id : int; address : string }

  let equals p1 p2 = p1.id = p2.id
end

module Command = struct
  type t = string
end

module Log_entry = struct
  type t = { term : Term.t; command : Command.t }
end

module Peer_index_list = struct
  type t = (Peer.t * int) list

  let init (peers : Peer.t list) : t = List.map peers ~f:(fun peer -> (peer, 0))

  let update (t : t) peer index : t =
    List.map t ~f:(fun (p, i) ->
        if Peer.equals p peer then (p, index) else (p, i))
end

module Leader_volatile_state = struct
  type t = { next_index : Peer_index_list.t; match_index : Peer_index_list.t }

  let init (peers : Peer.t list) : t =
    {
      next_index = Peer_index_list.init peers;
      match_index = Peer_index_list.init peers;
    }

  let update_next_index (t : t) peer index : t =
    { t with next_index = Peer_index_list.update t.next_index peer index }

  let update_match_index (t : t) peer index : t =
    { t with match_index = Peer_index_list.update t.match_index peer index }
end

type t = {
  current_term : Term.t;
  voted_for : Peer.t option;
  log : Log_entry.t list;
  commit_index : int;
  last_applied : int;
  leader_state : Leader_volatile_state.t option;
}
