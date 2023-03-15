open! Core
open! Async

module Peer = struct
  type t = { id : int; address : string }

  let equals p1 p2 = p1.id = p2.id
end

module Command = struct
  type t = string
end

module Log_entry = struct
  type t = { term : int; command : Command.t } [@@deriving fields]
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
  [@@deriving fields]

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

module Append_entries_call = struct
  type t = {
    term : int;
    leader_id : Peer.t;
    prev_log_index : int;
    prev_log_term : int;
    entries : Log_entry.t list;
    leader_commit : int;
  }
  [@@deriving fields]
end

type t = {
  current_term : int;
  voted_for : Peer.t option;
  log : Log_entry.t list;
  commit_index : int;
  last_applied : int;
  leader_state : Leader_volatile_state.t option;
}
[@@deriving fields]

let append_entries current_state call =
  let open Deferred.Or_error.Let_syntax in
  let current_term = current_term current_state in
  let term = Append_entries_call.term call in
  let%bind () =
    match term > current_term with
    | true -> return ()
    | false -> Deferred.Or_error.errorf "Term %d is too old" term
  in
  let log = log current_state in
  let prev_log_index = Append_entries_call.prev_log_index call in
  let prev_log_term = Append_entries_call.prev_log_term call in
  let%bind () =
    match List.nth log prev_log_index with
    | Some entry -> (
        match entry.term = prev_log_term with
        | true -> return ()
        | false -> Deferred.Or_error.errorf "Term mismatch")
    | None -> Deferred.Or_error.errorf "No entry at prevLogIndex"
  in
  (* If an existing entry conflicts has the same index as an entry in log but
     different terms delete the existing entry in our log and all that have a
     greater index *)
  (* Do the above *)
  let entries = Append_entries_call.entries call in
  let log = List.take log prev_log_index @ entries in
  let leader_commit = Append_entries_call.leader_commit call in
  let commit_index = min leader_commit (List.length log) in
  let last_applied = last_applied current_state in
  let leader_state = leader_state current_state in
  let voted_for = voted_for current_state in
  let new_state =
    { current_term; voted_for; log; commit_index; last_applied; leader_state }
  in
  return new_state
