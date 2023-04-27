open Core
open Async
module Rpc = Raft_rpc

let reset_timer state = State.set_heartbeat_timer state (Time.now ())

let send state =
  let term = State.current_term state in
  printf "%d: Sending heartbeat\n" term;
  let peers = State.peers state in
  let logs = State.log state in
  let prev_log_index = List.length logs - 1 in
  let prev_log_term =
    match List.last logs with Some v -> Log_entry.term v | None -> 0
  in
  let entries = [] in
  let leader_commit = State.commit_index state in
  let leader = State.self state |> Peer.to_host_and_port in
  let heartbeat =
    Rpc.Append_call.create ~term ~prev_log_index ~prev_log_term ~entries
      ~leader_commit ~leader
  in
  let event = heartbeat |> Rpc.Event.AppendEntriesCall in
  let from = State.self state |> Peer.to_host_and_port in
  let () = List.iter peers ~f:(Rpc.send_event { event; from }) in
  reset_timer state
