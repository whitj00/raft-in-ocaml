open Core
open Async
module Rpc = Raft_rpc

let reset_timer state = State.set_heartbeat_timer state (Time.now ())

let send state =
  let term = State.current_term state in
  let logs = State.log state in
  let prev_log_index = Command_log.last_index logs in
  let prev_log_term = Command_log.last_log_term logs in
  let entries = Command_log.init () in
  let leader_commit = State.commit_index state in
  let leader = State.self state |> Peer.to_host_and_port in
  let heartbeat =
    Rpc.Append_call.create ~term ~prev_log_index ~prev_log_term ~entries
      ~leader_commit ~leader
  in
  let event = heartbeat |> Rpc.Event.AppendEntriesCall in
  let from = State.self state |> Peer.to_host_and_port in
  let remote_peers = State.remote_nodes state in
  printf "%d: Sending heartbeat\n" term;
  let%bind () =
    Deferred.List.iter remote_peers ~f:(Rpc.send_event { event; from })
  in
  printf "%d: Sent heartbeat\n" term;
  let state = reset_timer state in
  return (Ok state)
