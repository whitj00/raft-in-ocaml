open Core
open Async
module Rpc = Raft_rpc

let append_entries state call =
  let open Or_error.Let_syntax in
  let current_term = State.current_term state in
  let%bind () =
    let term = Rpc.Append_call.term call in
    match term < current_term with
    | true -> return ()
    | false ->
        Or_error.errorf "Term is too old [term: %d] [current_term: %d]" term
          current_term
  in
  let%bind () =
    match List.nth (State.log state) (Rpc.Append_call.prev_log_index call) with
    | Some entry ->
        if Log_entry.term entry = Rpc.Append_call.prev_log_term call then
          return ()
        else Or_error.errorf "Term mismatch"
    | None -> Or_error.errorf "No entry at prevLogIndex"
  in
  let new_log =
    List.take (State.log state) (Rpc.Append_call.prev_log_index call)
    @ Rpc.Append_call.entries call
  in
  let new_commit_index =
    min (Rpc.Append_call.leader_commit call) (List.length new_log)
  in
  return { state with log = new_log; commit_index = new_commit_index }

let handle_append_entries_call peer state call =
  let term = Rpc.Append_call.term call in
  let current_term = State.current_term state in
  let leader = Peer.to_host_and_port peer |> Some in
  let state = State.update_term_and_convert_if_outdated state term leader in
  printf "%d: Received append entries from %s\n" current_term
    (Peer.to_string peer);
  match State.peer_type state with
  | Follower _ ->
      let response = append_entries state call in
      let response, state =
        match response with
        | Ok state ->
            let response = Rpc.Append_response.create ~term ~success:true in
            (response, state)
        | Error _ ->
            let response = Rpc.Append_response.create ~term ~success:false in
            (response, state)
      in
      let event = response |> Rpc.Event.AppendEntriesResponse in
      let from = State.self state |> Peer.to_host_and_port in
      let () = Rpc.send_event { event; from } peer in
      let state = State.reset_election_timer state in
      Ok state
  | Leader _ -> Ok state
  | Candidate _ -> State.convert_to_follower state ~leader |> Ok

let handle_append_entries_response peer state response =
  printf "%d: Append entries response from %s\n" (State.current_term state)
    (Peer.to_string peer);
  let term = Rpc.Append_response.term response in
  State.update_term_and_convert_if_outdated state term None |> Ok
