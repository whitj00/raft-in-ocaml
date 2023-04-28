open Core
open Async
module Rpc = Raft_rpc

module Call = struct
  type t = Rpc.Append_call.t [@@deriving sexp]

  let create = Rpc.Append_call.create

  let append_entries state (call : t) =
    let open Or_error.Let_syntax in
    let current_term = State.current_term state in
    let cmd_log = State.log state in
    let prev_log_term = Rpc.Append_call.prev_log_index call in
    let%bind () =
      let term = Rpc.Append_call.term call in
      match term < current_term with
      | false -> return ()
      | true ->
          Or_error.errorf "Term is too old [term: %d] [current_term: %d]" term
            current_term
    in
    let%bind () =
      match prev_log_term with
      | -1 -> return ()
      | _ -> (
          match Command_log.get_index cmd_log prev_log_term with
          | Some entry -> (
              match
                Command_log.Entry.term entry
                = Rpc.Append_call.prev_log_term call
              with
              | true -> return ()
              | false -> Or_error.errorf "Term mismatch")
          | None -> Or_error.errorf "No entry at prevLogIndex %d" prev_log_term)
    in
    let new_log =
      let entries = Rpc.Append_call.entries call in
      Command_log.append (Command_log.take cmd_log prev_log_term) entries
    in
    let new_commit_index =
      min (Rpc.Append_call.leader_commit call) (Command_log.length new_log)
    in
    return { state with log = new_log; commit_index = new_commit_index }

  let handle peer state call =
    let term = Rpc.Append_call.term call in
    let leader = Peer.to_host_and_port peer |> Some in
    let state = State.update_term_and_convert_if_outdated state term leader in
    let current_term = State.current_term state in
    printf "%d: Received append entries from %s\n" current_term
      (Peer.to_string peer);
    match State.peer_type state with
    | Follower _ ->
        let response = append_entries state call in
        let response, state =
          match response with
          | Ok state ->
              let state = State.reset_election_timer state in
              let response = Rpc.Append_response.create ~term:current_term ~success:true in
              (response, state)
          | Error e ->
              print_endline (Error.to_string_hum e);
              let response = Rpc.Append_response.create ~term:current_term ~success:false in
              (response, state)
        in
        let event = response |> Rpc.Event.AppendEntriesResponse in
        let from = State.self state |> Peer.to_host_and_port in
        let%bind () = Rpc.send_event { event; from } peer in
        Ok state |> return
    | Leader _ -> Ok state |> return
    | Candidate _ -> State.convert_to_follower state ~leader |> Ok |> return
end

module Heartbeat = struct
  let send state =
    let term = State.current_term state in
    let logs = State.log state in
    let prev_log_index = Command_log.last_index logs in
    let prev_log_term = Command_log.last_log_term logs in
    let entries = Command_log.init () in
    let leader_commit = State.commit_index state in
    let leader = State.self state |> Peer.to_host_and_port in
    let heartbeat =
      Call.create ~term ~prev_log_index ~prev_log_term ~entries ~leader_commit
        ~leader
    in
    let event = heartbeat |> Rpc.Event.AppendEntriesCall in
    let from = State.self state |> Peer.to_host_and_port in
    let remote_peers = State.remote_nodes state in
    printf "%d: Sending heartbeat\n" term;
    let%bind () =
      Deferred.List.iter remote_peers ~f:(Rpc.send_event { event; from })
    in
    printf "%d: Sent heartbeat\n" term;
    let state = State.reset_timer state in
    return (Ok state)
end

module Response = struct
  let handle peer state response =
    printf "%d: Append entries response from %s\n" (State.current_term state)
      (Peer.to_string peer);
    let term = Rpc.Append_response.term response in
    State.update_term_and_convert_if_outdated state term None |> Ok
end
