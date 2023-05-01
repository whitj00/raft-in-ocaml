open Core
open Async

module Call = struct
  type t = Server_rpc.Append_call.t [@@deriving sexp]

  let create = Server_rpc.Append_call.create

  let append_entries state (call : t) =
    let open Or_error.Let_syntax in
    let current_term = State.current_term state in
    let cmd_log = State.log state in
    let prev_log_index = Server_rpc.Append_call.prev_log_index call in
    let%bind () =
      let term = Server_rpc.Append_call.term call in
      match term < current_term with
      | false -> return ()
      | true ->
          Or_error.errorf "Term is too old [term: %d] [current_term: %d]" term
            current_term
    in
    let%bind () =
      match prev_log_index with
      | 0 -> return ()
      | _ -> (
          match Command_log.get_index cmd_log prev_log_index with
          | Some entry -> (
              match
                Command_log.Entry.term entry
                = Server_rpc.Append_call.prev_log_term call
              with
              | true -> return ()
              | false -> Or_error.errorf "Term mismatch")
          | None -> Or_error.errorf "No entry at prevLogIndex %d" prev_log_index
          )
    in
    let new_log =
      let entries = Server_rpc.Append_call.entries call in
      Command_log.append (Command_log.take cmd_log prev_log_index) entries
    in
    let new_commit_index =
      min
        (Server_rpc.Append_call.leader_commit call)
        (Command_log.last_index new_log)
    in
    let state = { state with log = new_log; commit_index = new_commit_index } in
    let state = State.update_peer_list state in
    return state

  let handle peer state call =
    let term = Server_rpc.Append_call.term call in
    let leader = Peer.to_host_and_port peer |> Some in
    let state = State.update_term_and_convert_if_outdated state term leader in
    let term = State.current_term state in
    printf "%d: Received append entries from %s\n" term (Peer.to_string peer);
    match State.peer_type state with
    | Follower follower_state ->
        let state =
          match Follower.State.is_following follower_state leader with
          | true -> state
          | false -> State.convert_to_follower state ~leader
        in
        let result = append_entries state call in
        let response, state =
          match result with
          | Ok state ->
              printf
                "%d: Sending append entries success to %s (state: %d) (log \
                 length: %d)\n"
                term (Peer.to_string peer)
                (Command_log.get_state state.log)
                (Command_log.length state.log);
              let state = State.reset_election_timer state in
              let matchIndex = Command_log.last_index state.log in
              let response =
                Server_rpc.Append_response.create ~term ~success:true
                  ~matchIndex
              in
              (response, state)
          | Error e ->
              printf
                "%d: Sending append entries error to %s (state: %d) (log \
                 length: %d): %s\n"
                term (Peer.to_string peer)
                (Command_log.get_state state.log)
                (Command_log.length state.log)
                (Error.to_string_hum e);
              let matchIndex = Command_log.last_index state.log in
              let response =
                Server_rpc.Append_response.create ~term ~success:false
                  ~matchIndex
              in
              (response, state)
        in
        let event = response |> Server_rpc.Event.AppendEntriesResponse in
        let from = State.self state |> Peer.to_host_and_port in
        let%bind () = Server_rpc.send_event { event; from } peer in
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
    let heartbeat =
      Call.create ~term ~prev_log_index ~prev_log_term ~entries ~leader_commit
    in
    let event = heartbeat |> Server_rpc.Event.AppendEntriesCall in
    let from = State.self state |> Peer.to_host_and_port in
    let remote_peers = State.remote_nodes state in
    printf "%d: Sending heartbeat (peers: %d)\n" term (List.length remote_peers);
    let%bind () =
      Deferred.List.iter remote_peers ~f:(Server_rpc.send_event { event; from })
    in
    let state = State.reset_timer state in
    return (Ok state)
end

module Response = struct
  type t = Server_rpc.Append_response.t [@@deriving sexp]

  let handle peer state response =
    let success = Server_rpc.Append_response.success response in
    let term = Server_rpc.Append_response.term response in
    let state = State.update_term_and_convert_if_outdated state term None in
    let matchIndex = Server_rpc.Append_response.matchIndex response in
    match State.peer_type state with
    | Follower _ | Candidate _ -> Ok state |> return
    | Leader leader_state -> (
        let leader_state =
          Leader.State.update_match_index leader_state peer matchIndex
        in
        let leader_state =
          Leader.State.update_next_index leader_state peer (matchIndex + 1)
        in
        let state = { state with peer_type = Leader leader_state } in
        match success with
        | false ->
            printf "%d: Append entries failure from %s (matchIndex: %d)\n"
              (State.current_term state)
              (Host_and_port.to_string peer)
              matchIndex;
            let%bind () = State.update_peers state in
            Ok state |> return
        | true ->
            printf "%d: Append entries success from %s (matchIndex: %d)\n"
              (State.current_term state)
              (Host_and_port.to_string peer)
              matchIndex;
            Ok state |> return)
end
