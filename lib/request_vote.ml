open! Core
open Async
module Rpc = Raft_rpc

let request_vote peer state call =
  let open Or_error.Let_syntax in
  let term = Rpc.Request_call.term call in
  let current_term = State.current_term state in
  let%bind () =
    match term < current_term with
    | true -> Or_error.errorf "Term is too old: %d" (Rpc.Request_call.term call)
    | false -> return ()
  in
  let%bind () =
    match State.voted_for state with
    | None -> return ()
    | Some voted_for -> (
        match Peer.equal voted_for peer with
        | true -> return ()
        | false -> Or_error.errorf "Already voted for someone else")
  in
  let%bind () =
    match
      Command_log.get_index (State.log state)
        (Rpc.Request_call.last_log_index call)
    with
    | Some entry ->
        if Command_log.Entry.term entry = Rpc.Request_call.last_log_term call
        then return ()
        else Or_error.errorf "Log_term mismatch"
    | None -> return ()
  in
  { state with voted_for = Some peer } |> State.reset_election_timer |> return

let handle_request_vote peer state call =
  let term = Rpc.Request_call.term call in
  let state = State.update_term_and_convert_if_outdated state term None in
  let response = request_vote peer state call in
  let current_term = State.current_term state in
  let response, state =
    match response with
    | Ok state ->
        printf "%d: granted vote request from %s\n" current_term
          (Peer.to_string peer);
        let response =
          Rpc.Request_response.create ~term:current_term ~success:true
        in
        (response, state)
    | Error e ->
        printf "%d: denied vote request from %s: %s\n" current_term
          (Peer.to_string peer) (Error.to_string_hum e);
        let response =
          Rpc.Request_response.create ~term:current_term ~success:false
        in
        (response, state)
  in
  let event = response |> Rpc.Event.RequestVoteResponse in
  let from = State.self state |> Peer.to_host_and_port in
  let request = Rpc.Remote_call.create ~event ~from in
  let%bind () = Rpc.send_event request peer in
  Ok state |> return

let handle_request_vote_response peer state response =
  let open Or_error.Let_syntax in
  let term = Rpc.Request_response.term response in
  let response = Rpc.Request_response.success response in
  let state = State.update_term_and_convert_if_outdated state term None in
  let current_term = State.current_term state in
  match State.peer_type state with
  | Follower _ | Leader _ -> return state
  | Candidate candidate_state ->
      let candidate_state =
        let current_votes = Candidate.State.count_votes candidate_state in
        let peer_str = Peer.to_string peer in
        match response with
        | true -> (
            match term = current_term with
            | true ->
                printf "%d: Received vote from %s [votes=%d]\n" current_term
                  peer_str (1 + current_votes);
                Candidate.State.add_vote candidate_state peer current_term
            | false ->
                printf "%d: Stale vote from %s for term %d [votes=%d]\n"
                  current_term peer_str term current_votes;
                candidate_state)
        | false ->
            printf "%d: Received vote rejection from %s [votes=%d]\n"
              current_term peer_str current_votes;
            candidate_state
      in
      let state =
        { state with peer_type = Candidate candidate_state }
        |> State.convert_if_votes
      in
      return state
