open! Core
open Async
module Rpc = Raft_rpc

let handle_request_vote_response peer state response =
  let open Or_error.Let_syntax in
  let update_term state response =
    match Rpc.Request_response.term response > State.current_term state with
    | true -> { state with current_term = Rpc.Request_response.term response }
    | false -> state
  in
  match State.peer_type state with
  | Follower _ | Leader _ -> return state
  | Candidate candidate_state ->
      let new_state = update_term state response in
      let term = State.current_term new_state in
      let candidate_state =
        match Rpc.Request_response.success response with
        | true ->
            printf "%d: Received vote from %s [votes=%d]\n"
              (State.current_term state) (Peer.to_string peer)
              (Candidate.State.count_votes candidate_state);
            Candidate.State.add_vote candidate_state peer term
        | false ->
            printf "%d: Received vote rejection from %s [votes=%d]\n"
              (State.current_term state) (Peer.to_string peer)
              (Candidate.State.count_votes candidate_state);
            candidate_state
      in
      let new_state =
        { new_state with peer_type = Candidate candidate_state }
        |> State.convert_if_votes
      in
      return new_state

let handle_append_entries_response peer state response =
  printf "%d: Append entries response from %s\n" (State.current_term state)
    (Peer.to_string peer);
  let update_term state response =
    match Rpc.Append_response.term response > State.current_term state with
    | true ->
        { state with current_term = Rpc.Append_response.term response }
        |> State.convert_to_follower
    | false -> state
  in
  update_term state response |> Ok

let request_vote peer state call =
  let open Or_error.Let_syntax in
  let%bind () =
    match State.peer_type state with
    | Follower _ -> return ()
    | Candidate _ -> return ()
    | Leader _ -> Or_error.errorf "I am the leader"
  in
  let%bind () =
    if Rpc.Request_call.term call >= State.current_term state then return ()
    else Or_error.errorf "Term is too old"
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
    match List.nth (State.log state) (Rpc.Request_call.last_log_index call) with
    | Some entry ->
        if Log_entry.term entry = Rpc.Request_call.last_log_term call then
          return ()
        else Or_error.errorf "Term mismatch"
    | None -> return ()
  in
  { state with voted_for = Some peer } |> State.convert_to_follower |> return

let handle_request_vote peer state call =
  let term = Int.max (Rpc.Request_call.term call) (State.current_term state) in
  let state = { state with current_term = term } in
  let response = request_vote peer state call in
  let response, state =
    match response with
    | Ok new_state ->
        printf "%d: granted vote request from %s\n" term (Peer.to_string peer);
        let response = Rpc.Request_response.create ~term ~success:true in
        (response, new_state)
    | Error e ->
        printf "%d: denied vote request from %s: %s\n" term
          (Peer.to_string peer) (Error.to_string_hum e);
        let response = Rpc.Request_response.create ~term ~success:false in
        (response, state)
  in
  let event = response |> Rpc.Event.RequestVoteResponse in
  let from = State.self state |> Peer.to_host_and_port in
  let request = Rpc.Remote_call.create ~event ~from in
  let () = Rpc.send_event request peer in
  Ok state
