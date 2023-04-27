open! Core
open Async
module Rpc = Raft_rpc

module Peer_type = struct
  type t =
    | Follower of Follower.State.t
    | Candidate of Candidate.State.t
    | Leader of Leader.State.t
end

module State = struct
  type t = {
    current_term : int;
    voted_for : Peer.t option;
    log : Log_entry.t list;
    commit_index : int;
    last_applied : int;
    leader_state : Leader.State.t option;
    peers : Peer.t list;
    peer_type : Peer_type.t;
    heartbeat_timeout : Time.Span.t;
    last_hearbeat : Time.t;
    election_timeout : Time.Span.t;
    last_election : Time.t;
    started_at : Time.t;
    self : Peer.t;
  }
  [@@deriving fields]

  let create_heartbeat_timer () = Time.Span.of_sec 0.75
  let reset_heartbeat_timer state = { state with last_hearbeat = Time.now () }
  let get_election_timeout () = Time.Span.of_sec (Random.float_range 1.5 6.)

  let reset_election_timer t =
    {
      t with
      election_timeout = get_election_timeout ();
      last_election = Time.now ();
    }

  let create ~peers port =
    {
      current_term = 0;
      voted_for = None;
      log = [];
      commit_index = 0;
      last_applied = 0;
      leader_state = None;
      peers;
      peer_type = Follower Follower.State.init;
      heartbeat_timeout = create_heartbeat_timer ();
      last_hearbeat = Time.now ();
      election_timeout = get_election_timeout ();
      last_election = Time.now ();
      started_at = Time.now ();
      self = Peer.create ~host:"127.0.0.1" ~port;
    }
end

let convert_to_follower state =
  match State.peer_type state with
  | Follower _ -> state
  | _ ->
      let term = State.current_term state in
      let voted_for = None in
      let volatile_state = Follower.State.init in
      let peer_type = Peer_type.Follower volatile_state in
      print_endline "----------------------------------";
      printf "%d: Converting to follower\n" term;
      print_endline "----------------------------------";
      { state with State.voted_for; peer_type } |> State.reset_election_timer

let send_heartbeat state =
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
  let heartbeat =
    Rpc.Append_call.create ~term ~prev_log_index ~prev_log_term ~entries
      ~leader_commit
  in
  let event = heartbeat |> Rpc.Event.AppendEntriesCall in
  let from = State.self state |> Peer.to_host_and_port in
  let () = List.iter peers ~f:(Rpc.send_event { event; from }) in
  State.reset_heartbeat_timer state

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
  { state with voted_for = Some peer } |> convert_to_follower |> return

let convert_to_leader state =
  let current_term = State.current_term state in
  print_endline "-----------------";
  printf "%d: Converting to leader\n" current_term;
  print_endline "-----------------";
  let voted_for = None in
  let peers = State.peers state in
  let volatile_state = Leader.State.init peers in
  let peer_type = Peer_type.Leader volatile_state in
  { state with State.voted_for; peer_type; current_term }

let convert_if_votes state =
  match State.peer_type state with
  | Follower _ | Leader _ -> state
  | Candidate candidate_state ->
      let votes = Candidate.State.votes candidate_state in
      let peers = State.peers state in
      let majority = ((List.length peers + 1) / 2) + 1 in
      if List.length votes >= majority then convert_to_leader state else state

let convert_to_candidate state =
  let current_term = State.current_term state + 1 in
  print_endline "-----------------";
  printf "%d: Converting to candidate\n" current_term;
  print_endline "-----------------";
  let voted_for = Some (State.self state) in
  let volatile_state = Candidate.State.init () in
  let peer_type = Peer_type.Candidate volatile_state in
  let new_state = { state with current_term; voted_for; peer_type } in
  let peers = State.peers new_state in
  let () =
    let term = current_term in
    let last_log_index = List.length (State.log new_state) - 1 in
    let last_log_term =
      match List.last (State.log new_state) with
      | Some v -> Log_entry.term v
      | None -> 0
    in
    let event =
      Rpc.Request_call.create ~term ~last_log_index ~last_log_term
      |> Rpc.Event.RequestVoteCall
    in
    let from = State.self state |> Peer.to_host_and_port in
    let request = Rpc.Remote_call.create ~event ~from in
    printf "%d: Requesting votes from peers\n" term;
    List.iter peers ~f:(Rpc.send_event request)
  in
  let new_state = convert_if_votes new_state |> State.reset_election_timer in
  new_state

let handle_heartbeat_timeout state =
  match State.peer_type state with
  | Leader _ -> State.reset_heartbeat_timer state |> send_heartbeat |> Ok
  | Follower _ | Candidate _ -> Ok state

let handle_election_timeout state =
  printf "%d: Election timeout\n" (State.current_term state);
  match State.peer_type state with
  | Leader _ -> Ok state
  | Follower _ -> convert_to_candidate state |> Ok
  | Candidate _ -> convert_to_follower state |> Ok

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

let append_entries state call =
  let open Or_error.Let_syntax in
  let current_term = State.current_term state in
  let%bind () =
    if Rpc.Append_call.term call < current_term then return ()
    else Or_error.errorf "Term is too old"
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

let handle_append_entries peer state call =
  let term = Int.max (Rpc.Append_call.term call) (State.current_term state) in
  printf "%d: Received append entries from %s\n" term (Peer.to_string peer);
  let state = { state with current_term = term } in
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
  | Candidate _ -> convert_to_follower state |> Ok

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
        |> convert_if_votes
      in
      return new_state

let handle_append_entries_response peer state response =
  printf "%d: Append entries response from %s\n" (State.current_term state)
    (Peer.to_string peer);
  let update_term state response =
    match Rpc.Append_response.term response > State.current_term state with
    | true ->
        { state with current_term = Rpc.Append_response.term response }
        |> convert_to_follower
    | false -> state
  in
  update_term state response |> Ok
