open! Core

module Peer = struct
  module T = struct
    type t = { address : string }
    [@@deriving fields, bin_io, compare, sexp, compare, equal]

    let create = Fields.create
    let empty = { address = "" }
    let self = { address = "self" }
  end

  include T
  include Comparator.Make (T)

  let create = Fields.create
end

module Command = struct
  type t = string [@@deriving bin_io, sexp]
end

module Log_entry = struct
  type t = { term : int; command : Command.t } [@@deriving fields, bin_io, sexp]
end

module Peer_index_list = struct
  type t = (Peer.t * int) list

  let init (peers : Peer.t list) : t = List.map peers ~f:(fun peer -> (peer, 0))

  let update (t : t) peer index : t =
    List.map t ~f:(fun (p, i) ->
        if Peer.equal p peer then (p, index) else (p, i))
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

module Candidate_volatile_state = struct
  (* Type t is a Peer.t Set.t *)
  type t = { votes : Peer.t List.t } [@@deriving fields]

  let init () : t = { votes = [ Peer.self ] }

  (* Adds a peer if not already in the list *)
  let add_vote (t : t) peer : t =
    match List.exists t.votes ~f:(fun p -> Peer.equal p peer) with
    | true -> t
    | false -> { votes = peer :: t.votes }

  let count_votes t : int = List.length t.votes
end

module Follower_volatile_state = struct
  (* Type t is a Peer.t Set.t *)
  type t = { election_timeout : Time.Span.t; last_election : Time.t }
  [@@deriving fields]

  let create_election_timer () = Time.Span.of_sec (Random.float_range 1.5 3.0)

  let reset_election_timer () =
    { election_timeout = create_election_timer (); last_election = Time.now () }

  let init =
    { election_timeout = create_election_timer (); last_election = Time.epoch }
end

module Peer_type = struct
  type t =
    | Follower of Follower_volatile_state.t
    | Candidate of Candidate_volatile_state.t
    | Leader of Leader_volatile_state.t
end

module Append_call = struct
  type t = {
    term : int;
    prev_log_index : int;
    prev_log_term : int;
    entries : Log_entry.t list;
    leader_commit : int;
  }
  [@@deriving fields, bin_io, sexp]

  let create = Fields.create
end

module Request_call = struct
  type t = { term : int; last_log_index : int; last_log_term : int }
  [@@deriving fields, bin_io]

  let create = Fields.create
end

module Append_response = struct
  type t = { term : int; success : bool } [@@deriving fields, bin_io]

  let create = Fields.create
end

module Request_response = struct
  type t = { term : int; success : bool } [@@deriving fields, bin_io]

  let create = Fields.create
end

module State = struct
  type t = {
    current_term : int;
    voted_for : Peer.t option;
    log : Log_entry.t list;
    commit_index : int;
    last_applied : int;
    leader_state : Leader_volatile_state.t option;
    peers : Peer.t list;
    peer_type : Peer_type.t;
    heartbeat_timeout : Time.Span.t;
    last_hearbeat : Time.t;
  }
  [@@deriving fields]

  let create_heartbeat_timer () = Time.Span.of_sec 0.5
  let reset_heartbeat_timer state = { state with last_hearbeat = Time.now () }

  let create ~peers =
    {
      current_term = 0;
      voted_for = None;
      log = [];
      commit_index = 0;
      last_applied = 0;
      leader_state = None;
      peers;
      peer_type = Follower Follower_volatile_state.init;
      heartbeat_timeout = create_heartbeat_timer ();
      last_hearbeat = Time.epoch;
    }
end

module Event = struct
  type t =
    | RequestVoteCall of Request_call.t
    | RequestVoteResponse of Request_response.t
    | AppendEntriesCall of Append_call.t
    | AppendEntriesResponse of Request_response.t
    | ElectionTimeout
    | HeartbeatTimeout
  [@@deriving bin_io]
end

let append_entries current_state call =
  let open Or_error.Let_syntax in
  let current_term = State.current_term current_state in
  let%bind () =
    if Append_call.term call > current_term then return ()
    else Or_error.errorf "Term is too old"
  in
  let%bind () =
    match
      List.nth (State.log current_state) (Append_call.prev_log_index call)
    with
    | Some entry ->
        if entry.term = Append_call.prev_log_term call then return ()
        else Or_error.errorf "Term mismatch"
    | None -> Or_error.errorf "No entry at prevLogIndex"
  in
  let new_log =
    List.take (State.log current_state) (Append_call.prev_log_index call)
    @ Append_call.entries call
  in
  let new_commit_index =
    min (Append_call.leader_commit call) (List.length new_log)
  in
  return { current_state with log = new_log; commit_index = new_commit_index }

let request_vote peer current_state call =
  let open Or_error.Let_syntax in
  let%bind () =
    if Request_call.term call > State.current_term current_state then return ()
    else Or_error.errorf "Term is too old"
  in
  let candidateID = peer in
  let%bind () =
    match State.voted_for current_state with
    | None -> return ()
    | Some voted_for -> (
        match Peer.equal voted_for candidateID with
        | true -> return ()
        | false -> Or_error.errorf "Already voted for someone else")
  in
  let%bind () =
    match
      List.nth (State.log current_state) (Request_call.last_log_index call)
    with
    | Some entry ->
        if entry.term = Request_call.last_log_term call then return ()
        else Or_error.errorf "Term mismatch"
    | None -> Or_error.errorf "No entry at lastLogIndex"
  in
  return { current_state with voted_for = Some candidateID }

let send_event _response _peer = assert false

let convert_to_leader state =
  print_endline "Converting to leader";
  let voted_for = None in
  let peers = State.peers state in
  let volatile_state = Leader_volatile_state.init peers in
  let peer_type = Peer_type.Leader volatile_state in
  { state with State.voted_for; peer_type }

let convert_if_votes state =
  match State.peer_type state with
  | Follower _ | Leader _ -> state
  | Candidate candidate_state ->
      let votes = Candidate_volatile_state.votes candidate_state in
      let peers = State.peers state in
      let majority = (List.length peers / 2) + 1 in
      if List.length votes >= majority then convert_to_leader state else state

let convert_to_candidate state =
  print_endline "Converting to candidate";
  let current_term = State.current_term state + 1 in
  let voted_for = Some Peer.self in
  let volatile_state = Candidate_volatile_state.init () in
  let peer_type = Peer_type.Candidate volatile_state in
  let new_state = { state with current_term; voted_for; peer_type } in
  let peers = State.peers new_state in
  let () =
    List.iter peers ~f:(fun peer ->
        let request = Request_call.create ~term:current_term in
        let () = send_event request peer in
        ())
  in
  let new_state = convert_if_votes new_state in
  new_state

let convert_to_follower state =
  print_endline "Converting to follower";
  let voted_for = None in
  let volatile_state = Follower_volatile_state.init in
  let peer_type = Peer_type.Follower volatile_state in
  { state with State.voted_for; peer_type }

let send_heartbeat state =
  print_endline "Sending heartbeat";
  let peers = State.peers state in
  let term = State.current_term state in
  let logs = State.log state in
  let prev_log_index = List.length logs - 1 in
  let prev_log_term = match List.last logs with
    | Some v -> Log_entry.term v
    | None -> 0 in
  let entries = [] in
  let leader_commit = State.commit_index state in
  let heartbeat =
    Append_call.create ~term ~prev_log_index ~prev_log_term ~entries
      ~leader_commit
  in
  Append_call.sexp_of_t heartbeat |> Sexp.to_string_hum |> print_endline;
  let () = List.iter peers ~f:(send_event heartbeat) in
  State.reset_heartbeat_timer state

let handle_heartbeat_timeout state =
  print_endline "Heartbeat timeout";
  match State.peer_type state with
  | Follower _ -> convert_to_candidate state |> Ok
  | Candidate _ -> convert_to_candidate state |> Ok
  | Leader _ ->
      let new_state = send_heartbeat state in
      Ok new_state

let handle_election_timeout state =
  print_endline "Election timeout";
  convert_to_candidate state |> Ok

let handle_request_vote peer current_state call =
  let response = request_vote peer current_state call in
  let term = State.current_term current_state in
  let response, state =
    match response with
    | Ok new_state ->
        let response = Request_response.create ~term ~success:true in
        (response, new_state)
    | Error _ ->
        let response = Request_response.create ~term ~success:false in
        (response, current_state)
  in
  let () = send_event response peer in
  Ok state

let handle_append_entries peer current_state call =
  match State.peer_type current_state with
  | Follower _ ->
      let response = append_entries current_state call in
      let term = State.current_term current_state in
      let response, state =
        match response with
        | Ok new_state ->
            let response = Append_response.create ~term ~success:true in
            (response, new_state)
        | Error _ ->
            let response = Append_response.create ~term ~success:false in
            (response, current_state)
      in
      let () = send_event response peer in
      Ok state
  | Leader _ ->
      print_endline "Leader received append entries";
      Ok current_state
  | Candidate _ ->
      print_endline "Candidate received append entries";
      Ok current_state

let update_term current_state response =
  match Request_response.term response > State.current_term current_state with
  | true ->
      {
        current_state with
        peer_type = Follower Follower_volatile_state.init;
        current_term = Request_response.term response;
      }
  | false -> current_state

let handle_request_vote_response peer current_state response =
  let open Or_error.Let_syntax in
  match State.peer_type current_state with
  | Follower _ | Leader _ -> return current_state
  | Candidate candidate_state ->
      let new_state = update_term current_state response in
      let candidate_state =
        match Request_response.success response with
        | true -> Candidate_volatile_state.add_vote candidate_state peer
        | false -> candidate_state
      in
      let new_state =
        { new_state with peer_type = Candidate candidate_state }
        |> convert_if_votes
      in
      return new_state
