open! Core
open Async

module Peer = struct
  type t = { host : string; port : int } [@@deriving fields, sexp, bin_io]

  let create = Fields.create
  let self = { host = "127.0.0.1"; port = 8080 }
  let equal t1 t2 = String.equal t1.host t2.host && t1.port = t2.port
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

  let init () : t = { votes = [] }

  (* Adds a peer if not already in the list *)
  let add_vote (t : t) peer : t =
    match List.exists t.votes ~f:(fun p -> Peer.equal p peer) with
    | true -> t
    | false ->
        printf "Adding vote from %s\n" (Peer.sexp_of_t peer |> Sexp.to_string);
        { votes = peer :: t.votes }

  let count_votes t : int = List.length t.votes
end

module Follower_volatile_state = struct
  (* Type t is a Peer.t Set.t *)
  type t = unit

  let init = ()
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
  [@@deriving fields, bin_io, sexp]

  let create = Fields.create
end

module Append_response = struct
  type t = { term : int; success : bool } [@@deriving fields, bin_io, sexp]

  let create = Fields.create
end

module Request_response = struct
  type t = { term : int; success : bool } [@@deriving fields, bin_io, sexp]

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
    election_timeout : Time.Span.t;
    last_election : Time.t;
    started_at : Time.t;
    self : Peer.t;
  }
  [@@deriving fields]

  let create_heartbeat_timer () = Time.Span.of_sec 3.
  let reset_heartbeat_timer state = { state with last_hearbeat = Time.now () }
  let get_election_timeout () = Time.Span.of_sec (Random.float_range 7. 12.5)

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
      peer_type = Follower Follower_volatile_state.init;
      heartbeat_timeout = create_heartbeat_timer ();
      last_hearbeat = Time.now ();
      election_timeout = get_election_timeout ();
      last_election = Time.now ();
      started_at = Time.now ();
      self = Peer.create ~host:"127.0.0.1" ~port;
    }
end

module Event = struct
  type t =
    | RequestVoteCall of Request_call.t
    | RequestVoteResponse of Request_response.t
    | AppendEntriesCall of Append_call.t
    | AppendEntriesResponse of Append_response.t
    | ElectionTimeout
    | HeartbeatTimeout of (Time_unix.t * Time.Span.t)
  [@@deriving bin_io, sexp]

  let to_string t = Sexp.to_string (sexp_of_t t)
end

module Remote_call = struct
  type t = { event : Event.t; from : Peer.t } [@@deriving bin_io, sexp]

  let to_string t = Sexp.to_string (sexp_of_t t)
end

let raft_rpc =
  Rpc.Rpc.create ~name:"raft" ~version:0 ~bin_query:Remote_call.bin_t
    ~bin_response:Unit.bin_t

let send_event event peer =
  let host = Peer.host peer in
  let port = Peer.port peer in
  (* Use async_rpc to send event *)
  let where_to_connect = Tcp.Where_to_connect.of_host_and_port { host; port } in
  don't_wait_for
    (let%bind connection = Rpc.Connection.client where_to_connect in
     match connection with
     | Error e ->
         print_int port;
         print_endline "";
         printf "connerr: message failed to send to peer %s:%d %s \n" host port
           (Exn.to_string e)
         |> return
     | Ok connection -> (
         let%bind response = Rpc.Rpc.dispatch raft_rpc connection event in
         match response with
         | Error e ->
             printf "rpcerr: message failed to send to peer %s:%d\n%s" host port
               (Error.to_string_hum e)
             |> return
         | Ok () -> printf "Event sent to peer %s:%d\n" host port |> return))

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
  let prev_log_term =
    match List.last logs with Some v -> Log_entry.term v | None -> 0
  in
  let entries = [] in
  let leader_commit = State.commit_index state in
  let heartbeat =
    Append_call.create ~term ~prev_log_index ~prev_log_term ~entries
      ~leader_commit
  in
  Append_call.sexp_of_t heartbeat |> Sexp.to_string_hum |> print_endline;
  let event = heartbeat |> Event.AppendEntriesCall in
  let from = State.self state in
  let () = List.iter peers ~f:(send_event { event; from }) in
  State.reset_heartbeat_timer state

let request_vote current_state call =
  let open Or_error.Let_syntax in
  let%bind () =
    match State.peer_type current_state with
    | Follower _ -> return ()
    | Candidate _ -> return ()
    | Leader _ -> Or_error.errorf "I am the leader"
  in
  let%bind () =
  if Request_call.term call > State.current_term current_state then return ()
  else Or_error.errorf "Term is too old"
  in
  let self = State.self current_state in
  let%bind () =
    match State.voted_for current_state with
    | None -> return ()
    | Some voted_for -> (
        match Peer.equal voted_for self with
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
    | None -> return ()
  in
  { current_state with voted_for = Some self } |> convert_to_follower |> return

let convert_to_leader state =
  print_endline "Converting to leader";
  let current_term = State.current_term state + 1 in
  let voted_for = None in
  let peers = State.peers state in
  let volatile_state = Leader_volatile_state.init peers in
  let peer_type = Peer_type.Leader volatile_state in
  { state with State.voted_for; peer_type ; current_term }

let convert_if_votes state =
  match State.peer_type state with
  | Follower _ | Leader _ -> state
  | Candidate candidate_state ->
      let votes = Candidate_volatile_state.votes candidate_state in
      print_endline (Printf.sprintf "Votes: %d" (List.length votes));
      let peers = State.peers state in
      let majority = ((List.length peers + 1) / 2) + 1 in
      if List.length votes >= majority then convert_to_leader state else state

let convert_to_candidate state =
  print_endline "Converting to candidate";
  let current_term = State.current_term state + 1 in
  let voted_for = Some (State.self state) in
  let volatile_state = Candidate_volatile_state.init () in
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
      Request_call.create ~term ~last_log_index ~last_log_term
      |> Event.RequestVoteCall
    in
    let from = State.self new_state in
    let request : Remote_call.t = { event; from } in
    print_endline "Sending request vote";
    List.iter peers ~f:(send_event request)
  in
  let new_state = convert_if_votes new_state |> State.reset_election_timer in
  new_state

let handle_heartbeat_timeout state start span =
  printf "Heartbeat timeout start=%s span=%s\n" (Time.to_string_utc start)
    (Time.Span.to_short_string span);
  match State.peer_type state with
  | Leader _ -> State.reset_heartbeat_timer state |> send_heartbeat |> Ok
  | Follower _ | Candidate _ -> Ok state

let handle_election_timeout state =
  print_endline "Election timeout";
  match State.peer_type state with
  | Leader _ -> Ok state
  | Follower _ -> convert_to_candidate state |> Ok
  | Candidate _ -> convert_to_candidate state |> Ok

let handle_request_vote peer current_state call =
  print_endline "Received request vote";
  let response = request_vote current_state call in
  let term =
    Int.max (Request_call.term call) (State.current_term current_state)
  in
  let response, state =
    match response with
    | Ok new_state ->
        print_endline "Vote granted";
        let response = Request_response.create ~term ~success:true in
        (response, new_state)
    | Error e ->
        print_endline "Vote denied";
        print_endline (Error.to_string_hum e);
        print_endline
          (State.voted_for current_state
          |> Option.value ~default:Peer.self
          |> Peer.sexp_of_t |> Sexp.to_string_hum);
        let response = Request_response.create ~term ~success:false in
        (response, current_state)
  in
  let event = response |> Event.RequestVoteResponse in
  let from = State.self state in
  let () = send_event { event; from } peer in
  Ok state

let handle_append_entries peer current_state call =
  print_endline "Received append entries";
  let term =
    Int.max (Append_call.term call) (State.current_term current_state)
  in
  match State.peer_type current_state with
  | Follower _ ->
      let response = append_entries current_state call in
      let response, state =
        match response with
        | Ok new_state ->
            let response = Append_response.create ~term ~success:true in
            (response, new_state)
        | Error _ ->
            let response = Append_response.create ~term ~success:false in
            (response, current_state)
      in
      let event = response |> Event.AppendEntriesResponse in
      let from = State.self state in
      let () = send_event { event; from } peer in
      let state = State.reset_election_timer state in
      Ok state
  | Leader _ ->
      print_endline "Leader received append entries";
      Ok current_state
  | Candidate _ ->
      print_endline "Candidate received append entries";
      convert_to_follower current_state |> Ok

let handle_request_vote_response peer current_state response =
  print_endline "Received request vote response";
  let open Or_error.Let_syntax in
  let update_term current_state response =
    match Request_response.term response > State.current_term current_state with
    | true ->
        { current_state with current_term = Request_response.term response }
    | false -> current_state
  in
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

let handle_append_entries_response _peer current_state response =
  print_endline "Received append entries response";
  let update_term current_state response =
    match Append_response.term response > State.current_term current_state with
    | true ->
        { current_state with current_term = Append_response.term response }
        |> convert_to_follower
    | false -> current_state
  in
  update_term current_state response |> Ok
