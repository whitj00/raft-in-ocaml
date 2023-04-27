open Core
open Async
open Protocol
module Rpc = Raft_rpc

let rec read_from_pipe pipe_reader =
  let%bind response = Pipe.read pipe_reader in
  match response with
  | `Eof -> read_from_pipe pipe_reader
  | `Ok remote_call -> Deferred.return remote_call

let get_election_timeout state =
  let election_timer = State.election_timeout state in
  let time_since_last_election =
    Time.diff (Time.now ()) (State.last_election state)
  in
  Time.Span.(election_timer - time_since_last_election)

let handle_heartbeat_timeout state =
  match State.peer_type state with
  | Leader _ -> State.reset_heartbeat_timer state |> State.send_heartbeat |> Ok
  | Follower _ | Candidate _ -> Ok state

let handle_election_timeout state =
  printf "%d: Election timeout\n" (State.current_term state);
  match State.peer_type state with
  | Leader _ -> Ok state
  | Follower _ -> State.convert_to_candidate state |> Ok
  | Candidate _ -> State.convert_to_follower state |> Ok

let get_heartbeat_timeout state =
  let started_at = State.started_at state in
  let time_since_start = Time.diff (Time.now ()) started_at in
  if Time.Span.(time_since_start < of_sec 5.) then Time.Span.of_sec 10.
  else
    let now = Time.now () in
    let heartbeat_timer = State.heartbeat_timeout state in
    let last_heartbeat = State.last_hearbeat state in
    let time_since_last_heartbeat = Time.diff now last_heartbeat in
    Time.Span.(heartbeat_timer - time_since_last_heartbeat)

let rec get_next_event pipe_reader state =
  let%bind () = Clock.after (Time.Span.of_sec 0.5) in
  let election_timeout = get_election_timeout state in
  let heartbeat_timeout = get_heartbeat_timeout state in
  let uses_heartbeat =
    match State.peer_type state with
    | Leader _ -> true
    | Candidate _ -> false
    | Follower _ -> false
  in
  if (not uses_heartbeat) && Time.Span.(election_timeout < Time.Span.of_sec 0.)
  then
    let event = Rpc.Event.ElectionTimeout in
    let from = State.self state |> Peer.to_host_and_port in
    Deferred.return { Rpc.Remote_call.event; from }
  else if uses_heartbeat && Time.Span.(heartbeat_timeout < Time.Span.of_sec 0.)
  then
    let event = Rpc.Event.HeartbeatTimeout in
    let from = State.self state |> Peer.to_host_and_port in
    Deferred.return { Rpc.Remote_call.event; from }
  else if Pipe.length pipe_reader > 0 then
    let%bind response = read_from_pipe pipe_reader in
    Deferred.return response
  else get_next_event pipe_reader state

let handle_event host_and_port state event =
  let get_peer () =
    let peers = State.peers state in
    let peer_opt =
      List.find peers ~f:(fun peer ->
          Host_and_port.equal (Peer.to_host_and_port peer) host_and_port)
    in
    match peer_opt with None -> failwith "Peer not found" | Some peer -> peer
  in

  match (event : Rpc.Event.t) with
  | Rpc.Event.ElectionTimeout -> handle_election_timeout state
  | HeartbeatTimeout -> handle_heartbeat_timeout state
  | RequestVoteResponse response ->
      handle_request_vote_response (get_peer ()) state response
  | AppendEntriesResponse response ->
      handle_append_entries_response (get_peer ()) state response
  | AppendEntriesCall call -> Append_entries.handle_append_entries_call (get_peer ()) state call
  | RequestVoteCall call -> handle_request_vote (get_peer ()) state call

let rec event_loop (event_reader : Rpc.Remote_call.t Pipe.Reader.t) state =
  let%bind { Rpc.Remote_call.from = peer; event } =
    get_next_event event_reader state
  in
  let state = handle_event peer state event in
  match state with
  | Ok state -> event_loop event_reader state
  | Error error ->
      print_endline (Error.to_string_hum error);
      Deferred.unit

let main port peer_port_1 peer_port_2 () =
  let peers =
    [
      Peer.create ~host:"127.0.0.1" ~port:peer_port_1;
      Peer.create ~host:"127.0.0.1" ~port:peer_port_2;
    ]
  in
  let _peers = [] in
  let server_state = State.create ~peers ~port in
  let event_pipe = Pipe.create () in
  let (event_reader, event_writer)
        : Rpc.Remote_call.t Pipe.Reader.t * Rpc.Remote_call.t Pipe.Writer.t =
    event_pipe
  in
  let%bind _server = Rpc.start_server event_writer port in
  let%bind () = event_loop event_reader server_state in
  Deferred.unit
