open Core
open Async
open Protocol

let start_server (writer : Remote_call.t Pipe.Writer.t) port =
  let implementation =
    Rpc.Rpc.implement raft_rpc (fun _ remote_call ->
        let%bind () = Pipe.write writer remote_call in
        Deferred.unit)
  in
  Tcp.Server.create (Tcp.Where_to_listen.of_port port) ~on_handler_error:`Ignore
    (fun remote reader writer ->
      Rpc.Connection.server_with_close reader writer
        ~implementations:
          (Rpc.Implementations.create_exn ~on_unknown_rpc:`Raise
             ~implementations:[ implementation ])
        ~connection_state:(fun _ ->
          let host_and_port = Socket.Address.Inet.to_host_and_port remote in
          let host = Host_and_port.host host_and_port in
          let port = Host_and_port.port host_and_port in
          Peer.create ~host ~port)
        ~on_handshake_error:`Ignore)

let rec read_from_pipe pipe_reader =
  let%bind response = Pipe.read pipe_reader in
  match response with
  | `Eof ->
      read_from_pipe pipe_reader
  | `Ok { Remote_call.event; from } ->
      Deferred.return { Remote_call.event; from }

let get_election_timeout state =
  let election_timer = State.election_timeout state in
  let time_since_last_election =
    Time.diff (State.last_election state) (Time.now ())
  in
  Time.Span.(election_timer - time_since_last_election)

let get_heartbeat_timeout state =
  let started_at = State.started_at state in
  let time_since_start = Time.diff (Time.now ()) started_at in
  if Time.Span.(time_since_start < of_sec 5.) then
    (Time.Span.of_sec 10., started_at, Time.Span.of_sec 10.)
  else
    let now = Time.now () in
    let heartbeat_timer = State.heartbeat_timeout state in
    let last_heartbeat = State.last_hearbeat state in
    let time_since_last_heartbeat = Time.diff now (State.last_hearbeat state) in
    ( Time.Span.(heartbeat_timer - time_since_last_heartbeat),
      last_heartbeat,
      heartbeat_timer )

let get_next_event pipe_reader state =
  let peer_type = State.peer_type state in
  let election_timeout_deferred () =
    match peer_type with
    | Follower _ | Candidate _ ->
        print_endline "Election timeout";
        get_election_timeout state |> after >>| fun () ->
        let event = Event.ElectionTimeout in
        let from = State.self state in
        { Remote_call.event; from }
    | _ -> Deferred.never ()
  in
  let _heartbeat_timeout_deferred () =
    let timeout, start, span = get_heartbeat_timeout state in
    let%map () = after timeout in
    printf "Heartbeat timeout: %s\n" (Time.Span.to_string span);
    let event = Event.HeartbeatTimeout (start, span) in
    let from = State.self state in
    { Remote_call.event; from }
  in
  let incoming_events_deferred () =
    let%bind response = read_from_pipe pipe_reader in
    print_endline "Recieved remote_call from pipe";
    print_endline (Remote_call.to_string response);
    Deferred.return response
  in
  let%bind choice =
    Deferred.choose
      [
        Deferred.choice (incoming_events_deferred ()) Fn.id;
        Deferred.choice (election_timeout_deferred ()) Fn.id;
       (* Deferred.choice (heartbeat_timeout_deferred ()) Fn.id; *)
      ]
  in
  print_endline "Chose event";
  print_endline (Remote_call.to_string choice);
  Deferred.return choice

let handle_event peer state event =
  print_endline "Handling event";
  print_endline (Event.to_string event);
  match (event : Event.t) with
  | RequestVoteResponse response ->
      handle_request_vote_response peer state response
  | AppendEntriesResponse response ->
      handle_append_entries_response peer state response
  | AppendEntriesCall call -> handle_append_entries peer state call
  | RequestVoteCall call -> handle_request_vote peer state call
  | Event.ElectionTimeout -> handle_election_timeout state
  | HeartbeatTimeout (start, span) -> handle_heartbeat_timeout state start span

let rec event_loop event_reader state =
  let%bind { Remote_call.from = peer; event } =
    get_next_event event_reader state
  in
  let state = handle_event peer state event in
  match state with
  | Ok state -> event_loop event_reader state
  | Error error ->
      print_endline (Error.to_string_hum error);
      Deferred.unit

let main port peer_port () =
  let peers = [ Peer.create ~host:"127.0.0.1" ~port:peer_port ] in
  let _peers = [] in
  let server_state = State.create ~peers port in
  let event_pipe = Pipe.create () in
  let event_reader, event_writer = event_pipe in
  let%bind _server = start_server event_writer port in
  let%bind () = event_loop event_reader server_state in
  Deferred.unit
