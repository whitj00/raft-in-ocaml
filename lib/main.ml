open Core
open Async
open Protocol

let eval_rpc =
  Rpc.Rpc.create ~name:"eval" ~version:0 ~bin_query:Event.bin_t
    ~bin_response:Unit.bin_t

let start_server writer port =
  let implementation =
    Rpc.Rpc.implement eval_rpc (fun peer event ->
        let%bind () = Pipe.write writer (peer, event) in
        Deferred.unit)
  in
  Tcp.Server.create (Tcp.Where_to_listen.of_port port) ~on_handler_error:`Ignore
    (fun address reader writer ->
      Rpc.Connection.server_with_close reader writer
        ~implementations:
          (Rpc.Implementations.create_exn ~on_unknown_rpc:`Raise
             ~implementations:[ implementation ])
        ~connection_state:(fun _ ->
          let address = Socket.Address.Inet.to_string address in
          Peer.create ~address)
        ~on_handshake_error:`Ignore)

let rec read_from_pipe pipe_reader =
  let%bind event = Pipe.read pipe_reader in
  match event with
  | `Eof -> read_from_pipe pipe_reader
  | `Ok event -> Deferred.return event

let get_election_timeout v =
  let election_timer = Follower_volatile_state.election_timeout v in
  let time_since_last_election =
    Time.diff (Follower_volatile_state.last_election v) (Time.now ())
  in
  Time.Span.(election_timer - time_since_last_election)

let get_heartbeat_timeout v =
  let heartbeat_timer = State.heartbeat_timeout v in
  let time_since_last_heartbeat =
    Time.diff (Time.now ()) (State.last_hearbeat v)
  in
  Time.Span.(heartbeat_timer - time_since_last_heartbeat)

let get_next_event pipe_reader state =
  let peer_type = State.peer_type state in
  let election_timeout_deferred =
    match peer_type with
    | Follower volatile ->
        let election_timeout =
          get_election_timeout volatile |> after >>| fun () ->
          (Peer.empty, Event.ElectionTimeout)
        in
        election_timeout
    | _ -> Deferred.never ()
  in
  let heartbeat_timeout_deferred =
    get_heartbeat_timeout state |> after >>| fun () ->
    (Peer.empty, Event.HeartbeatTimeout)
  in
  let incoming_events_deferred = read_from_pipe pipe_reader in
  Deferred.choose
    [
      Deferred.choice election_timeout_deferred Fn.id;
      Deferred.choice heartbeat_timeout_deferred Fn.id;
      Deferred.choice incoming_events_deferred Fn.id;
    ]

let handle_event peer state event =
  match (event : Event.t) with
  | RequestVoteResponse response ->
      handle_request_vote_response peer state response
  | AppendEntriesResponse response -> Ok (update_term state response)
  | AppendEntriesCall call -> handle_append_entries peer state call
  | RequestVoteCall call -> handle_request_vote peer state call
  | Event.ElectionTimeout -> handle_election_timeout state
  | HeartbeatTimeout -> handle_heartbeat_timeout state

let rec event_loop event_reader state =
  let%bind peer, event = get_next_event event_reader state in
  let state = handle_event peer state event in
  match state with
  | Ok state -> event_loop event_reader state
  | Error error ->
      print_endline (Error.to_string_hum error);
      Deferred.unit

let main () =
  let server_state = State.create ~peers:[] in
  let event_pipe = Pipe.create () in
  let event_reader, event_writer = event_pipe in
  let%bind _server = start_server event_writer 8080 in
  let%bind () = event_loop event_reader server_state in
  Deferred.unit
