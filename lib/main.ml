open Core
open Async

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
  | Leader _ -> Append_entries.Heartbeat.send state
  | Follower _ | Candidate _ -> Ok state |> return

let handle_election_timeout state =
  printf "%d: Election timeout\n" (State.current_term state);
  let%bind state =
    match State.peer_type state with
    | Follower _ | Candidate _ -> State.convert_to_candidate state
    | Leader _ -> return state
  in
  Ok state |> return

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
  if Pipe.length pipe_reader > 0 then
    let%bind response = read_from_pipe pipe_reader in
    Deferred.return response
  else if
    (not uses_heartbeat) && Time.Span.(election_timeout < Time.Span.of_sec 0.)
  then
    let event = Server_rpc.Event.ElectionTimeout in
    let from = State.self state |> Peer.to_host_and_port in
    Deferred.return { Server_rpc.Remote_call.event; from }
  else if uses_heartbeat && Time.Span.(heartbeat_timeout < Time.Span.of_sec 0.)
  then
    let event = Server_rpc.Event.HeartbeatTimeout in
    let from = State.self state |> Peer.to_host_and_port in
    Deferred.return { Server_rpc.Remote_call.event; from }
  else get_next_event pipe_reader state

let handle_event host_and_port state event =
  let state = State.convert_if_votes state |> State.update_peer_list in
  let%bind peer =
    let peers = State.peers state in
    let peer_opt =
      List.find peers ~f:(fun peer ->
          Host_and_port.equal (Peer.to_host_and_port peer) host_and_port)
    in
    let peer =
      match peer_opt with
      | None -> Peer.create ~host_and_port
      | Some peer -> peer
    in
    let conn = Peer.conn peer |> Option.value_exn in
    (* Connect to the peer *)
    let%bind _ =
      Persistent_connection.Rpc.connected_or_failed_to_connect conn
    in
    return peer
  in

  match (event : Server_rpc.Event.t) with
  | ElectionTimeout -> handle_election_timeout state
  | HeartbeatTimeout -> handle_heartbeat_timeout state
  | AppendEntriesCall call -> Append_entries.Call.handle peer state call
  | AppendEntriesResponse response ->
      Append_entries.Response.handle host_and_port state response
  | RequestVoteCall call -> Request_vote.handle_request_vote peer state call
  | RequestVoteResponse response ->
      Request_vote.handle_request_vote_response peer state response |> return
  | CommandCall command -> State.handle_command_call state command

let rec event_loop (event_reader : Server_rpc.Remote_call.t Pipe.Reader.t) state
    =
  let%bind { Server_rpc.Remote_call.from = peer; event } =
    get_next_event event_reader state
  in
  let%bind state = handle_event peer state event in
  match state with
  | Ok state -> event_loop event_reader state
  | Error error ->
      print_endline (Error.to_string_hum error);
      Deferred.unit

let main ~port ~host ~(bootstrap : Host_and_port.t option) () =
  let create_host ~hostname ~port =
    let host_and_port = Host_and_port.create ~host:hostname ~port in
    Peer.create ~host_and_port
  in
  let local = create_host ~hostname:host ~port in
  let%bind peers =
    match bootstrap with
    | Some bootstrap ->
        let event =
          Command_log.Command.AddServer (local |> Peer.to_host_and_port)
          |> Server_rpc.Event.CommandCall
        in
        let remote_call =
          {
            Server_rpc.Remote_call.event;
            from = local |> Peer.to_host_and_port;
          }
        in
        let bootstrap_peer = Peer.create ~host_and_port:bootstrap in
        let%bind () =
          Server_rpc.send_event_blocking remote_call bootstrap_peer
        in
        return [ local; bootstrap_peer ]
    | None -> return [ local ]
  in
  let server_state =
    match bootstrap with
    | None ->
        let state = State.create ~peers ~port |> State.convert_to_leader in
        let log =
          Command_log.append_one
            (Command_log.Entry.create ~term:0
               ~command:
                 (Command_log.Command.AddServer (local |> Peer.to_host_and_port)))
            state.log
        in
        { state with log }
    | Some _ ->
        State.create ~peers ~port |> State.convert_to_follower ~leader:None
  in
  let event_pipe = Pipe.create () in
  let (event_reader, event_writer)
        : Server_rpc.Remote_call.t Pipe.Reader.t
          * Server_rpc.Remote_call.t Pipe.Writer.t =
    event_pipe
  in
  let%bind _server_rpc = Server_rpc.start_server event_writer port in
  let self = State.self server_state |> Peer.to_host_and_port in
  let%bind _client_rpc =
    Client_rpc.start_server self event_writer (port + 1000)
  in
  let%bind () = event_loop event_reader server_state in
  return ()
