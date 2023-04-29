open Core
open Async

type t = {
  current_term : int;
  voted_for : Peer.t option;
  log : Command_log.t;
  commit_index : int;
  last_applied : int;
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

let create_heartbeat_timer () = Time.Span.of_sec 2.
let set_heartbeat_timer t time = { t with last_hearbeat = time }
let reset_timer t = set_heartbeat_timer t (Time.now ())
let get_election_timeout () = Time.Span.of_sec (Random.float_range 10.0 20.0)

let remote_nodes t =
  let self = self t in
  let is_not_self peer = not (Peer.equal self peer) in
  List.filter ~f:is_not_self (peers t)

let reset_election_timer t =
  {
    t with
    election_timeout = get_election_timeout ();
    last_election = Time.now ();
  }

let set_term t ~term = { t with current_term = term }

let create ~peers ~port =
  {
    current_term = 0;
    voted_for = None;
    log = Command_log.init ();
    commit_index = 0;
    last_applied = 0;
    peers;
    peer_type = Follower.State.init None |> Peer_type.Follower;
    heartbeat_timeout = create_heartbeat_timer ();
    last_hearbeat = Time.now ();
    election_timeout = get_election_timeout ();
    last_election = Time.now ();
    started_at = Time.now ();
    self = Peer.create ~host:"127.0.0.1" ~port;
  }

let convert_to_follower t ~leader =
  let term = current_term t in
  let voted_for = None in
  let volatile_state = Follower.State.init leader in
  let peer_type = Peer_type.Follower volatile_state in
  let leader_str =
    Option.value_map ~default:"None" ~f:Host_and_port.to_string leader
  in
  print_endline "----------------------------------";
  printf "%d: Converting to follower (leader: %s)\n" term leader_str;
  print_endline "----------------------------------";
  { t with voted_for; peer_type } |> reset_election_timer

let convert_to_leader t =
  let current_term = current_term t in
  print_endline "----------------------------------";
  printf "%d: Converting to leader\n" current_term;
  print_endline "----------------------------------";
  let voted_for = None in
  let peers = peers t in
  let last_log_index = Command_log.last_index (log t) in
  let volatile_state = Leader.State.init ~peers ~last_log_index in
  let peer_type = Peer_type.Leader volatile_state in
  { t with voted_for; peer_type; current_term }

let convert_if_votes t =
  match peer_type t with
  | Follower _ | Leader _ -> t
  | Candidate candidate_state -> (
      let votes = Candidate.State.votes candidate_state in
      let peers = peers t in
      let majority = (List.length peers / 2) + 1 in
      match List.length votes >= majority with
      | true -> convert_to_leader t
      | false -> t)

let convert_to_candidate t =
  let current_term = current_term t + 1 in
  print_endline "-----------------";
  printf "%d: Converting to candidate\n" current_term;
  print_endline "-----------------";
  let voted_for = Some (self t) in
  let peer_type = Candidate.State.init (self t) |> Peer_type.Candidate in
  let t = { t with current_term; voted_for; peer_type } in
  let%bind () =
    let term = current_term in
    let last_log_index = Command_log.last_index (log t) in
    let last_log_term = Command_log.last_log_term (log t) in
    let event =
      Server_rpc.Request_call.create ~term ~last_log_index ~last_log_term
      |> Server_rpc.Event.RequestVoteCall
    in
    let from = self t |> Peer.to_host_and_port in
    let request = Server_rpc.Remote_call.create ~event ~from in
    printf "%d: Requesting votes from peers\n" term;
    remote_nodes t |> Deferred.List.iter ~f:(Server_rpc.send_event request)
  in
  (* let new_state = convert_if_votes new_state in *)
  reset_election_timer t |> return

let update_term_and_convert_if_outdated state term leader =
  let current_term = current_term state in
  match term > current_term with
  | false -> state
  | true -> set_term ~term state |> convert_to_follower ~leader

let get_leader t =
  match peer_type t with
  | Follower state -> Follower.State.following state
  | Leader _ -> self t |> Peer.to_host_and_port |> Some
  | Candidate _ -> None

let find_peer_opt t host_and_port =
  let peers = peers t in
  let is_peer peer =
    Host_and_port.equal (Peer.to_host_and_port peer) host_and_port
  in
  List.find ~f:is_peer peers

let find_peer_exn t host_and_port =
  match find_peer_opt t host_and_port with
  | None ->
      failwith
        (sprintf "find_peer_exn: Peer not found %s\n"
           (Host_and_port.to_string host_and_port))
  | Some peer -> peer

(* If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log
   entries starting at nextIndex *)
let update_peers state =
  match peer_type state with
  | Follower _ | Candidate _ -> return ()
  | Leader leader_state ->
      let command_log = log state in
      let term = current_term state in
      let last_log_index = Command_log.last_index command_log in
      let is_outdated peer =
        let next_index = Leader.State.get_next_index_exn leader_state peer in
        last_log_index >= next_index
      in
      let outdated_peers = List.filter ~f:is_outdated (remote_nodes state) in
      let get_log_entries_after_match_index peer =
        let next_index = Leader.State.get_next_index_exn leader_state peer in
        (peer, Command_log.entries_from command_log next_index, next_index)
      in
      let log_entries =
        List.map ~f:get_log_entries_after_match_index outdated_peers
      in
      Deferred.List.iter log_entries ~f:(fun (peer, entries, next_index) ->
          let prev_log_index = next_index - 1 in
          let prev_log_term =
            Command_log.get_term_exn command_log prev_log_index
          in
          let leader_commit = commit_index state in
          let event =
            Server_rpc.Append_call.create ~term ~prev_log_index ~prev_log_term
              ~entries ~leader_commit
            |> Server_rpc.Event.AppendEntriesCall
          in
          let from = self state |> Peer.to_host_and_port in
          let request = Server_rpc.Remote_call.create ~event ~from in
          printf "%d: Sending append entries to %s from %s\n" term
            (Peer.to_string peer)
            (Host_and_port.to_string from);
          Server_rpc.send_event request peer)

(*
 * If leader: Append entries to log, return Ok
 * If follower: Send command to leader, return Ok
 * If candidate: Return error
 *)
let handle_command_call (state : t) (command : Command_log.Command.t) =
  let term = current_term state in
  let leader = get_leader state in
  let state = update_term_and_convert_if_outdated state term leader in
  match peer_type state with
  | Candidate _ ->
      Error.of_string "Cannot handle command as candidate" |> Error |> return
  | Leader _ ->
      let log = log state in
      let entry = Command_log.Entry.create ~term ~command in
      let log = Command_log.append_one log entry in
      let state = { state with log } in
      let%bind () = update_peers state in
      Ok state |> return
  | Follower _ -> (
      match leader with
      | None -> Error.of_string "No leader" |> Error |> return
      | Some leader ->
          let event = Server_rpc.Event.CommandCall command in
          let from = self state |> Peer.to_host_and_port in
          let request = Server_rpc.Remote_call.create ~event ~from in
          let leader_peer = find_peer_exn state leader in
          let%bind () = Server_rpc.send_event request leader_peer in
          return (Ok state))
