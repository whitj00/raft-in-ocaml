open Core
module Rpc = Raft_rpc

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
let set_heartbeat_timer t time = { t with last_hearbeat = time }
let get_election_timeout () = Time.Span.of_sec (Random.float_range 1.5 6.)

let reset_election_timer t =
  {
    t with
    election_timeout = get_election_timeout ();
    last_election = Time.now ();
  }

let create ~peers ~port =
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

let convert_to_follower t =
  match peer_type t with
  | Follower _ -> t
  | _ ->
      let term = current_term t in
      let voted_for = None in
      let volatile_state = Follower.State.init in
      let peer_type = Peer_type.Follower volatile_state in
      print_endline "----------------------------------";
      printf "%d: Converting to follower\n" term;
      print_endline "----------------------------------";
      { t with voted_for; peer_type } |> reset_election_timer

let convert_to_leader t =
  let current_term = current_term t in
  print_endline "-----------------";
  printf "%d: Converting to leader\n" current_term;
  print_endline "-----------------";
  let voted_for = None in
  let peers = peers t in
  let volatile_state = Leader.State.init peers in
  let peer_type = Peer_type.Leader volatile_state in
  { t with voted_for; peer_type; current_term }

let convert_if_votes t =
  match peer_type t with
  | Follower _ | Leader _ -> t
  | Candidate candidate_state -> (
      let votes = Candidate.State.votes candidate_state in
      let peers = peers t in
      let majority = ((List.length peers + 1) / 2) + 1 in
      match List.length votes >= majority with
      | true -> convert_to_leader t
      | false -> t)

let convert_to_candidate t =
  let current_term = current_term t + 1 in
  print_endline "-----------------";
  printf "%d: Converting to candidate\n" current_term;
  print_endline "-----------------";
  let voted_for = Some (self t) in
  let volatile_state = Candidate.State.init () in
  let peer_type = Peer_type.Candidate volatile_state in
  let new_state = { t with current_term; voted_for; peer_type } in
  let peers = peers new_state in
  let () =
    let term = current_term in
    let last_log_index = List.length (log new_state) - 1 in
    let last_log_term =
      match List.last (log new_state) with
      | Some v -> Log_entry.term v
      | None -> 0
    in
    let event =
      Rpc.Request_call.create ~term ~last_log_index ~last_log_term
      |> Rpc.Event.RequestVoteCall
    in
    let from = self t |> Peer.to_host_and_port in
    let request = Rpc.Remote_call.create ~event ~from in
    printf "%d: Requesting votes from peers\n" term;
    List.iter peers ~f:(Rpc.send_event request)
  in
  (* let new_state = convert_if_votes new_state in *)
  let new_state = reset_election_timer new_state in
  new_state
