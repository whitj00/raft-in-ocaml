open Core
open Async
module Rpc = Raft_rpc

type t = {
  current_term : int;
  voted_for : Peer.t option;
  log : Log_entry.t list;
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

let create_heartbeat_timer () = Time.Span.of_sec 0.1
let set_heartbeat_timer t time = { t with last_hearbeat = time }
let get_election_timeout () = Time.Span.of_sec (Random.float_range 1.5 3.0)

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
    log = [];
    commit_index = 0;
    last_applied = 0;
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
  let () =
    let term = current_term in
    let last_log_index = List.length (log t) - 1 in
    let last_log_term =
      match List.last (log t) with Some v -> Log_entry.term v | None -> 0
    in
    let event =
      Rpc.Request_call.create ~term ~last_log_index ~last_log_term
      |> Rpc.Event.RequestVoteCall
    in
    let from = self t |> Peer.to_host_and_port in
    let request = Rpc.Remote_call.create ~event ~from in
    printf "%d: Requesting votes from peers\n" term;
    remote_nodes t |> List.iter ~f:(Rpc.send_event request)
  in
  (* let new_state = convert_if_votes new_state in *)
  reset_election_timer t

let update_term_and_convert_if_outdated state term =
  let current_term = current_term state in
  match term > current_term with
  | false -> state
  | true -> set_term ~term state |> convert_to_follower
