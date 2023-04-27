open Core
open Async

module State = struct
  type t = { votes : Peer.t List.t } [@@deriving fields]

  let init self : t = { votes = [ self ] }

  (* Adds a peer if not already in the list *)
  let add_vote t peer term =
    match List.exists t.votes ~f:(fun p -> Peer.equal p peer) with
    | true -> t
    | false ->
        printf "%d: adding vote from %s\n" term (Peer.to_string peer);
        { votes = peer :: t.votes }

  let count_votes t : int = List.length t.votes
end
