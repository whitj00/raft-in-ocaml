open Core

type 'a t = (Peer.t * 'a) list

let init peers = List.map peers ~f:(fun peer -> (peer, 0))

let update_value t peer index =
  List.map t ~f:(fun (p, i) -> if Peer.equal p peer then (p, index) else (p, i))
