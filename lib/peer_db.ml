open Core

type 'a t = (Peer.t * 'a) list

let init peers value = List.map peers ~f:(fun peer -> (peer, value))

let update_value t peer index =
  List.map t ~f:(fun (p, i) -> if Peer.equal p peer then (p, index) else (p, i))
