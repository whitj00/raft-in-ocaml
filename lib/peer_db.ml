open Core

type 'a t = (Peer.t * 'a) list

let init peers value = List.map peers ~f:(fun peer -> (peer, value))

let update_value t peer index =
  List.map t ~f:(fun (p, i) -> if Peer.equal p peer then (p, index) else (p, i))

let get_value t peer =
  List.find_map t ~f:(fun (p, i) -> if Peer.equal p peer then Some i else None)

let get_value_exn t peer =
  match get_value t peer with
  | Some i -> i
  | None -> failwith "get_value_exn: peer not found"
