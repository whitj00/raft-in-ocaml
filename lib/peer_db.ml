open Core
open Async

type 'a t = (Host_and_port.t * 'a) list

let init peers value =
  List.map ~f:Peer.to_host_and_port peers
  |> List.map ~f:(fun peer -> (peer, value))

let add_peer (t : 'a t) peer value = (peer, value) :: t

let update_value t peer index =
  List.map t ~f:(fun (p, i) ->
      if Host_and_port.equal p peer then (p, index) else (p, i))

let get_value t peer =
  let peer = Peer.to_host_and_port peer in
  List.find_map t ~f:(fun (p, i) ->
      if Host_and_port.equal p peer then Some i else None)

let get_value_exn t peer =
  match get_value t peer with
  | Some i -> i
  | None -> failwith "get_value_exn: peer not found"

let majority_have_at_least_n t n =
  let total = List.length t in
  let have = List.count t ~f:(fun (_, i) -> i >= n) in
  printf "have: %d, total: %d\n" have total ;
  have >= (total / 2) + 1