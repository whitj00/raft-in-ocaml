open Core
open Async

type t = {
  host : string;
  port : int;
  conn : Persistent_connection.Rpc.t option;
}
[@@deriving fields]

let create ~host ~port =
  let conn =
    Some
      (Persistent_connection.Rpc.create' ~server_name:"RPC server" (fun () ->
           return (Ok { Host_and_port.host; port })))
  in
  Fields.create ~host ~port ~conn

let to_host_and_port t = Host_and_port.create ~host:t.host ~port:t.port
let self = { host = "127.0.0.1"; port = 8080; conn = None }
let equal t1 t2 = String.equal t1.host t2.host && t1.port = t2.port
let to_string t = sprintf "%s:%d" t.host t.port
