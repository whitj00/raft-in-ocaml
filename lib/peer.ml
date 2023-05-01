open Core
open Async

module T = struct
  type t = {
    host_and_port : Host_and_port.t;
    conn : Persistent_connection.Rpc.t option;
  }
  [@@deriving fields]

  let compare = Comparable.lift Host_and_port.compare ~f:host_and_port

  let create ~host_and_port =
    let conn =
      Some
        (Persistent_connection.Rpc.create' ~server_name:"RPC server" (fun () ->
             return (Ok host_and_port)))
    in
    Fields.create ~host_and_port ~conn

  let sexp_of_t t = Host_and_port.sexp_of_t t.host_and_port
  let t_of_sexp sexp = create ~host_and_port:(Host_and_port.t_of_sexp sexp)
end

include T
include Comparable.Make (T)

let host t = t.host_and_port.host
let port t = t.host_and_port.port
let to_host_and_port t = t.host_and_port
let equal t1 t2 = Host_and_port.equal t1.host_and_port t2.host_and_port
let to_string t = t.host_and_port |> Host_and_port.to_string
