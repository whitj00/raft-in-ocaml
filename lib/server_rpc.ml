open Core
open Async

module Append_call = struct
  type t = {
    term : int; (* Leader's term *)
    prev_log_index : int;
    prev_log_term : int;
    entries : Command_log.t;
    leader_commit : int;
  }
  [@@deriving fields, bin_io, sexp]

  let create = Fields.create
end

module Append_response = struct
  type t = { term : int; success : bool; matchIndex : int }
  [@@deriving fields, bin_io, sexp]

  let create = Fields.create
end

module Request_call = struct
  type t = { term : int; last_log_index : int; last_log_term : int }
  [@@deriving fields, bin_io, sexp]

  let create = Fields.create
end

module Request_response = struct
  type t = { term : int; success : bool } [@@deriving fields, bin_io, sexp]

  let create = Fields.create
end

module Add_server_call = struct
  type t = { term : int; host : string; port : int }
  [@@deriving fields, bin_io, sexp]

  let create = Fields.create
end

module Event = struct
  type t =
    | AddServerCall of Add_server_call.t
    | RequestVoteCall of Request_call.t
    | RequestVoteResponse of Request_response.t
    | AppendEntriesCall of Append_call.t
    | AppendEntriesResponse of Append_response.t
    | ElectionTimeout
    | HeartbeatTimeout
    | CommandCall of Command_log.Command.t
  [@@deriving bin_io, sexp]

  let to_string t = Sexp.to_string (sexp_of_t t)
end

module Remote_call = struct
  type t = { event : Event.t; from : Host_and_port.t }
  [@@deriving bin_io, sexp, fields]

  let to_string t = Sexp.to_string (sexp_of_t t)
  let create = Fields.create
end

let server_rpc =
  Rpc.One_way.create ~name:"raft" ~version:0 ~bin_msg:Remote_call.bin_t

let send_event event peer =
  let host = Peer.host peer in
  let port = Peer.port peer in
  let conn =
    match Peer.conn peer with
    | Some conn -> conn
    | None -> failwith "conn not set"
  in
  (* Use async_rpc to send event *)
  let dispatch connection =
    let response = Rpc.One_way.dispatch server_rpc connection event in
    match response with
    | Error _ ->
        printf "rpcerr: message failed to send to peer %s:%d\n" host port
        |> return
    | Ok () -> return ()
  in
  let conn_res = Persistent_connection.Rpc.current_connection conn in
  match conn_res with
  | None ->
      printf "connerr: connection failed to peer %s:%d\n" host port |> return
  | Some connection -> dispatch connection

let start_server writer port =
  let implementation =
    Rpc.One_way.implement server_rpc (fun _peer event ->
        ignore (Pipe.write writer event))
  in
  Tcp.Server.create (Tcp.Where_to_listen.of_port port) ~on_handler_error:`Ignore
    (fun remote reader writer ->
      Rpc.Connection.server_with_close reader writer
        ~implementations:
          (Rpc.Implementations.create_exn ~on_unknown_rpc:`Raise
             ~implementations:[ implementation ])
        ~connection_state:(fun _ ->
          let host_and_port = Socket.Address.Inet.to_host_and_port remote in
          let host = Host_and_port.host host_and_port in
          let port = Host_and_port.port host_and_port in
          Peer.create ~host ~port)
        ~on_handshake_error:`Ignore)
