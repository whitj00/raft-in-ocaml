open Core
open Async

module Append_call = struct
  type t = {
    term : int;
    prev_log_index : int;
    prev_log_term : int;
    entries : Log_entry.t list;
    leader_commit : int;
  }
  [@@deriving fields, bin_io, sexp]

  let create = Fields.create
end

module Request_call = struct
  type t = { term : int; last_log_index : int; last_log_term : int }
  [@@deriving fields, bin_io, sexp]

  let create = Fields.create
end

module Append_response = struct
  type t = { term : int; success : bool } [@@deriving fields, bin_io, sexp]

  let create = Fields.create
end

module Request_response = struct
  type t = { term : int; success : bool } [@@deriving fields, bin_io, sexp]

  let create = Fields.create
end

module Event = struct
  type t =
    | RequestVoteCall of Request_call.t
    | RequestVoteResponse of Request_response.t
    | AppendEntriesCall of Append_call.t
    | AppendEntriesResponse of Append_response.t
    | ElectionTimeout
    | HeartbeatTimeout
  [@@deriving bin_io, sexp]

  let to_string t = Sexp.to_string (sexp_of_t t)
end

module Remote_call = struct
  type t = { event : Event.t; from : Host_and_port.t }
  [@@deriving bin_io, sexp, fields]

  let to_string t = Sexp.to_string (sexp_of_t t)
  let create = Fields.create
end

let raft_rpc =
  Rpc.Rpc.create ~name:"raft" ~version:0 ~bin_query:Remote_call.bin_t
    ~bin_response:Unit.bin_t

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
    let%bind response = Rpc.Rpc.dispatch raft_rpc connection event in
    match response with
    | Error _ ->
        printf "rpcerr: message failed to send to peer %s:%d\n" host port
        |> return
    | Ok () -> return ()
  in
  don't_wait_for
    (let%bind conn_res =
       Persistent_connection.Rpc.connected_or_failed_to_connect conn
     in
     match conn_res with
     | Error _ ->
         printf "connerr: connection failed to peer %s:%d\n" host port |> return
     | Ok connection -> dispatch connection)

let start_server port =
  Tcp.Server.create (Tcp.Where_to_listen.of_port port) ~on_handler_error:`Ignore
    (fun remote reader writer ->
      let implementation =
        Rpc.Rpc.implement raft_rpc (fun _ remote_call ->
            let%bind () = Pipe.write writer remote_call in
            Deferred.unit)
      in    
      Rpc.Connection.server_with_close reader writer
        ~implementations:
          (Rpc.Implementations.create_exn ~on_unknown_rpc:`Raise
             ~implementations:[ implementation ])
        ~connection_state:(fun _ ->
          let host_and_port = Socket.Address.Inet.to_host_and_port remote in
          let host = Host_and_port.host host_and_port in
          let port = Host_and_port.port host_and_port in
          Peer.create ~host ~port)
        ~on_handshake_error:`Ignore
