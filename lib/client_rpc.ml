open Core
open Async

module Request = struct
  type t = Command_log.Command.t [@@deriving bin_io, sexp]
end

module Response = struct
  type t = { success : bool } [@@deriving bin_io, sexp, fields]
end

let client_rpc =
  Rpc.Rpc.create ~name:"client" ~version:0 ~bin_query:Request.bin_t
    ~bin_response:Response.bin_t

let send_command command hp =
  let host = Host_and_port.host hp in
  let port = Host_and_port.port hp in
  let%bind result =
    Rpc.Connection.with_client
      (Tcp.Where_to_connect.of_host_and_port { host; port })
      (fun conn -> Rpc.Rpc.dispatch_exn client_rpc conn command)
  in
  match result with
  | Ok _ ->
      print_endline "Success";
      return ()
  | Error e ->
      print_endline (Exn.to_string e);
      return ()

let start_server writer port =
  let implementation =
    Rpc.Rpc.implement client_rpc (fun peer query ->
        print_endline "Received command";
        let response = Response.Fields.create ~success:true in
        let from = Peer.to_host_and_port peer in
        let event = Server_rpc.Event.CommandCall query in
        let remote_call = Server_rpc.Remote_call.create ~event ~from in
        let%bind () = Pipe.write writer remote_call in
        return response)
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
