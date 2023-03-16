open! Core
open Async

let () =
  Command.async ~summary:"Start a raft server"
    (let%map_open.Command port =
       flag "-port" (required int) ~doc:" Port to listen on"
     and peer_port =
       flag "-peer-port" (required int) ~doc:" Port to connect to"
     in
     Raft.Main.main port peer_port)
  |> Command_unix.run
