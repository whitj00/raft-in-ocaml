open! Core
open Async

let () =
  Command.async ~summary:"Start a raft server"
    (let%map_open.Command port =
       flag "-port" (required int) ~doc:" Port to listen on"
     and peer_port_1 = flag "-pp1" (required int) ~doc:" Port to connect to"
     and peer_port_2 = flag "-pp2" (required int) ~doc:" Port to connect to"
     and peer_port_3 = flag "-pp3" (required int) ~doc:" Port to connect to" in
     Raft.Main.main ~port ~peer_port_1 ~peer_port_2 ~peer_port_3)
  |> Command_unix.run
