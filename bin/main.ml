open! Core
open Async

let node =
  Command.async ~summary:"Start a raft server"
    (let%map_open.Command port =
       flag "-port" (required int) ~doc:" Port to listen on"
     and peer_port_1 = flag "-pp1" (required int) ~doc:" Port to connect to"
     and peer_port_2 = flag "-pp2" (required int) ~doc:" Port to connect to"
     and peer_port_3 = flag "-pp3" (required int) ~doc:" Port to connect to" in
     Raft.Main.main ~port ~peer_port_1 ~peer_port_2 ~peer_port_3)

let increment =
  Command.async ~summary:"Send a message to a raft server"
    (let%map_open.Command host_and_port =
       flag "-node" (required host_and_port) ~doc:" host to connect to"
     in
     fun () ->
       Raft.Client_rpc.send_command Raft.Command_log.Command.Increment
         host_and_port)

let decrement =
  Command.async ~summary:"Send a message to a raft server"
    (let%map_open.Command host_and_port =
       flag "-node" (required host_and_port) ~doc:" host to connect to"
     in
     fun () ->
       Raft.Client_rpc.send_command Raft.Command_log.Command.Decrement
         host_and_port)

let client =
  Command.group ~summary:"Client commands"
    [ ("increment", increment); ("decrement", decrement) ]

let () =
  Command_unix.run
    (Command.group ~summary:"Raft commands"
       [ ("node", node); ("client", client) ])
