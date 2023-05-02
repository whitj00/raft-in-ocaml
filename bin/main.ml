open! Core
open Async

let node =
  Command.async ~summary:"Start a raft server"
    (let%map_open.Command port =
       flag "-port" (required int) ~doc:" Port to listen on"
     and bootstrap =
       flag "-bootstrap-server" (optional host_and_port)
         ~doc:" Port to connect to"
     and host = flag "-host" (required string) ~doc:"your host" in
     Raft.Main.main ~port ~bootstrap ~host)

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

let add_server =
  Command.async ~summary:"Send a message to a raft server"
    (let%map_open.Command host_and_port =
       flag "-node" (required host_and_port) ~doc:" host to connect to"
     and new_server =
       flag "-new-server" (required host_and_port) ~doc:" host to connect to"
     in
     fun () ->
       Raft.Client_rpc.send_command
         (Raft.Command_log.Command.AddServer new_server) host_and_port)

let client =
  Command.group ~summary:"Client commands"
    [
      ("increment", increment);
      ("decrement", decrement);
      ("add-server", add_server);
    ]

let () =
  Command_unix.run
    (Command.group ~summary:"Raft commands"
       [ ("node", node); ("client", client) ])
