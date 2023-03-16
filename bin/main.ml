open! Core
open Async

let () =
  Command.async ~summary:"Start a raft server"
    (let%map_open.Command () = return () in
     Raft.Main.main)
  |> Command_unix.run
