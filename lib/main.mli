open! Core
open Async

val main :
  port:int -> peer_port_1:int -> peer_port_2:int -> unit -> unit Deferred.t