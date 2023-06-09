open Core
open Async

module Call : sig
  type t

  val handle :
    Peer.t ->
    State.t ->
    Server_rpc.Append_call.t ->
    (State.t, Error.t) result Deferred.t
end

module Response : sig
  val handle :
    Host_and_port.t ->
    State.t ->
    Server_rpc.Append_response.t ->
    (State.t, Error.t) result Deferred.t
end

module Heartbeat : sig
  val send : State.t -> (State.t, Error.t) Result.t Deferred.t
end
