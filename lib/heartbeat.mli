open! Core
open Async

val reset_timer : State.t -> State.t
val send : State.t -> (State.t, Error.t) Result.t Deferred.t
