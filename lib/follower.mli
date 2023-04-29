open Core

module State : sig
  type t

  val init : Host_and_port.t option -> t
  val following : t -> Host_and_port.t option
end
