open! Core

module State = struct
  type t = { following : Host_and_port.t option } [@@deriving fields]

  let init following = { following }
end
