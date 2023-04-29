open! Core

module State = struct
  type t = { following : Host_and_port.t option } [@@deriving fields]

  let init following = { following }
  let is_following t hpo = Option.equal Host_and_port.equal hpo t.following
end
