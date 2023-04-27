open! Core

type t =
  | Follower of Follower.State.t
  | Candidate of Candidate.State.t
  | Leader of Leader.State.t
