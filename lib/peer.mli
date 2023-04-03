type t

val conn : t -> Persistent_connection.Rpc.t option
val port : t -> int
val host : t -> string
val create : host:string -> port:int -> t
val to_host_and_port : t -> Core.Host_and_port.t
val self : t
val equal : t -> t -> bool
val to_string : t -> string
