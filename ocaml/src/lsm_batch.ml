type message = Lsm_message.t [@@deriving show]

type key = bytes [@@deriving show]

type operation =
  | Single of key * message
  (* | TODO Range of range * message *)
[@@deriving show]

type t = operation array [@@deriving show]
