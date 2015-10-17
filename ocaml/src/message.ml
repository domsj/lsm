type value = bytes

type side_effect = bytes
type is_final = bool

class type t =
  object
    method merge_to_value : get_next : (unit -> t option) -> value option
    method merge_to_message : t list -> is_final -> t option * side_effect option
  end

let messages : (int, bytes -> int -> t) Hashtbl.t = Hashtbl.create 3

let register_message_type
      code
      from_bytes =
  if Hashtbl.mem messages code
  then failwith "bad code";
  Hashtbl.add messages code from_bytes

class delete =
  (object(self)
      method merge_to_value ~get_next = None
      method merge_to_message raws is_final =
        (if is_final
         then None
         else Some (self :> t)),
        None
    end : t)

class set value =
  (object(self)
      method merge_to_value ~get_next = Some value
      method merge_to_message raws is_final = Some (self :> t), None
    end : t)

class multi_message (messages : t list) =
  let msg_o, msgs = match messages with
    | [] -> None, ref []
    | hd :: tl -> Some hd, ref tl
  in
  (object
      method merge_to_value ~get_next =
        let get_next' () =
          match !msgs with
          | [] -> get_next ()
          | op :: tl ->
             msgs := tl;
             Some op
        in
        match (match msg_o with
               | None -> get_next ()
               | Some m -> Some m) with
        | None -> None
        | Some m -> m # merge_to_value get_next'

      method merge_to_message raws is_final =
        Some (new multi_message (List.append messages raws)), None
    end : t)

class int64_addition delta =
  (object(self)
      method merge_to_value ~get_next =
        let cur = match get_next () with
          | None -> 0L
          | Some op ->
             begin
               match op # merge_to_value ~get_next with
               | None -> 0L
               | Some v -> Marshal.from_bytes v 0
             end
        in
        Some (Marshal.to_bytes Int64.(add cur delta) [])

      method merge_to_message raws is_final =
        Some (new multi_message ((self :> t) :: raws)), None
    end : t)

let () =
  register_message_type
    1 (Marshal.from_bytes : bytes -> int -> delete);
  register_message_type
    2 (Marshal.from_bytes : bytes -> int -> set);
  register_message_type
    3 (Marshal.from_bytes : bytes -> int -> multi_message);
  register_message_type
    4 (Marshal.from_bytes : bytes -> int -> int64_addition)
