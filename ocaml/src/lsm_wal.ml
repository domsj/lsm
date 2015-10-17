type batch = Lsm_batch.t

class type wal =
  object
    method append : int64 -> batch -> unit
    method iterate_from : int64 -> (int64 -> batch -> unit) -> unit
    method discard_until : int64 -> unit
  end

class no_wal =
object(self : #wal)
  method append cnt batch = ()
  method iterate_from cnt f = ()
  method discard_until cnt = ()
end

class memory_wal =
object(self : #wal)
  val mutable items = []
  method get_items = items

  method append cnt batch =
    (* TODO verify cnt is exactly 1 bigger than previous *)
    items <- (cnt, batch) :: items

  method iterate_from cnt f =
    let rec inner = function
      | [] -> ()
      | (cnt', batch) :: tl ->
         let () =
           match compare cnt' cnt with
           | 0 -> ()
           | 1 -> inner tl
           | -1 -> failwith "asked to replay from future"
           | _ -> assert false
         in
         f cnt' batch
    in
    inner items

  method discard_until cnt = () (* TODO *)
end
