type key = bytes

(* raw_value may contain
 * an actual value, a delete marker
 * or a merge message.
 * so it's presence doesn't mean there's an
 * actual value as far as the user is concerned
 *)
type raw_value = bytes

type range = key * key (* [ first_inclusive, last_exclusive [ *)

(* TODO
 * - range operators
 *)
type operation =
  | Single of key * raw_value
  (* | DeleteRange of range *)


(* TODO
 * - what are semantics related to ordering inside the batch?
 *)
type batch = operation list


class type writeable =
  object
    method apply : batch -> unit
  end

type counter = int64

class type wal =
  object
    method append : counter -> batch -> unit
    method iterate_from : counter -> (counter -> batch -> unit) -> unit
    method discard_until : counter -> unit
  end

class no_wal =
  (object
      method append cnt batch = ()
      method iterate_from cnt f = ()
      method discard_until cnt = ()
    end : wal)


type direction =
  | LE (* less than or equal *)
  | GE (* greater than or equal *)

class type iterator =
  object
    method is_valid : bool

    method jump : key -> direction -> bool
    method jump_to_last : bool

    method next : bool
    method prev : bool

    method get_key : key option
    method get_value : raw_value option
  end


class type queryable =
  object
    (* method maybe_exists : key -> bool *)
    method get : key -> raw_value option
    (* TODO method get_iterator : iterator *)
  end


exception Immutable_memtable

class type memtable =
  object
    (* TODO
     * - allow requesting size so that caller can choose to write
     *   to this memtable or freeze it and start a new one
     * - allow flushing to a sstable
     *)

    inherit writeable
    inherit queryable

    (* turns the memtable into an immutable one *)
    method freeze : unit
  end


module StringMap = Map.Make(String)

class map_memtable =
  (object
      val mutable frozen = false
      method freeze = frozen <- true

      val mutable map = StringMap.empty

      method apply batch =
        if frozen
        then raise Immutable_memtable;
        List.iter
          (function
            | Single (key, raw_value) ->
               map <- StringMap.add key raw_value map
          )
          batch

      (* method maybe_exists key = *)
      (*   StringMap.mem key map *)

      method get key =
        try Some (StringMap.find key map)
        with Not_found -> None

    end : memtable)

class type sstable =
  object
    inherit queryable
  end

type 'a partitioned = 'a * (key * 'a) list
type sstable_node =
  | Empty
  | Node of sstable * sstable_node partitioned

type manifest = {
    next_counter : counter;
    sstable_dag : sstable_node;
  }

(* manifest + wal = persisted lsm state *)

class type manifest_store =
  object
    method get : manifest
    method set : manifest -> unit
  end

class mem_manifest_store current_manifest =
  (object
      val mutable current_manifest = current_manifest
      method get = current_manifest
      method set mf = current_manifest <- mf
    end : manifest_store)


class type lsm_type =
  object
    inherit writeable
    inherit queryable

    (* TODO allow making snapshots etc *)
  end

class lsm
        (manifest_store : manifest_store)
        (wal : wal)
        (make_memtable : unit -> memtable)
  =
  (object
      val mutable active_memtable = make_memtable ()

      (* TODO background threads
       * - to schedule compaction
       *   (update manifest + discard stale sstables)
       * - flush frozen memtables to sstables
       *   (free memtable + update manifest)
       *)

      method apply batch =
        (* TODO check size of memtable,
         * maybe freeze current and start new one *)
        active_memtable # apply batch

      method get key =
        match active_memtable # get key with
        | (Some _) as vo -> vo
        | None ->
           let rec walk_sstable = function
             | Empty -> None
             | Node (sstable, sharded_sstables) ->
                begin
                  match sstable # get key with
                  | (Some _) as vo -> vo
                  | None ->
                     let rec inner = function
                       | (prev, []) -> prev
                       | (prev, ((key', sstable') :: tl)) ->
                          if key < key'
                          then prev
                          else inner (sstable', tl)
                     in
                     walk_sstable (inner sharded_sstables)
                end
           in
           walk_sstable (manifest_store # get).sstable_dag

    end : lsm_type)

(* TODO add tests *)
let () =
  let lsm = new lsm
                (new mem_manifest_store
                     { next_counter = 0L;
                       sstable_dag = Empty; })
                (new no_wal)
                (fun () -> new map_memtable)
  in
  lsm # apply [ Single ("key", "value"); ];
  assert (lsm # get "key" = Some "value");
  print_endline "cucu"
