type key = bytes
type message = Lsm_message.t
type value = Lsm_message.value

type range = key * key option (* [ first_inclusive, last_exclusive [ *)

type operation =
  | Single of key * message
  (* | TODO Range of range * message *)


(* TODO
 * - what are semantics related to ordering inside the batch?
 *)
type batch = operation list

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


(* type direction = *)
(*   | LE (\* less than or equal *\) *)
(*   | GE (\* greater than or equal *\) *)

(* class type iterator = *)
(*   object *)
(*     method is_valid : bool *)

(*     method jump : key -> direction -> bool *)
(*     method jump_to_last : bool *)

(*     method next : bool *)
(*     method prev : bool *)

(*     method get_key : key option *)
(*     method get_value : raw_value option *)
(*   end *)

class type queryable =
  object
    (* method maybe_exists : key -> bool *)
    method get : key -> message option
    (* TODO method get_iterator : iterator *)
  end

exception Immutable_memtable

class type memtable =
  object
    (* force apply batch even if it's too big
     * note: will only be called on a fresh memtable
     *)
    method force_apply : batch -> unit

    (* apply batch if it fits in,
     * returns whether it was applied or not
     *)
    method maybe_apply : batch -> bool

    inherit queryable

    (* turns the memtable into an immutable one *)
    method freeze : unit
  end


module StringMap = Map.Make(String)

class map_memtable max_size =
  (object(self)
      val mutable size = 0
      val mutable frozen = false
      val mutable map = StringMap.empty

      method freeze = frozen <- true

      method force_apply batch =
        if frozen
        then raise Immutable_memtable;

        List.iter
          (function
            | Single (key, message) ->
               let map' = match self # get key with
               | None -> StringMap.add key message map
               | Some m ->
                  begin
                    match m # merge_to_message [ message ] false with
                    | None, _ -> StringMap.remove key map
                    | Some m, _ -> StringMap.add key m map
                  end
               in
               map <- map')
          batch;
        size <- size + List.length batch

      method maybe_apply batch =
        if size + (List.length batch) > max_size
        then false
        else
          begin
            self # force_apply batch;
            true
          end

      (* method maybe_exists key = *)
      (*   StringMap.mem key map *)

      method get key =
        try Some (StringMap.find key map)
        with Not_found -> None

    end : memtable)

class type sstable =
  object
    inherit queryable

    method key_range : key * key
  end

(* all sstables in a level should be not overlapping
 * sstables are sorted in key-range order
 *)
type sstable_level = sstable array

let get_sstable_from_level level key =
  (* TODO could use binary search *)
  let rec inner i =
    if i = Array.length level
    then None
    else
      begin
        let sstable = level.(i) in
        let min, max = sstable # key_range in
        if key > max
        then inner (i + 1)
        else
          if key < min
          then None
          else Some sstable
      end
  in
  inner 0


(* most recent level is at the head of the list
 * oldest level is at the tail of the list
 * new levels are added to the front
 *)
type sstable_tree = sstable_level list

let get_next_message_from_sstable_tree levels key =
  let levels = ref levels in
  let rec inner () = match !levels with
    | [] -> None
    | level :: tl ->
       levels := tl;
       begin
         match get_sstable_from_level level key with
         | None ->
            inner ()
         | Some sstable ->
            sstable # get key
       end
  in
  inner

(* TODO
want cached (precomputed) view based on sstable_tree
- per key range have the list of relevant sstables
=> used for point queries and range queries
=> used by the scheduler to see which key ranges
   are most in need of compaction
   (to keep cost of range queries down)
   (compaction to keep cost of point queries
    low should be decided based upon
    hyperloglog info of overlapping levels?)
 *)


type manifest = {
    next_counter : counter;
    sstables : sstable_tree;
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
    method apply : batch -> unit

    method set : key -> value -> unit
    method delete : key -> unit

    method get : key -> value option

    (* TODO allow making snapshots etc *)
  end

class lsm
        (manifest_store : manifest_store)
        (wal : wal)
        (make_memtable : unit -> memtable)
  =
  (object(self)
      val mutable active_memtable = make_memtable ()
      val mutable frozen_memtables = []

      (* TODO background threads
       * - to schedule compaction
       *   (update manifest + discard stale sstables)
       * - flush frozen memtables to sstables
       *   (free memtable + update manifest)
       *)

      method apply batch =
        if not (active_memtable # maybe_apply batch)
        then
          begin
            active_memtable # freeze;
            frozen_memtables <- active_memtable :: frozen_memtables;
            active_memtable <- make_memtable ();
            active_memtable # force_apply batch
          end

      method set key value =
        self # apply
             [ let open Lsm_message in
               Single (key, new set value); ]

      method delete key =
        self # apply
             [ let open Lsm_message in
               Single (key, new delete); ]

      method get key : value option =
        let get_next_message_from_sstable_tree =
          get_next_message_from_sstable_tree
            (manifest_store # get).sstables
            key
        in
        let get_next_memtable =
          let memtables =
            ref (active_memtable :: frozen_memtables) in
          fun () ->
          match !memtables with
          | [] -> None
          | t :: ts ->
             memtables := ts;
             Some t
        in
        let rec get_next_message_from_memtable () =
          match get_next_memtable () with
          | None -> None
          | Some memtable ->
             begin
               match memtable # get key with
               | None -> get_next_message_from_memtable ()
               | (Some _) as mo -> mo
             end
        in
        let get_next () =
          match get_next_message_from_memtable () with
          | None -> get_next_message_from_sstable_tree ()
          | (Some _) as mo -> mo
        in
        match get_next () with
        | None ->
           None
        | Some m ->
           m # merge_to_value ~get_next

    end : lsm_type)
