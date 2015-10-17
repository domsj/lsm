type key = Lsm_batch.key
type message = Lsm_message.t
type value = Lsm_message.value
type batch = Lsm_batch.t [@@deriving show]

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

        Array.iter
          (function
            | Lsm_batch.Single (key, message) ->
               let map' = match self # get key with
                 | None ->
                    StringMap.add key message map
                 | Some m ->
                    begin
                      (* TODO proper is_final *)
                      let is_final = false in
                      match message # merge_to_message
                                    [ m ]
                                    is_final with
                      | None, _ ->
                         StringMap.remove key map
                      | Some m_res, _ ->
                         StringMap.add key m_res map
                    end
               in
               map <- map')
          batch;
        (* TODO use a better metric for size *)
        size <- size + Array.length batch

      method maybe_apply batch =
        if size + (Array.length batch) > max_size
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
    next_counter : int64;
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

module Lsm =
  struct
    type t = {
        manifest_store : manifest_store;
        wal : Lsm_wal.wal;
        make_memtable : (batch -> memtable);
        mutable next_counter : int64;
        mutable active_memtable : memtable;
        mutable frozen_memtables : memtable list;
      }

    let apply_no_wal t batch =
      if not (t.active_memtable # maybe_apply batch)
      then
        begin
          t.active_memtable # freeze;
          t.frozen_memtables <-
            t.active_memtable :: t.frozen_memtables;
          t.active_memtable <- t.make_memtable batch
        end

    let make manifest_store wal make_memtable =
      let manifest = manifest_store # get in
      let t =
        { manifest_store;
          wal;
          make_memtable;
          next_counter = manifest.next_counter;
          active_memtable = make_memtable [||];
          frozen_memtables = []; }
      in
      wal # iterate_from manifest.next_counter
          (fun cnt batch ->
           assert (cnt = t.next_counter);
           apply_no_wal t batch;
           t.next_counter <- Int64.succ cnt);
      t

      (* TODO background threads
       * - to schedule compaction
       *   (update manifest + discard stale sstables)
       * - flush frozen memtables to sstables
       *   (free memtable + update manifest)
       *)

    let apply t batch =
      (* TODO do this while holding a (lwt)lock *)
      t.wal # append t.next_counter batch;
      t.next_counter <- Int64.succ t.next_counter;
      apply_no_wal t batch

    let set t key value =
      apply t
            [| let open Lsm_message in
               let open Lsm_batch in
               Single (key, new set value); |]

    let delete t key =
      apply t
            [| let open Lsm_message in
               let open Lsm_batch in
               Single (key, new delete); |]

    let get t key : value option =
      let get_next_message_from_sstable_tree =
        get_next_message_from_sstable_tree
          (t.manifest_store # get).sstables
          key
      in
      let get_next_memtable =
        let memtables =
          ref (t.active_memtable :: t.frozen_memtables) in
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
  end
