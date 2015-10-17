open Lsm_prelude
open Lsm
open Lsm_message


let test_memtable () =
  let memtable = new map_memtable 5 in
  memtable # force_apply
           [| Lsm_batch.Single ("key", new set "test_memtable") |];
  match memtable # get "key" with
  | None -> assert false
  | Some v -> Printf.printf
                "found %s\n"
                (v # merge_to_value
                   ~get_next:(fun () -> None)
                 |> Option.get_some)

let make_lsm ?(wal : #Lsm_wal.wal = new Lsm_wal.no_wal) () =
  Lsm.make
    (new mem_manifest_store
         { next_counter = 0L;
           sstables = []; })
    wal
    (fun batch ->
     let r = new map_memtable 10 in
     r # force_apply batch;
     r)

let test_lsm () =
  let lsm = make_lsm () in

  let keys =
    Int.map
      0 100
      (fun i -> Printf.sprintf "key_%i" i)
  in
  List.iter
    (fun key -> Lsm.set lsm key key)
    keys;
  List.iter
    (fun key ->
     Printf.printf "%s\n" key;
     match Lsm.get lsm key with
     | None -> assert false
     | Some v -> assert (v = key))
    keys;
  List.iter
    (fun key -> Lsm.delete lsm key)
    keys;
  List.iter
    (fun key -> assert (None = Lsm.get lsm key))
    keys

let test_merge_messages () =
  let lsm = make_lsm () in
  (* push increment messages down the tree
   * query after each step to see if the result
   * is still as expected!
   *)
  let k = "key" in
  let push_message () =
    Lsm.apply
      lsm
      [| Lsm_batch.Single (k,
                           new int64_addition 1L) |]
  in
  let get_int64 () =
    match Lsm.get lsm k with
    | None -> 0L
    | Some v -> Marshal.from_bytes v 0
  in
  Int.iter
    0 100
    (fun i ->
     let vi = get_int64 () in
     Printf.printf "i = %i, vi = %Li\n" i vi;
     assert (Int64.of_int i = vi);
     push_message ())

let test_recover_from_wal () =
  let wal = new Lsm_wal.memory_wal in
  let lsm1 = make_lsm ~wal:(wal :> Lsm_wal.wal) () in
  Lsm.set lsm1 "1" "";
  Lsm.set lsm1 "2" "";
  Lsm.delete lsm1 "1";

  let assert_lsm lsm =
    assert (None = Lsm.get lsm "1");
    assert (Some "" = Lsm.get lsm "2")
  in
  assert_lsm lsm1;

  Printf.printf "%s\n" ([%show : (int64 * batch) list]
                          (wal # get_items));

  (* make new lsm based on the same WAL
   * it should have the same contents (after replay)
   *)
  let lsm2 = make_lsm ~wal:(wal :> Lsm_wal.wal) () in

  assert_lsm lsm1;
  assert_lsm lsm2

let () =
  test_memtable ();
  test_lsm ();
  test_merge_messages ();
  test_recover_from_wal ()
