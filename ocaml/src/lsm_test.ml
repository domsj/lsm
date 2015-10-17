open Lsm
open Message

module Option = struct
    let get_some = function
      | None -> failwith "None while (Some _) was expected"
      | Some v -> v
  end

module Int = struct
    let rec iter min max f =
      if min < max
      then
        begin
          f min;
          iter (min + 1) max f
        end

    let map min max f =
      let res = ref [] in
      iter
        min max
        (fun i -> res := (f i) :: !res);
      List.rev !res
  end

let test_memtable () =
  let memtable = new map_memtable 5 in
  memtable # force_apply [ Single ("key", new set "test_memtable") ];
  match memtable # get "key" with
  | None -> assert false
  | Some v -> Printf.printf
                "found %s\n"
                (v # merge_to_value
                   ~get_next:(fun () -> None)
                 |> Option.get_some)

let test_lsm () =
  let lsm = new lsm
                (new mem_manifest_store
                     { next_counter = 0L;
                       sstables = []; })
                (new no_wal)
                (fun () -> new map_memtable 5)
  in

  let keys =
    Int.map
      0 20
      (fun i -> Printf.sprintf "key_%i" i)
  in
  List.iter
    (fun key -> lsm # set key key)
    keys;
  List.iter
    (fun key ->
     Printf.printf "%s\n" key;
     match lsm # get key with
     | None -> assert false
     | Some v -> assert (v = key))
    keys;
  List.iter
    (fun key -> lsm # delete key)
    keys;
  List.iter
    (fun key -> assert (None = lsm # get key))
    keys

let () =
  test_memtable ();
  test_lsm ()
