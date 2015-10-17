open Lsm
open Message

let () =
  let lsm = new lsm
                (new mem_manifest_store
                     { next_counter = 0L;
                       sstables = []; })
                (new no_wal)
                (fun () -> new map_memtable)
  in
  lsm # apply [ Single ("key", new set "value"); ];
  assert (lsm # get "key" = Some "value");
  lsm # apply [ Single ("key", new delete); ];
  assert (lsm # get "key" = None);
  print_endline "cucu"