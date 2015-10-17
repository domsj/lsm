type 'a list' = 'a list [@@deriving show]
type 'a list = 'a list' [@@deriving show]

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
