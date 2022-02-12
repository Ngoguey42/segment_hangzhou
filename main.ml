(** Hello

    Requires https://github.com/Ngoguey42/irmin/pull/new/expose-compress *)

(*
   - genre: (blob-{0-31,32-127,128,511,512+}|inode-{root,inner}-{tree,val})

   collected infos:
   - "per commit tree" x "per pack-file-area" x "per path prefix" x "per genre"
     - # of entries
     - # of bytes used
   - "per commit tree" x "per pack-file-area" x "per path prefix" x ()
     - # of bytes used by hard-coded steps
     - # of hard-coded steps
     - # of dict steps
   - "per commit tree" x "per pack-file-area" x ()                x ()
     - # of pages touched
     - # of chunks (contiguous groups)
   - () x                "per pack-file-area" x ()                x "per genre + commit"
     - # of total entries (needed for leftover calculation)
     - # of total bytes used (needed for leftover calculation)

   missing infos:
   - which area references which area? (i.e. analysis of pq when changing area)
   - intersection between trees
   - breakdown of everything in a pack file

   to show:
   - (leftward horizontal histogram?) averaged on all ref commits
     - at which cycle-distance are all genre (entry weighted / bytes weighted)
     - at which cycle-distance are all paths (entry weighted / bytes weighted)
   - (camembert) averaged on all ref commits + (curves) evolution for all ref commits
     - at which cycle-distance are the entries (entry weighted / bytes weighted)
     - number in each genre  (entry weighted / bytes weighted)
     - number in each directory (entry weighted / bytes weighted)
     - which path grows the most (entry weighted / bytes weighted)

    morallement, quelles info je veux:
    -
    -
    -
    -
    -
    -

*)
open Import
open Main_tools

module Intset = Set.Make (struct
  type t = int

  let compare = compare
end)

module Int63map = Map.Make (Int63)

type kind =
  [ `Contents_0_31
  | `Contents_128_511
  | `Contents_32_127
  | `Contents_512_plus
  | `Inode_nonroot_tree
  | `Inode_nonroot_values
  | `Inode_root_tree
  | `Inode_root_values ]
[@@deriving repr ~pp]

let multiple = [ "multiple" ]

let pp_path_prefix_rev ppf l =
  if l === multiple then Format.fprintf ppf "%s" "multiple"
  else Format.fprintf ppf "/%s" (l |> List.rev |> String.concat "/")

module D0 = struct
  type k = {
    parent_cycle_start : int;
    entry_area : int;
    path_prefix_rev : string list;
    kind : kind;
  }
  [@@deriving repr ~pp]

  type v = { count : int; bytes : int } [@@deriving repr ~pp]

  let save path tbl =
    let chan = open_out path in
    output_string chan
      "parent_cycle_start,entry_area,path_prefix,kind,count,bytes\n";
    Hashtbl.iter
      (fun k v ->
        Fmt.str "%d,%d,%a,%a,%d,%d\n" k.parent_cycle_start k.entry_area
          pp_path_prefix_rev k.path_prefix_rev pp_kind k.kind v.count v.bytes
        |> output_string chan)
      tbl;
    close_out chan
end

type acc = {
  dict : Dict.t;
  entry_count : int;
  area_of_offset : int63 -> int;
  d0 : (D0.k, D0.v) Hashtbl.t;
  path_sharing : (string list, string list) Hashtbl.t;
}

type p = { truncated_path_rev : string list; ancestor_cycle_starts : Intset.t }

let blob_encoding_prefix_size = 32 + 1

let kind_of_entry (entry : _ Traverse.entry) =
  match entry.v with
  | `Contents ->
      let len = entry.length - blob_encoding_prefix_size in
      assert (len >= 0);
      if len <= 31 then `Contents_0_31
      else if len <= 127 then `Contents_32_127
      else if len <= 511 then `Contents_128_511
      else `Contents_512_plus
  | `Inode t -> (
      match t.tv with
      | Traverse.Inode.Compress.V0_stable _ | V0_unstable _ -> assert false
      | V1_root { v = Values _; _ } -> `Inode_root_values
      | V1_root { v = Tree _; _ } -> `Inode_root_tree
      | V1_nonroot { v = Values _; _ } -> `Inode_nonroot_values
      | V1_nonroot { v = Tree _; _ } -> `Inode_nonroot_tree)

let on_entry acc (entry : _ Traverse.entry) =
  let kind = kind_of_entry entry in
  let area = acc.area_of_offset entry.offset in
  let ancestor_cycle_starts = entry.payload.ancestor_cycle_starts in
  let prefix = entry.payload.truncated_path_rev in
  Intset.iter
    (fun parent_cycle_start ->
      let open D0 in
      let entry_area = area in
      let path_prefix_rev = prefix in
      let kind = kind in
      let k0 = { parent_cycle_start; entry_area; path_prefix_rev; kind } in
      match Hashtbl.find_opt acc.d0 k0 with
      | None -> Hashtbl.add acc.d0 k0 { count = 1; bytes = entry.length }
      | Some { count; bytes } ->
          Hashtbl.replace acc.d0 k0
            { count = count + 1; bytes = bytes + entry.length })
    entry.payload.ancestor_cycle_starts;

  if acc.entry_count mod 50_000 = 0 then
    Fmt.epr "on_entry: %#d, area:%d, kind:%a, prefix:%a\n%!" acc.entry_count
      area pp_kind kind pp_path_prefix_rev prefix;

  (* if not @@ Hashtbl.mem lol prefix then (
   *   Fmt.epr "/%s \n%!" (prefix |> List.rev |> String.concat "/");
   *   Hashtbl.add lol prefix ()); *)
  let extend_prefix = List.length prefix <= 1 && prefix <<>> multiple in
  let preds =
    match entry.v with
    | `Contents -> []
    | `Inode t ->
        preds_of_inode acc.dict t
        |> List.map (fun (step_opt, off) ->
               let truncated_path_rev =
                 if not extend_prefix then prefix
                 else
                   match step_opt with
                   | None -> prefix
                   | Some step -> (
                       let path = step :: prefix in
                       match Hashtbl.find_opt acc.path_sharing path with
                       | Some path -> path
                       | None ->
                           Hashtbl.add acc.path_sharing path path;
                           path)
               in

               let payload = { truncated_path_rev; ancestor_cycle_starts } in
               (off, payload))
  in

  ({ acc with entry_count = acc.entry_count + 1 }, preds)

let on_chunk acc (chunk : Revbuffer.chunk) =
  ignore chunk;
  acc

let on_read acc ({ first; last } : IO.page_range) =
  ignore (first, last);
  acc

let merge_payloads _off
    ~older:{ truncated_path_rev = l; ancestor_cycle_starts = s }
    ~newer:{ truncated_path_rev = l'; ancestor_cycle_starts = s' } =
  let truncated_path_rev =
    if l == l' then l else if l === l' then l else multiple
    (* Fmt.failwith "Which path to choose? /%s /%s" ( l |> List.rev |> String.concat "/") *)
    (* ( l' |> List.rev |> String.concat "/") *)
  in
  (* TODO: truncated_path_rev     *)
  { truncated_path_rev; ancestor_cycle_starts = Intset.union s s' }

let main () =
  Fmt.epr "Hello World\n%!";

  let conf = Irmin_pack.config ~fresh:false ~readonly:true path in
  let* repo = Store.Repo.v conf in
  let dict = Dict.v ~fresh:false ~readonly:true path in
  let* cycle_starts = lookup_cycle_starts_in_repo repo in
  Fmt.epr "pack-store contains %d cycle starts\n%!" (List.length cycle_starts);

  let max_offsets =
    List.map (fun ((c : Cycle_start.t), off) -> (off, c.cycle_idx)) cycle_starts
  in

  let area_boundaries = max_offsets |> List.to_seq |> Int63map.of_seq in
  let area_of_offset off =
    let _, closest_cycle_start_on_the_right =
      Int63map.find_first (fun off' -> Int63.(off' >= off)) area_boundaries
    in
    closest_cycle_start_on_the_right - 1
  in
  let d0 = Hashtbl.create 1_000_000 in
  let path_sharing = Hashtbl.create 100 in
  let acc0 = { dict; entry_count = 0; area_of_offset; d0; path_sharing } in

  let max_offsets =
    List.map
      (fun (off, cycle_idx) ->
        assert (area_of_offset off = cycle_idx - 1);
        ( off,
          {
            truncated_path_rev = [];
            ancestor_cycle_starts =
              [ cycle_idx ] |> List.to_seq |> Intset.of_seq;
          } ))
      max_offsets
  in

  let acc =
    Traverse.fold path max_offsets ~merge_payloads ~on_entry ~on_chunk ~on_read
      acc0
  in

  let () =
    let open D0 in
    Fmt.epr "d0 len: %d\n%!" (Hashtbl.length acc.d0);
    let l =
      Hashtbl.to_seq acc.d0
      |> List.of_seq
      |> List.sort (fun (_, { bytes; _ }) (_, { bytes = bytes'; _ }) ->
             compare bytes' bytes)
    in
    List.iteri
      (fun i (k, v) -> if i < 15 then Fmt.epr "%a %a\n%!" pp_k k pp_v v)
      l
  in
  D0.save "d0.csv" acc.d0;
  Fmt.epr "Bye World\n%!";
  Lwt.return_unit

let () = Lwt_main.run (main ())
