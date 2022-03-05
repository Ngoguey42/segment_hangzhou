(** Hello

    Requires https://github.com/Ngoguey42/irmin/pull/new/expose-compress *)

(*
   D0:
   Info per pack file entry

   D1:
   Truc

   D2:
   Info per pack file area

   about to impl:
   - Split [Inode_root_tree] given length 33-256, 257-2048, 2049-16384, 16385+
   - Split [Inode_nonroot_tree] and [Inode_nonroot_values] given their
     root length: 33-256, 257-2048, 2049-16384, 16385+, multiple
   - Let's record more about paths:
     - The exact names up to len 2, and stars for higher lenghts
     - "/"
     - "/data/"
     - "/data/contracts"
     - "/data/contracts/*"
     - "/data/contracts/*/*"
     - "/data/contracts/*/*/*"
     - "/data/contracts/*/*/*/*"
     - "/data/contracts/*/*/*/*/*"

   missing infos (osef for now):
   - are the steps uniques?
     - Inside single tree
     - Cross tree
   - are the hashes uniques?
     - Inside single tree
     - Cross tree
   - average relative offset lenghts
     - Inside single tree
   - space taken by length field
   - which area references which area? (i.e. analysis of pq when changing area)
   - intersection between trees (put source trees in payload)
   - number of times an object is referenced
     - Inside single tree
     - Cross tree

  thoughts:
   - I learned that they want to freeze at each cycle. I don't think it
     changes much for the plots, the reader should just consider distance 0
     and distance 1+

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

type extended_kind =
  [ `Contents_0_31
  | `Contents_128_511
  | `Contents_32_127
  | `Contents_512_plus
  | `Inode_nonroot_tree
  | `Inode_nonroot_values
  | `Inode_root_tree
  | `Inode_root_values
  | `Commit ]
[@@deriving repr ~pp]

let dynamic_size_of_step_encoding =
  match Irmin.Type.Size.of_value Store.step_t with
  | Dynamic f -> f
  | Static _ | Unknown -> assert false

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

  type v = {
    count : int;
    bytes : int;
    indirect_count : int;
    direct_count : int;
    direct_bytes : int;
  }
  [@@deriving repr ~pp]

  let path = "./entries.csv"

  let save tbl =
    let chan = open_out path in
    output_string chan
      "parent_cycle_start,entry_area,path_prefix,kind,count,bytes,indirect_count,direct_count,direct_bytes\n";
    Hashtbl.iter
      (fun k v ->
        Fmt.str "%d,%d,%a,%a,%d,%d,%d,%d,%d\n" k.parent_cycle_start k.entry_area
          pp_path_prefix_rev k.path_prefix_rev pp_kind k.kind v.count v.bytes
          v.indirect_count v.direct_count v.direct_bytes
        |> output_string chan)
      tbl;
    close_out chan
end

module D1 = struct
  type k = { parent_cycle_start : int; entry_area : int }
  type v = { pages_touched : int; chunk_count : int; algo_chunk_count : int }

  let path = "./memory_layout.csv"

  let save tbl =
    let chan = open_out path in
    output_string chan
      "parent_cycle_start,entry_area,pages_touched,chunk_count,algo_chunk_count\n";
    Hashtbl.iter
      (fun k v ->
        Fmt.str "%d,%d,%d,%d,%d\n" k.parent_cycle_start k.entry_area
          v.pages_touched v.chunk_count v.algo_chunk_count
        |> output_string chan)
      tbl;
    close_out chan
end

module D2 = struct
  type k = { area : int; kind : extended_kind }
  type v = { entry_count : int; byte_count : int }

  let path = "./areas.csv"

  let save tbl =
    let chan = open_out path in
    output_string chan "area,kind,entry_count,byte_count\n";
    Hashtbl.iter
      (fun k v ->
        Fmt.str "%d,%a,%d,%d\n" k.area pp_extended_kind k.kind v.entry_count
          v.byte_count
        |> output_string chan)
      tbl;
    close_out chan
end

type acc = {
  dict : Dict.t;
  area_of_offset : int63 -> int;
  d0 : (D0.k, D0.v) Hashtbl.t;
  d1 : (D1.k, D1.v) Hashtbl.t;
  path_sharing : (string list, string list) Hashtbl.t;
  (* The last ones should be reset before each traversal *)
  entry_count : int;
  ancestor_cycle_start : int;
  leftmost_page_touched : int option;
  current_chunk : int63 option;
}

type payload = { truncated_path_rev : string list }

let blob_encoding_prefix_size = 32 + 1

let kind_of_contents entry_length =
  let len = entry_length - blob_encoding_prefix_size in
  assert (len >= 0);
  if len <= 31 then `Contents_0_31
  else if len <= 127 then `Contents_32_127
  else if len <= 511 then `Contents_128_511
  else `Contents_512_plus

(* quick and dirty *)
let kind_of_inode2 =
  let open Seq_traverse.Inode.Compress in
  fun t ->
    match t.tv with
    | V0_stable _ | V0_unstable _ -> assert false
    | V1_root { v = Values _; _ } -> `Inode_root_values
    | V1_root { v = Tree _; _ } -> `Inode_root_tree
    | V1_nonroot { v = Values _; _ } -> `Inode_nonroot_values
    | V1_nonroot { v = Tree _; _ } -> `Inode_nonroot_tree

let kind_of_inode =
  let open Traverse.Inode.Compress in
  fun t ->
    match t.tv with
    | V0_stable _ | V0_unstable _ -> assert false
    | V1_root { v = Values _; _ } -> `Inode_root_values
    | V1_root { v = Tree _; _ } -> `Inode_root_tree
    | V1_nonroot { v = Values _; _ } -> `Inode_nonroot_values
    | V1_nonroot { v = Tree _; _ } -> `Inode_nonroot_tree

let kind_of_entry (entry : _ Traverse.entry) =
  match entry.v with
  | `Contents -> kind_of_contents entry.length
  | `Inode t -> kind_of_inode t

let register_entry acc (entry : _ Traverse.entry) entry_area path_prefix_rev
    kind parent_cycle_start raw_preds =
  let open D0 in
  let k = { parent_cycle_start; entry_area; path_prefix_rev; kind } in
  let v =
    match Hashtbl.find_opt acc.d0 k with
    | None ->
        {
          count = 0;
          bytes = 0;
          indirect_count = 0;
          direct_count = 0;
          direct_bytes = 0;
        }
    | Some e -> e
  in
  let v = { v with count = v.count + 1; bytes = v.bytes + entry.length } in
  let v =
    List.fold_left
      (fun v -> function
        | Some (step, `Direct), _off ->
            let size = dynamic_size_of_step_encoding step in
            {
              v with
              direct_count = v.direct_count + 1;
              direct_bytes = v.direct_bytes + size;
            }
        | Some (_step, `Indirect), _off ->
            { v with indirect_count = v.indirect_count + 1 }
        | None, _ -> v)
      v raw_preds
  in
  Hashtbl.replace acc.d0 k v

let on_entry acc (entry : _ Traverse.entry) =
  let kind = kind_of_entry entry in
  let area = acc.area_of_offset entry.offset in
  let ancestor_cycle_start = acc.ancestor_cycle_start in
  assert (area < ancestor_cycle_start);
  let prefix = entry.payload.truncated_path_rev in
  let extend_prefix = List.length prefix <= 1 && prefix <<>> multiple in
  if acc.entry_count mod 2_000_000 = 0 then
    Fmt.epr "on_entry: %#d, area:%d, kind:%a, prefix:%a\n%!" acc.entry_count
      area pp_kind kind pp_path_prefix_rev prefix;

  let raw_preds =
    match entry.v with `Contents -> [] | `Inode t -> preds_of_inode acc.dict t
  in

  register_entry acc entry area prefix kind ancestor_cycle_start raw_preds;

  let preds =
    List.map
      (fun (step_opt, off) ->
        let truncated_path_rev =
          if not extend_prefix then prefix
          else
            match step_opt with
            | None -> prefix
            | Some (step, (`Direct | `Indirect)) -> (
                let path = step :: prefix in
                match Hashtbl.find_opt acc.path_sharing path with
                | Some path -> path
                | None ->
                    Hashtbl.add acc.path_sharing path path;
                    path)
        in
        let payload = { truncated_path_rev } in
        (off, payload))
      raw_preds
  in
  ({ acc with entry_count = acc.entry_count + 1 }, preds)

let on_chunk acc (chunk : Revbuffer.chunk) =
  let first = chunk.offset in
  let right = Int63.(add_distance first chunk.length) in
  let last = Int63.(right - one) in
  let entry_area = acc.area_of_offset first in
  assert (entry_area = acc.area_of_offset last);
  let parent_cycle_start = acc.ancestor_cycle_start in
  assert (entry_area < parent_cycle_start);
  let k = D1.{ parent_cycle_start; entry_area } in
  let v : D1.v =
    let open D1 in
    match Hashtbl.find_opt acc.d1 k with
    | None -> { pages_touched = 0; chunk_count = 0; algo_chunk_count = 0 }
    | Some prev -> prev
  in

  (* Fmt.epr "on_chunk: %a\n%!" Revbuffer.pp_chunk chunk;
   * Fmt.epr "pages: %d/%d (+%d)\n%!" first_page_touched last_page_touched
   *   incr_pages_touched; *)

  (* 1 - Increment pages_touched *)
  let first_page_touched = IO.page_idx_of_offset first in
  let last_page_touched = IO.page_idx_of_offset last in
  let incr_pages_touched =
    match acc.leftmost_page_touched with
    | None -> 1 + Int.distance_exn ~lo:first_page_touched ~hi:last_page_touched
    | Some leftmost_page_touched ->
        assert (first_page_touched <= leftmost_page_touched);
        assert (last_page_touched <= leftmost_page_touched);
        Int.distance_exn ~lo:first_page_touched ~hi:leftmost_page_touched
  in
  let v = { v with pages_touched = v.pages_touched + incr_pages_touched } in

  (* 2 - Increment algo_chunk_count *)
  let v = { v with algo_chunk_count = v.algo_chunk_count + 1 } in

  (* 3 - Increment chunk_count *)
  let v =
    match acc.current_chunk with
    | None ->
        assert (v.chunk_count = 0);
        { v with chunk_count = 1 }
    | Some prev_left ->
        if Int63.(right > prev_left) then assert false
        else if Int63.(right = prev_left) then v
        else { v with chunk_count = v.chunk_count + 1 }
  in

  Hashtbl.replace acc.d1 k v;
  {
    acc with
    leftmost_page_touched = Some first_page_touched;
    current_chunk = Some first;
  }

let on_read acc ({ first; last } : IO.page_range) =
  ignore (first, last);
  acc

let merge_payloads _off ~older:{ truncated_path_rev = l }
    ~newer:{ truncated_path_rev = l' } =
  let truncated_path_rev =
    if l == l' then l else if l === l' then l else multiple
  in
  { truncated_path_rev }

let main () =
  Fmt.epr "Hello World\n%!";

  let conf = Irmin_pack.config ~fresh:false ~readonly:true path in
  let* repo = Store.Repo.v conf in
  let dict = Dict.v ~fresh:false ~readonly:true path in
  let* cycle_starts = lookup_cycle_starts_in_repo repo in
  Fmt.epr "pack-store contains %d cycle starts\n%!" (List.length cycle_starts);

  let area_boundaries =
    cycle_starts
    |> List.map (fun ((c : Cycle_start.t), _node_off, commit_off) ->
           (commit_off, c.cycle_idx))
    |> List.to_seq
    |> Int63map.of_seq
  in
  let area_of_offset off =
    (* Fmt.epr "area_of_offset: %#14d\n%!" Int63.(to_int off); *)
    let _, closest_cycle_start_on_the_right =
      Int63map.find_first (fun off' -> Int63.(off' > off)) area_boundaries
    in
    closest_cycle_start_on_the_right - 1
  in

  Fmt.epr "\n%!";
  Fmt.epr "\n%!";
  Fmt.epr "First, traverse sequentially the file\n%!";
  let () =
    let d2 = Hashtbl.create 1_000 in
    let _, _, offset_to_stop_at = cycle_starts |> List.rev |> List.hd in
    Seq_traverse.fold path () ~on_entry:(fun () off length entry ->
        let left = off in
        let right = Int63.add_distance off length in
        assert (Int63.(right <= offset_to_stop_at));
        let area = area_of_offset left in
        assert (area = area_of_offset Int63.(right - one));
        let open D2 in
        let kind =
          match entry with
          | `Commit -> `Commit
          | `Contents -> kind_of_contents length
          | `Inode t -> kind_of_inode2 t
        in
        let k = { area; kind } in
        let v =
          match Hashtbl.find_opt d2 k with
          | None -> { entry_count = 1; byte_count = length }
          | Some prev ->
              {
                entry_count = prev.entry_count + 1;
                byte_count = prev.byte_count + length;
              }
        in
        Hashtbl.replace d2 k v;
        (* Fmt.epr "on_entry %#14d, %d, %a, area:%d\n%!" (Int63.to_int off) length
         *   (Repr.pp Irmin_pack.Pack_value.Kind.t)
         *   kind area; *)
        ((), Int63.(right < offset_to_stop_at)));
    D2.save d2
  in

  let d0 = Hashtbl.create 1_000 in
  let d1 = Hashtbl.create 1_000 in
  let path_sharing = Hashtbl.create 100 in
  let acc =
    {
      dict;
      area_of_offset;
      d0;
      d1;
      path_sharing;
      entry_count = 0;
      ancestor_cycle_start = 0;
      leftmost_page_touched = None;
      current_chunk = None;
    }
  in
  let acc =
    List.fold_left
      (fun acc ((c : Cycle_start.t), node_off, _commit_off) ->
        Fmt.epr "\n%!";
        Fmt.epr "\n%!";
        assert (area_of_offset node_off = c.cycle_idx - 1);
        let acc =
          {
            acc with
            ancestor_cycle_start = c.cycle_idx;
            entry_count = 0;
            leftmost_page_touched = None;
            current_chunk = None;
          }
        in
        let payload = { truncated_path_rev = [] } in
        let acc =
          Traverse.fold path
            [ (node_off, payload) ]
            ~merge_payloads ~on_entry ~on_chunk ~on_read acc
        in

        (* if true then failwith "super"; *)
        acc)
      acc cycle_starts
  in

  D0.save acc.d0;
  D1.save acc.d1;
  Fmt.epr "Bye World\n%!";
  Lwt.return_unit

let () = Lwt_main.run (main ())
