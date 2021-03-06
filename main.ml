(** Hello

    Requires https://github.com/Ngoguey42/irmin/pull/new/expose-compress *)

(*
   D0:
   Info per pack file entry

   D1:
   Truc

   D2:
   Info per pack file area

   missing infos (osef for now):
   - The number of chunks and pages touched when the distance is <1 (bas si je sais) (bah non je sais pas) (bah si) (non, definitivement)
   - are the steps uniques?
     - Inside single tree
     - Cross tree
   - are the hashes uniques?
     - Inside single tree
     - Cross tree
   - average relative offset lengths
     - Inside single tree
   - space taken by length field
   - which area references which area? (i.e. analysis of pq when changing area)
   - intersection between trees (I know that!)
   - number of times an object is referenced
     - Inside single tree
     - Cross tree
   - stats on what's not kept within an area, would need to perform a single
     traversal of all commits.
   - what's in an area that is not kept by the end of the cycle
   - Distribution of length in direct steps
   - in full file, infos on steps
   - size of the unshared tree. i.e. if the DAG is made a tree.
   - which paths are new compared to the previous cycle
   - Correlating with an actions trace, where are the RO patterns focused?
   - The steps in "areas.csv"
   TODO: Some multiples might be missing because of the star aliasing
   TODO: When I reason about size of directories I'm not counting the multiples

  thoughts:
   - I learned that they want to freeze at each cycle. I don't think it
     changes much for the plots, the reader should just consider distance 0
     and distance 1+


  small infos learned from areas.csv
  - 54_882MB (stops at last commit observed)
  - 236.9M entries
  - 18 areas from 427 to 444
  - areas 428+ have average 13.07M entries and 3120.63MB
    - area 427 have 112% entries and 59% bytes
  - area 427 has 1 commit (which?) 428-444 have between 8198 and 8204 commits

  smalls infos learned on full df0 (Beware, this does not cover the full file!!!)
  - 37.1GB total out of 57.5GB or 54.9GB
  - 296_483_303 entries total
  -  91_582_806 steps are Indirect while the dict capacity is 100_000
  - 408_225_829 steps are Direct, they weigh 14.9GB

  smalls infos learned on averaged trees:
  - 71% bytes inodes
  - 57% entries inodes
  - 46% bytes Nl_16385_plus
  - 36% entries Inode_root_values
  - 1% entries Tree [?]
  - 43% bytes /data/big_maps/*/*/*
  - 2.3GB a tree
  - 18M entries in a tree
  - 915MB of direct steps dont 745MB dans /data/big_maps/*/*/* [?]
  - 584MB of hash header [?]
  - 781MB in the rest [?]
  - 6% entries at distance <1 (13% for bytes)
  - 85% entries at distance 4+ (73% for bytes)
  - 000.12% of the bytes are reachable from 2 or more paths
  - "/data/contracts/index" has 33_820/886_966 Inode_nonroot_tree/Inode_nonroot_values which weigh 149MB
  - "/data/contracts/index/*" has 1_946_293 Inode_root_values which weigh 179MB
  - "/data/contracts/index/*/**" has 3.5M objects that use 207MB
  - "/data/contracts/index/*/**" has 0 indirect steps [?]
  - "/data/contracts/index/*/**" total:(87MB in direct, 111MB in headers, 86MB in others) [?]
  - "/data/contracts/index/*/**" has 190MB in contents, 107MB in headers, 82MB in rest [?]
  - 99% of the indirect steps are in bigmaps [?]

  small infos learned:
  - A tree touches most of the pages of the first area (between 89 and 99%)
  - An area contains roughly as many entries as one tree, but 2x more bytes
  - There are massive directories in [/data/big_maps/index/*/contents]
    - 58 inode trees with 11_017_653 steps, (10_796_975 which are direct and that take 701_803_429 byte)
  - The pack file grows 1GB per day



*)
open Import
open Main_tools

module Intset = Set.Make (struct
  type t = int

  let compare = compare
end)

module Int63map = Map.Make (Int63)

type contents_kind = [ `Contents ]
type inode_root = [ `Inode_root_tree | `Inode_root_values ]
type inode_nonroot = [ `Inode_nonroot_tree | `Inode_nonroot_values ]

type node_length =
  [ `Na
  | `Multiple
  | `Nl_0_32
  | `Nl_33_256
  | `Nl_257_2048
  | `Nl_2049_16384
  | `Nl_16385_plus ]
[@@deriving repr ~pp]

type contents_size = [ `Na | `Cs_0_31 | `Cs_32_127 | `Cs_128_511 | `Cs_512_plus ]
[@@deriving repr ~pp]

type kind =
  [ `Contents
  | `Inode_root_values
  | `Inode_root_tree
  | `Inode_nonroot_tree
  | `Inode_nonroot_values ]
[@@deriving repr ~pp]

type extended_kind =
  [ `Contents
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

type context_path = [ `Multiple | `Single of string ] [@@deriving repr]

let pp_context_path ppf = function
  | `Multiple -> Format.fprintf ppf "multiple"
  | `Single s -> Format.fprintf ppf "%s" s

type context = {
  path : context_path;
  node_length : [ `Outside | `Multiple | `Inside_one of int ];
}
[@@deriving repr]

let pp_context ppf context =
  pp_context_path ppf context.path;
  Format.fprintf ppf ",";
  match context.node_length with
  | `Multiple -> Format.fprintf ppf "multiple"
  | `Outside -> ()
  | `Inside_one len -> Format.fprintf ppf "%d" len

module D0 = struct
  type k = {
    parent_cycle_start : int;
    entry_area : int;
    path : context_path;
    kind : kind;
    node_length : node_length;
    contents_size : contents_size;
  }

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
      "parent_cycle_start,entry_area,path,kind,node_length,contents_size,count,bytes,indirect_count,direct_count,direct_bytes\n";
    Hashtbl.iter
      (fun k v ->
        Fmt.str "%d,%d,%a,%a,%a,%a,%d,%d,%d,%d,%d\n" k.parent_cycle_start
          k.entry_area pp_context_path k.path pp_kind k.kind pp_node_length
          k.node_length pp_contents_size k.contents_size v.count v.bytes
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
  type k = { area : int; kind : extended_kind; contents_size : contents_size }
  type v = { entry_count : int; byte_count : int }

  let path = "./areas.csv"

  let save tbl =
    let chan = open_out path in
    output_string chan "area,kind,contents_size,entry_count,byte_count\n";
    Hashtbl.iter
      (fun k v ->
        Fmt.str "%d,%a,%a,%d,%d\n" k.area pp_extended_kind k.kind
          pp_contents_size k.contents_size v.entry_count v.byte_count
        |> output_string chan)
      tbl;
    close_out chan
end

type acc = {
  dict : Dict.t;
  area_of_offset : int63 -> int;
  d0 : (D0.k, D0.v) Hashtbl.t;
  d1 : (D1.k, D1.v) Hashtbl.t;
  (* path_sharing : (string list, string list) Hashtbl.t; *)
  (* The last ones should be reset before each traversal *)
  entry_count : int;
  ancestor_cycle_start : int;
  leftmost_page_touched : int option;
  current_chunk : int63 option;
}

let blob_encoding_prefix_size = 32 + 1

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

let node_length_of_entry_exn (entry : _ Traverse.entry) =
  match entry.v with
  | `Contents -> assert false
  | `Inode comp -> (
      let open Traverse.Inode.Compress in
      match v_of_compress comp with
      | Values l -> List.length l
      | Tree { length; _ } -> length)

let kind_of_entry (entry : _ Traverse.entry) =
  match entry.v with `Contents -> `Contents | `Inode t -> kind_of_inode t

let register_entry ~acc ~length ~(context : context) ~entry_area ~kind
    ~parent_cycle_start ~raw_preds =
  let open D0 in
  let node_length =
    match context.node_length with
    | `Multiple -> `Multiple
    | `Outside -> `Na
    | `Inside_one len ->
        if len <= 32 then `Nl_0_32
        else if len <= 256 then `Nl_33_256
        else if len <= 2048 then `Nl_257_2048
        else if len <= 16384 then `Nl_2049_16384
        else `Nl_16385_plus
  in
  let contents_size =
    match kind with
    | `Contents ->
        let len = length - blob_encoding_prefix_size in
        assert (len >= 0);
        if len <= 31 then `Cs_0_31
        else if len <= 127 then `Cs_32_127
        else if len <= 511 then `Cs_128_511
        else `Cs_512_plus
    | _ -> `Na
  in
  let k =
    {
      parent_cycle_start;
      entry_area;
      path = context.path;
      kind;
      node_length;
      contents_size;
    }
  in
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
  let v = { v with count = v.count + 1; bytes = v.bytes + length } in
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

  let context = entry.payload in
  let current =
    let node_length =
      match (kind, context.node_length) with
      | #contents_kind, `Outside -> `Outside
      | #contents_kind, _ -> assert false
      | #inode_root, `Outside -> `Inside_one (node_length_of_entry_exn entry)
      | #inode_root, _ -> assert false
      | #inode_nonroot, `Multiple -> `Multiple
      | #inode_nonroot, `Inside_one len -> `Inside_one len
      | #inode_nonroot, `Outside -> assert false
    in
    { context with node_length }
  in

  (* if current.path === `Single [ "index"; "contracts"; "data" ] then (
   *   Fmt.epr "\n%!";
   *   Fmt.epr "%a\n%!" (Repr.pp context_t) current;
   *   Fmt.epr "\n%!";
   *   (match entry.v with
   *   | `Inode v ->
   *       Fmt.epr "%a\n%!" (Repr.pp Main_tools.Traverse.Inode.compress_t) v
   *   | `Contents -> assert false);
   *   failwith "super"); *)
  let context_of_child step_opt =
    let path =
      match current.path with
      | `Multiple -> `Multiple
      | `Single path ->
          let path =
            match step_opt with
            | None -> path
            | Some (step, (`Direct | `Indirect)) ->
              shorten_path (path ^ "/" ^ step)
          in
          `Single path
    in
    let node_length =
      match (step_opt, current.node_length) with
      | None, `Inside_one len -> `Inside_one len
      | None, `Multiple -> `Multiple
      | None, `Outside -> assert false
      | Some _, (`Multiple | `Inside_one _) -> `Outside
      | Some _, `Outside -> assert false
    in
    { path; node_length }
  in

  if acc.entry_count mod 2_000_000 = 0 then
    Fmt.epr "on_entry: %#d, area:%d, kind:%a, context:%a\n%!" acc.entry_count
      area pp_kind kind pp_context context;

  let raw_preds =
    match entry.v with `Contents -> [] | `Inode t -> preds_of_inode acc.dict t
  in

  register_entry ~acc ~length:entry.length ~context:current ~entry_area:area
    ~kind ~parent_cycle_start:ancestor_cycle_start ~raw_preds;

  let preds =
    List.map (fun (step_opt, off) -> (off, context_of_child step_opt)) raw_preds
  in
  ({ acc with entry_count = acc.entry_count + 1 }, preds)

let on_chunk acc (chunk : Revbuffer.chunk) =
  let first = chunk.offset in
  let right = Int63.(add_distance first chunk.length) in
  let last = Int63.(right - one) in
  let area = acc.area_of_offset first in
  assert (area = acc.area_of_offset last);
  let parent_cycle_start = acc.ancestor_cycle_start in
  assert (area < parent_cycle_start);
  let k = D1.{ parent_cycle_start; entry_area = area } in
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
  assert (first_page_touched <= last_page_touched);
  let incr_pages_touched =
    match acc.leftmost_page_touched with
    | None -> 1 + Int.distance_exn ~lo:first_page_touched ~hi:last_page_touched
    | Some leftmost_page_touched ->
        assert (first_page_touched <= leftmost_page_touched);
        assert (last_page_touched <= leftmost_page_touched);
        let progress =
          Int.distance_exn ~lo:first_page_touched ~hi:leftmost_page_touched
        in
        let pages_skiped =
          if last_page_touched = leftmost_page_touched then 0
          else
            Int.distance_exn ~lo:last_page_touched ~hi:leftmost_page_touched - 1
        in
        progress - pages_skiped
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

let merge_payloads _off ~older ~newer =
  let path = if older.path === newer.path then older.path else `Multiple in
  let node_length =
    if older.node_length === newer.node_length then older.node_length
    else
      match (older.node_length, newer.node_length) with
      | (`Inside_one _ | `Multiple), (`Inside_one _ | `Multiple) -> `Multiple
      | _ -> assert false
  in
  { path; node_length }

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

  (* Fmt.epr "\n%!";
   * Fmt.epr "\n%!";
   * Fmt.epr "First, traverse sequentially the file\n%!";
   * let () =
   *   let d2 = Hashtbl.create 1_000 in
   *   let _, _, offset_to_stop_at = cycle_starts |> List.rev |> List.hd in
   *   Seq_traverse.fold path () ~on_entry:(fun () off length entry ->
   *       let left = off in
   *       let right = Int63.add_distance off length in
   *       assert (Int63.(right <= offset_to_stop_at));
   *       let area = area_of_offset left in
   *       assert (area = area_of_offset Int63.(right - one));
   *       let open D2 in
   *       let kind, contents_size =
   *         match entry with
   *         | `Commit -> (`Commit, `Na)
   *         | `Contents ->
   *             ( `Contents,
   *               let len = length - blob_encoding_prefix_size in
   *               assert (len >= 0);
   *               if len <= 31 then `Cs_0_31
   *               else if len <= 127 then `Cs_32_127
   *               else if len <= 511 then `Cs_128_511
   *               else `Cs_512_plus )
   *         | `Inode t -> (kind_of_inode2 t, `Na)
   *       in
   *       let k = { area; kind; contents_size } in
   *       let v =
   *         match Hashtbl.find_opt d2 k with
   *         | None -> { entry_count = 1; byte_count = length }
   *         | Some prev ->
   *             {
   *               entry_count = prev.entry_count + 1;
   *               byte_count = prev.byte_count + length;
   *             }
   *       in
   *       Hashtbl.replace d2 k v;
   *       (\* Fmt.epr "on_entry %#14d, %d, %a, area:%d\n%!" (Int63.to_int off) length
   *        *   (Repr.pp Irmin_pack.Pack_value.Kind.t)
   *        *   kind area; *\)
   *       ((), Int63.(right < offset_to_stop_at)));
   *   D2.save d2
   * in *)

  (* if true then failwith "super"  ; *)
  let d0 = Hashtbl.create 1_000 in
  let d1 = Hashtbl.create 1_000 in
  (* let path_sharing = Hashtbl.create 100 in *)
  let acc =
    {
      dict;
      area_of_offset;
      d0;
      d1;
      (* path_sharing; *)
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
        let acc =
          Traverse.fold path
            [ (node_off, { path = `Single ""; node_length = `Outside }) ]
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
