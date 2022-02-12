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
   - () x                "per pack-file-area" x "per path prefix" x "per genre + commit"
     - # of total entries (needed for leftover calculation)
     - # of total bytes used (needed for leftover calculation)

   missing infos:
   - which area references which area? (i.e. analysis of pq when changing area)
   - intersection between trees

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

module Hash = Irmin_tezos.Schema.Hash
module Maker = Irmin_pack.Maker (Irmin_tezos.Conf)
module Store = Maker.Make (Irmin_tezos.Schema)
module Traverse = Traverse.Make (Irmin_tezos.Conf) (Irmin_tezos.Schema)
open Import

type hash = Store.hash [@@deriving repr ~pp]

module Key = struct
  include Irmin_pack.Pack_key
  include Irmin_pack.Pack_key.Make (Hash)

  type t = Store.node_key [@@deriving repr]

  let offset t =
    match inspect t with
    | Direct { offset; _ } -> offset
    | Indexed _ -> failwith "Trying to get offset from indexed key"

  let length t =
    match inspect t with
    | Direct { length; _ } -> length
    | Indexed _ -> failwith "Trying to get length from indexed key"
end

let loc =
  match Unix.gethostname () with
  | "DESKTOP-S4MOBKQ" -> `Home
  | "comanche" -> `Com
  | s -> Fmt.failwith "Unknown hostname %S\n%!" s

let path =
  match loc with
  | `Home ->
      "/home/nico/r/irmin/_artefacts/1f1548b9-b7d2-433b-b199-ef376b567951/store"
      (* "/home/nico/r/irmin/_artefacts/050a1e79-be77-45e5-a833-b963a3aad2d8/store" *)
      (* "/home/nico/r/irmin/_artefacts/45994b5d-94ed-4143-b0e5-3ba28f6f7a8e/store" *)
      (* "/mnt/y/tz/replay/none_a/store" *)
      (* "/home/nico/r/irmin/_artefacts/99e9b48e-4a9d-48d7-b9c3-4658104e4d48/store" *)
      (* "/home/nico/tz/hangzu_plus2_1916931_BLu79NTncAFXHiwoHDwir4BDjh2Bdc7jgL71QYGkjv2c2oD8FwZ/store_imported/context" *)
  | `Com ->
      "/home/ngoguey/bench/ro/hangzu_plus2_1916931_BLu79NTncAFXHiwoHDwir4BDjh2Bdc7jgL71QYGkjv2c2oD8FwZ/store_post_node_run/context/"

(* let root_hash =
 *   match loc with
 *   | `Home ->
 *       (\* https://tzkt.io/1916931 *\)
 *       (\* CYCLE & POSITION 428 (2 of 8192) *\)
 *       "CoV6QV47kn2oRnTihfjAC3dKPfrjEZjojMXVEYBLPYM7EmFkDqdS"
 *   | `Com ->
 *       (\* https://tzkt.io/2056193 *\)
 *       (\* CYCLE & POSITION 445 (0 of 8192) *\)
 *       "CoWMUSFj7gp4LngpAhaZa62xPYZcKWMyr4Wnh14CcyyQWsPrghLx" *)

let hash_of_string =
  let f = Repr.of_string Irmin_tezos.Schema.Hash.t in
  fun x -> match f x with Error (`Msg x) -> failwith x | Ok v -> v

(* let root_hash = hash_of_string root_hash *)
let hash_to_bin_string = Repr.to_bin_string hash_t |> Repr.unstage

(* TODO: maybe shift some of these stats to traverse et al? *)
type acc = {
  entry_count : int;
  tot_length : int;
  tot_chunk : int;
  tot_chunk_algo : int;
  tot_length_chunk : int;
  current_chunk : (int63 * int63) option;
  largest_algo_chunk : int;
  largest_chunk : int;
  emission_reset : int;
  emission_witnessed : int;
  emission_oos : int;
}
[@@deriving repr ~pp]

type p = Payload

let offset_of_address =
  let open Traverse.Inode.Compress in
  function
  | Offset x -> x
  | Hash _ -> failwith "traverse doesn't handle inode children by hash"

let preds_of_inode v =
  let open Traverse.Inode.Compress in
  let v =
    match v.tv with
    | V0_stable v | V0_unstable v | V1_root { v; _ } | V1_nonroot { v; _ } -> v
  in
  match v with
  | Values l ->
      List.map
        (function
          | Contents (_, addr, _) -> offset_of_address addr
          | Node (_, addr) -> offset_of_address addr)
        l
  | Tree { entries; _ } ->
      List.map (fun (e : ptr) -> offset_of_address e.hash) entries

let accumulate acc (entry : _ Traverse.entry) =
  if acc.entry_count mod 1_500_000 = 0 then
    Fmt.epr "accumulate: %#d\n%!" acc.entry_count;

  let preds =
    match entry.v with `Contents -> [] | `Inode t -> preds_of_inode t
  in
  let preds = List.map (fun off -> (off, Payload)) preds in
  let acc =
    {
      acc with
      entry_count = acc.entry_count + 1;
      tot_length = acc.tot_length + entry.length;
    }
  in
  (acc, preds)

let on_chunk acc (chunk : Revbuffer.chunk) =
  let left_offset = chunk.offset in
  let right_offset = Int63.add_distance left_offset chunk.length in
  let tot_chunk, current_chunk =
    match acc.current_chunk with
    | None ->
        assert (acc.tot_chunk = 0);
        (* Very first algo_chunk of very first chunk *)
        (1, (left_offset, right_offset))
    | Some (left, right) when Int63.(left = right_offset) ->
        (* Continuity of current_chunk *)
        (acc.tot_chunk, (left_offset, right))
    | Some _ ->
        (* Beginning of new chunk *)
        (acc.tot_chunk + 1, (left_offset, right_offset))
  in
  let largest_algo_chunk = max acc.largest_algo_chunk chunk.length in
  let largest_chunk =
    max acc.largest_chunk
      (Int63.distance_exn ~lo:(fst current_chunk) ~hi:(snd current_chunk))
  in
  let acc =
    match chunk.emission_reason with
    | `Reset -> { acc with emission_reset = acc.emission_reset + 1 }
    | `Witnessed_discontinuity ->
        { acc with emission_witnessed = acc.emission_witnessed + 1 }
    | `Out_of_space -> { acc with emission_oos = acc.emission_oos + 1 }
  in
  {
    acc with
    tot_chunk;
    tot_chunk_algo = acc.tot_chunk_algo + 1;
    tot_length_chunk = acc.tot_length_chunk + chunk.length;
    current_chunk = Some current_chunk;
    largest_algo_chunk;
    largest_chunk;
  }

let root_node_offset_of_commit commit =
  let k =
    match Store.Commit.tree commit |> Store.Tree.key with
    | None -> assert false
    | Some (`Contents _) -> assert false
    | Some (`Node k) -> k
  in
  let offset =
    match Key.inspect k with
    | Indexed _ -> assert false
    | Direct { offset; _ } -> offset
  in
  offset

let lookup_cycles_in_repo repo =
  let+ l =
    Lwt_list.fold_left_s
      (fun acc (cycle : Cycle_start.t) ->
        let h = hash_of_string cycle.context_hash in
        let* commit_opt = Store.Commit.of_hash repo h in
        let acc =
          Option.fold ~none:acc
            ~some:(fun commit ->
              let offset = root_node_offset_of_commit commit in
              (* Fmt.epr "pack store contains %a at offset %#14d\n%!" Cycle_start.pp *)
              (* cycle (Int63.to_int offset); *)
              (cycle, offset) :: acc)
            commit_opt
        in
        Lwt.return acc)
      [] Cycle_start.all
  in
  List.rev l

let main () =
  Fmt.epr "Hello World\n%!";

  let conf = Irmin_pack.config ~fresh:false ~readonly:true path in
  let* repo = Store.Repo.v conf in
  let* cycles = lookup_cycles_in_repo repo in
  Fmt.epr "pack-store contains %d cycles\n%!" (List.length cycles);

  let cycles = List.rev cycles in

  List.iter
    (fun (cycle, offset) ->
      Fmt.epr "pack store contains %a at offset %#14d\n%!" Cycle_start.pp cycle
        (Int63.to_int offset);
      let acc0 =
        {
          entry_count = 0;
          tot_length = 0;
          tot_chunk = 0;
          tot_chunk_algo = 0;
          tot_length_chunk = 0;
          current_chunk = None;
          largest_algo_chunk = 0;
          largest_chunk = 0;
          emission_reset = 0;
          emission_witnessed = 0;
          emission_oos = 0;
        }
      in
      let acc =
        Traverse.fold path
          [ (offset, Payload) ]
          (fun _off ~older:Payload ~newer:Payload -> Payload)
          accumulate on_chunk acc0
      in
      Fmt.epr "%a\n%!" pp_acc acc;
      Fmt.epr "\n%!";
      Fmt.epr "\n%!";
      if true then failwith "super"      ;
      Fmt.epr "\n%!")
    cycles;

  (* let* commit_opt = Store.Commit.of_hash repo root_hash in
   * let commit =
   *   match commit_opt with
   *   | None -> failwith "Could not find root_hash in index"
   *   | Some c -> c
   * in
   * let offset = root_node_offset_of_commit commit in *)
  Fmt.epr "Bye World\n%!";
  Lwt.return_unit

let () = Lwt_main.run (main ())
