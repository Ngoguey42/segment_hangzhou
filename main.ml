(** Hello *)

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
      "/home/nico/tz/hangzu_plus2_1916931_BLu79NTncAFXHiwoHDwir4BDjh2Bdc7jgL71QYGkjv2c2oD8FwZ/store_post_node_run/context"
  | `Com ->
      "/home/ngoguey/bench/ro/hangzu_plus2_1916931_BLu79NTncAFXHiwoHDwir4BDjh2Bdc7jgL71QYGkjv2c2oD8FwZ/store_post_node_run/context/"

let root_hash =
  match loc with
  | `Home ->
      (* https://tzkt.io/1916931 *)
      (* CYCLE & POSITION 428 (2 of 8192) *)
      "CoV6QV47kn2oRnTihfjAC3dKPfrjEZjojMXVEYBLPYM7EmFkDqdS"
  | `Com ->
      (* https://tzkt.io/2056193 *)
      (* CYCLE & POSITION 445 (0 of 8192) *)
      "CoWMUSFj7gp4LngpAhaZa62xPYZcKWMyr4Wnh14CcyyQWsPrghLx"

let hash_of_string =
  let f = Repr.of_string Irmin_tezos.Schema.Hash.t in
  fun x -> match f x with Error (`Msg x) -> failwith x | Ok v -> v

let root_hash = hash_of_string root_hash
let hash_to_bin_string = Repr.to_bin_string hash_t |> Repr.unstage

type acc = { i : int }

let accumulate acc _entry =
  if acc.i mod 3_000_000 = 0 then Fmt.epr "accumulate: %#d\n%!" acc.i;

  { acc with i = acc.i + 1 }

let main () =
  Fmt.epr "Hello World\n%!";

  let conf = Irmin_pack.config ~fresh:false ~readonly:true path in
  let* repo = Store.Repo.v conf in
  let* cycles =
    Lwt_list.fold_left_s
      (fun acc (cycle : Cycles.t) ->
        let h = hash_of_string cycle.context_hash in
        let* commit_opt = Store.Commit.of_hash repo h in
        let acc =
          Option.fold ~none:acc
            ~some:(fun c ->
              let k = Store.Commit.key c in
              let offset =
                match Key.inspect k with
                | Indexed _ -> assert false
                | Direct { offset; _ } -> offset
              in
              Fmt.epr "pack store contains %a at offset %#14d\n%!" Cycles.pp
                cycle (Int63.to_int offset);
              (cycle, c) :: acc)
            commit_opt
        in
        Lwt.return acc)
      [] Cycles.l
  in
  Fmt.epr "pack-store contains %d cycles\n%!" (List.length cycles);

  let* commit_opt = Store.Commit.of_hash repo root_hash in
  let commit =
    match commit_opt with
    | None -> failwith "Could not find root_hash in index"
    | Some c -> c
  in
  let root_key =
    match Store.Commit.tree commit |> Store.Tree.key with
    | None -> assert false
    | Some (`Contents _) -> assert false
    | Some (`Node k) -> k
  in
  let root_left_offset = Key.offset root_key in

  let acc0 = { i = 0 } in
  let acc = Traverse.fold path [ root_left_offset ] accumulate acc0 in
  ignore acc;

  Fmt.epr "Bye World\n%!";
  Lwt.return_unit

let () = Lwt_main.run (main ())

(* Etapes:
   - Pulls la liste de tous les cycles de tzstats.com
   - Filtrer cette liste en fonction de l'index, recup des commit_key
   - S'assurrer que cette liste est non vide et sans trous

   - init [pq]
   - init [results]
   - Pour chaque cycle, du plus grand au plus petit: [current_cycle]
      - Inserer dans [pq] la tuple [key du root node du commit du cycle], [cycle_id], ["/"]
      - Tant que pq n'est pas vide et que [max pq] est plus grand que l'offset du commit du cycle precedent
         - [key, parent_cycles, path = pop_max pq]
         - Apprendre: length, genre (blob-{0-31,32-127,128,511,512+}|inode-{root,inner}-{tree,val}), (pred * step_opt) list
         - [path'] c'est le prefix de taille 2 de [path / step_opt]
         - pour chaque [parent_cycle]
           - [k = parent_cycle, current_cycle, path', genre]
           - [results_count[k] += (1, length)]
         - inserer les [preds] dans [pq] annotes avec [parent_cycles] et [path']

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

   not needed?
   - () x                "per pack-file-area" x ()                x ()
     - # of bytes
     - # of entries

   missing infos:
   - which area references which area? (i.e. analysis of pq when changing area)
   - intersection between trees
   - the traversal itself (c'est seulement utile si je traverse un seul commit lol)
     - traversal timings
     - stats on size of pq
     - distribution of situations on pull
       - also include number of pulled pages?
     - buffer blits

   to show:
   - (leftward horizontal histogram?) averaged on all ref commits
     - at which cycle-distance are all genre (entry weighted / bytes weighted)
     - at which cycle-distance are all paths (entry weighted / bytes weighted)
   - (camembert) averaged on all ref commits + (curves) evolution for all ref commits
     - at which cycle-distance are the entries (entry weighted / bytes weighted)
     - number in each genre  (entry weighted / bytes weighted)
     - number in each directory (entry weighted / bytes weighted)
     - which path grows the most (entry weighted / bytes weighted)
*)
