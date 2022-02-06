module Int63 = Optint.Int63
module Hash = Irmin_tezos.Schema.Hash
module Maker = Irmin_pack.Maker (Irmin_tezos.Conf)
module Store = Maker.Make (Irmin_tezos.Schema)
module IO = Pack_file_ios
open Lwt.Syntax

module Key = struct
  open Irmin_pack.Pack_key

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

module Pq = Priority_queue.Make (struct
  type t = Key.t

  let compare a b = Int63.compare (Key.offset a) (Key.offset b)
end)

type key = Key.t [@@deriving repr ~pp]
type hash = Store.hash [@@deriving repr ~pp]
type int63 = Int63.t [@@deriving repr ~pp]
type cycle_idx = int
type folder = { pq : cycle_idx list Pq.t; buf : Revbuffer.t; io : IO.t }

let ( ++ ) = Int63.add
let ( -- ) = Int63.sub

let loc =
  match Unix.gethostname () with
  | "DESKTOP-S4MOBKQ" -> `Home
  | s -> Fmt.failwith "Unknown hostname %S\n%!" s

let path =
  match loc with
  | `Home ->
      "/home/nico/tz/hangzu_plus2_1916931_BLu79NTncAFXHiwoHDwir4BDjh2Bdc7jgL71QYGkjv2c2oD8FwZ/store_post_node_run/context"
  | `Com ->
      "/home/ngoguey/bench/ro/hangzu_plus2_1916931_BLu79NTncAFXHiwoHDwir4BDjh2Bdc7jgL71QYGkjv2c2oD8FwZ/store_post_node_run/context/"

let hash_of_string =
  let f = Repr.of_string Irmin_tezos.Schema.Hash.t in
  fun x -> match f x with Error (`Msg x) -> failwith x | Ok v -> v

let root_hash =
  hash_of_string "CoV6QV47kn2oRnTihfjAC3dKPfrjEZjojMXVEYBLPYM7EmFkDqdS"

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
         - Apprendre: length, genre (blob|inode-{root,inner}-{tree,val}), preds, step_opt
         - [path'] c'est le prefix de taille 2 de [path / step_opt]
         - pour chaque [parent_cycle]
           - [k = parent_cycle, current_cycle, path', genre]
           - [results_count[k] += 1]
           - [results_bytes[k] += length]
         - inserer les [preds] dans [pq] annotes avec [parent_cycles] et [path']

   missing infos:
   - quantity of entries/bytes "belonging" to each cycle commit
   - quantity of entries/bytes per cycle
   - number of contiguous chunks of entries/bytes
   - pages
   - blobs size
*)

let main () =
  Fmt.epr "Hello World\n%!";
  let conf = Irmin_pack.config ~fresh:false ~readonly:true path in
  let* repo = Store.Repo.v conf in
  let io = IO.v (Filename.concat path "store.pack") in
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
  Fmt.epr "root_key: %a\n%!" pp_key root_key;
  let pq = Pq.create () in
  let buf =
    Revbuffer.create ~capacity:(4096 * 100)
      ~right_offset:
        (Key.offset root_key ++ (Key.length root_key |> Int63.of_int))
  in
  let folder = { buf; io; pq } in
  Fmt.epr "Bye World\n%!";
  Lwt.return_unit

let () = Lwt_main.run (main ())
