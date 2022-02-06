(** Hello

    Throughout the code [left] and [right] are the names used to designate
    classic ranges where [left] is the beginning of a range and [right] is
    [left + length].

    Throughout the code [first] and [last] are the names used to designate
    non-empty ranges where [first] is the index of the first element of the
    range and [last] is [first + length - 1].
*)

module Int63 = Optint.Int63
module Hash = Irmin_tezos.Schema.Hash
module Maker = Irmin_pack.Maker (Irmin_tezos.Conf)
module Store = Maker.Make (Irmin_tezos.Schema)
module IO = Pack_file_ios
open Import

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

module Pq = struct
  include Priority_queue.Make (struct
    type t = Key.t

    let compare a b = Int63.compare (Key.offset a) (Key.offset b)
  end)

  let push t k x = update t k (function None -> [ x ] | Some l -> x :: l)
end

type key = Key.t [@@deriving repr ~pp]
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

let ensure_key_is_in_buf folder k =
  let left_offset = Key.offset k in
  let length = Key.length k in
  let right_offset = Int63.add_distance left_offset length in
  let page_range = IO.page_range_of_offset_length left_offset length in
  match Revbuffer.first_offset_opt folder.buf with
  | None ->
      (* 1 - Nothing in rev buffer *)
      IO.load_pages folder.io page_range (Revbuffer.ingest folder.buf)
  | Some first_loaded_offset ->
      let first_loaded_page_idx = IO.page_idx_of_offset first_loaded_offset in
      if page_range.first > first_loaded_page_idx then
        (* We would have already loaded pages lower than page_range *)
        assert false
      else if page_range.first = first_loaded_page_idx then
        (* 2 - We have already loaded all the needed pages *)
        ()
      else if page_range.last >= first_loaded_page_idx then (
        (* 3 - Some of the needed pages are already loaded (not all) *)
        assert (page_range.first < first_loaded_page_idx);
        let page_range = { page_range with last = first_loaded_page_idx - 1 } in
        IO.load_pages folder.io page_range (Revbuffer.ingest folder.buf))
      else (
        (* 4 - None of the needed pages are loaded *)
        Revbuffer.reset folder.buf right_offset;
        IO.load_pages folder.io page_range (Revbuffer.ingest folder.buf))

let traverse folder =
  if Pq.is_empty folder.pq then (
    Fmt.epr "> travese: bye bye\n%!";
    ())
  else
    let k, _truc = Pq.pop_exn folder.pq in
    ensure_key_is_in_buf folder k;
    (* Fmt.epr "> traverse %a pages:%#d-%#d\n%!" pp_key k page_range.first *)
      (* page_range.last; *)
    ()

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
  Pq.push pq root_key 42;
  let buf =
    Revbuffer.create ~capacity:(4096 * 100)
      ~right_offset:
        (Key.offset root_key ++ (Key.length root_key |> Int63.of_int))
  in
  let folder = { buf; io; pq } in
  traverse folder;
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
   - blobs size *)
