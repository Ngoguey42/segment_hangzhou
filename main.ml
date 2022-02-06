(** Hello

    Throughout the code [left] and [right] are the names used to designate
    classic ranges where [left] is the beginning of a range and [right] is
    [left + length].

    Throughout the code [first] and [last] are the names used to designate
    non-empty ranges where [first] is the index of the first element of the
    range and [last] is [first + length - 1].

    TODO: Disallow or implement schemas with no length in blobs

*)

module Int63 = Optint.Int63
module Hash = Irmin_tezos.Schema.Hash
module Maker = Irmin_pack.Maker (Irmin_tezos.Conf)
module Store = Maker.Make (Irmin_tezos.Schema)
module IO = Pack_file_ios
module Kind = Irmin_pack.Pack_value.Kind
open Import

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

module Inode = struct
  module Value = Irmin_tezos.Schema.Node (Key) (Key)
  include Irmin_pack.Inode.Make_internal (Irmin_tezos.Conf) (Hash) (Key) (Value)
end

module Pq = struct
  include Priority_queue.Make (struct
    type t = int63

    let compare = Int63.compare
  end)

  let push t k x = update t k (function None -> [ x ] | Some l -> x :: l)
end

type kind = Kind.t [@@deriving repr ~pp]
type hash = Store.hash [@@deriving repr ~pp]
type key = Key.t [@@deriving repr ~pp]
type cycle_idx = int

type folder = {
  pq : cycle_idx list Pq.t;
  buf : Revbuffer.t;
  io : IO.t;
  decode_inode : string -> int -> Inode.Val.t * int63 list;
}

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

(* let is_entry_in_buffer folder offset *)

module Varint = struct
  type t = int [@@deriving repr ~decode_bin]

  let min_encoded_size = 1

  (** LEB128 stores 7 bits per byte. An OCaml [int] has at most 63 bits.
      [63 / 7] equals [9]. *)
  let max_encoded_size = 9
end

let min_bytes_needed_to_discover_length =
  Hash.hash_size + 1 + Varint.min_encoded_size

let max_bytes_needed_to_discover_length =
  Hash.hash_size + 1 + Varint.max_encoded_size

(* TODO: Maybe more, let's compute stats on misses *)
let expected_entry_size = 40

let decode_entry_length folder offset =
  (* Using [min_bytes_needed_to_discover_length] just so [read] doesn't
     crash. [read] should be improved. *)
  Revbuffer.read ~mark_dirty:false folder.buf offset
    min_bytes_needed_to_discover_length
  @@ fun buf i0 ->
  let ilength = i0 + Hash.hash_size + 1 in
  Varint.decode_bin buf (ref ilength)

let blindfolded_load_entry_in_buf folder left_offset =
  let guessed_length =
    max_bytes_needed_to_discover_length + expected_entry_size
  in
  let guessed_page_range =
    IO.page_range_of_offset_and_guessed_length folder.io left_offset
      guessed_length
  in
  let page_range_right_offset =
    IO.right_offset_of_page_idx folder.io guessed_page_range.last
  in
  Revbuffer.reset folder.buf page_range_right_offset;
  IO.load_pages folder.io guessed_page_range (Revbuffer.ingest folder.buf);
  let length = decode_entry_length folder left_offset in
  let actual_page_range = IO.page_range_of_offset_length left_offset length in
  if actual_page_range.last > guessed_page_range.last then (
    (* We missed loading enough. Let's start again without reusing what was
       just loaded. *)
    let page_range_right_offset =
      IO.right_offset_of_page_idx folder.io actual_page_range.last
    in
    Revbuffer.reset folder.buf page_range_right_offset;
    IO.load_pages folder.io actual_page_range (Revbuffer.ingest folder.buf))
  else if actual_page_range.last < guessed_page_range.last then
    (* Loaded too much *)
    ()
  else (* Loaded just what was needed *)
    ()

let ensure_entry_is_in_buf folder left_offset =
  let left_page_idx = IO.page_idx_of_offset left_offset in
  match Revbuffer.first_offset_opt folder.buf with
  | None ->
      (* 1 - Nothing in rev buffer. This only happens the first time we enter
         [ensure_entry_is_in_buf]. *)
      blindfolded_load_entry_in_buf folder left_offset
  | Some first_loaded_offset ->
      let first_loaded_page_idx = IO.page_idx_of_offset first_loaded_offset in
      if left_page_idx > first_loaded_page_idx then
        (* We would have already loaded pages lower than page_range *)
        assert false
      else if left_page_idx = first_loaded_page_idx then
        (* 2 - We have already loaded all the needed pages *)
        ()
      else if left_page_idx = first_loaded_page_idx + 1 then
        (* 3 - The beginning of the entry is in the next page on the left. We
           don't know if it is totally contained in that left page of if it also
           spans on [first_loaded_page_idx]. It doesn't matter, we can deal with
           both cases the same way. *)
        IO.load_page folder.io left_page_idx (Revbuffer.ingest folder.buf)
      else
        (* 3 - If the entry spans on 3 pages, we might have a suffix of it in
           buffer, nerver mind, let's discard everything in the buffer. *)
        blindfolded_load_entry_in_buf folder left_offset

let decode_entry folder offset =
  (* Using [min_bytes_needed_to_discover_length] just so [read] doesn't
     crash. [read] should be improved. *)
  Revbuffer.read ~mark_dirty:true folder.buf offset
    min_bytes_needed_to_discover_length
  @@ fun buf i0 ->
  let imagic = i0 + Hash.hash_size in
  (* Fmt.epr "Size of buffer                   %d\n%!" (String.length buf); *)
  (* Fmt.epr "Supposed to have entry at bufidx %d\n%!" i0; *)
  (* Fmt.epr "Looking for magic at bufidx      %d\n%!" imagic; *)
  (* Fmt.epr "%S\n%!" (String.sub buf i0 length); *)
  let kind = Kind.of_magic_exn buf.[imagic] in
  Fmt.epr "   %a\n%!" pp_kind kind;
  match kind with
  | Inode_v1_unstable | Inode_v1_stable | Commit_v1 | Commit_v2 ->
      Fmt.failwith "unhandled %a" pp_kind kind
  | Contents -> (`Contents, [])
  | Inode_v2_root | Inode_v2_nonroot ->
      let v_with_corrupted_keys, preds = folder.decode_inode buf i0 in
      (`Inode v_with_corrupted_keys, preds)

let rec traverse i folder =
  if Pq.is_empty folder.pq then (
    Fmt.epr "> traverse %d: bye bye\n%!" (Int63.to_int i);
    ())
  else
    let offset, _truc = Pq.pop_exn folder.pq in
    Fmt.epr "> traverse %d: offset:%#14d\n%!" (Int63.to_int i) (Int63.to_int offset);
    ensure_entry_is_in_buf folder offset;
    let _entry, preds = decode_entry folder offset in
    Fmt.epr "   %d preds\n%!" (List.length preds);
    List.iter (fun offset -> Pq.push folder.pq offset 42) preds;
    traverse (Int63.succ i) folder

let hash_to_bin_string = Repr.to_bin_string hash_t |> Repr.unstage

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
  let root_left_offset = Key.offset root_key in
  let root_right_offset =
    Int63.add_distance root_left_offset (Key.length root_key)
  in
  let root_page_right_offset =
    IO.right_offset_of_page_idx_from_offset io root_right_offset
  in

  Fmt.epr "\n%!";
  Fmt.epr "\n%!";
  Fmt.epr "commit hash: %a\n%!" pp_hash root_hash;
  Fmt.epr "commit hash: %S\n%!" (hash_to_bin_string root_hash);
  let h = Key.to_hash root_key in
  Fmt.epr "root_key: %a\n%!" pp_key root_key;
  Fmt.epr "    hash: %a\n%!" pp_hash h;
  Fmt.epr "    hash: %S\n%!" (hash_to_bin_string h);

  let* node =
    match Store.Commit.tree commit |> Store.Tree.destruct with
    | `Contents _ -> assert false
    | `Node n -> Store.to_backend_node n
  in
  Fmt.epr "\n%!";
  Fmt.epr "\n%!";
  Fmt.epr "inode:\n%!";
  Fmt.epr "%a\n%!" (Repr.pp Store.Backend.Node.Val.t) node;
  Fmt.epr "\n%!";
  Fmt.epr "\n%!";

  let pq = Pq.create () in
  Pq.push pq root_left_offset 42;
  let buf =
    Revbuffer.create ~capacity:(4096 * 100) ~right_offset:root_page_right_offset
  in
  let decode_inode =
    let preds = ref [] in
    let dict =
      let open Irmin_pack.Dict in
      v ~fresh:false ~readonly:true path |> find
    in
    let key_of_offset offset =
      preds := offset :: !preds;
      (* Generating a corrupted key *)
      Key.v_direct ~hash:root_hash ~length:0 ~offset
    in
    let key_of_hash _h =
      (* might be dangling hash, no choice but to fail anyway *)
      assert false
    in
    let to_raw = Inode.Raw.decode_bin ~dict ~key_of_offset ~key_of_hash in
    let find ~expected_depth:_ _k = None in
    let of_raw = Inode.Val.of_raw find in
    fun string offset ->
      preds := [];
      let v_with_corrupted_keys = to_raw string (ref offset) |> of_raw in
      let l = !preds in
      preds := [];
      (v_with_corrupted_keys, l)
  in

  let folder = { buf; io; pq; decode_inode } in
  traverse Int63.zero folder;
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
