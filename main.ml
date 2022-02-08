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

let ref_t x = Repr.map x ref ( ! )

type stats = {
  hit : int ref;
  blindfolded_perfect : int ref;
  blindfolded_too_much : int ref;
  blindfolded_not_enough : int ref;
  was_previous : int ref;
}
[@@deriving repr ~pp]

let fresh_stats () =
  {
    hit = ref 0;
    blindfolded_perfect = ref 0;
    blindfolded_too_much = ref 0;
    blindfolded_not_enough = ref 0;
    was_previous = ref 0;
  }

type folder = {
  pq : cycle_idx list Pq.t;
  buf : Revbuffer.t;
  io : IO.t;
  decode_inode : string -> int -> Inode.Val.t * int63 list;
  stats : stats;
}

let ( ++ ) = Int63.add
let ( -- ) = Int63.sub

(** maximum size of a blob *)
let buffer_capacity = 4096 * 100

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

(* TODO: Maybe more, let's compute stats on misses. The goal is to minimise
   [too_much + not_enough]. *)
let expected_entry_size = 40

let decode_entry_length folder offset =
  (* Using [min_bytes_needed_to_discover_length] just so [read] doesn't
     crash. [read] should be improved. *)
  Revbuffer.read ~mark_dirty:false folder.buf offset
    min_bytes_needed_to_discover_length
  @@ fun buf i0 ->
  let ilength = i0 + Hash.hash_size + 1 in
  let pos_ref = ref ilength in
  let suffix_length = Varint.decode_bin buf pos_ref in
  let length_length = !pos_ref - ilength in
  Hash.hash_size + 1 + length_length + suffix_length

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
  Fmt.epr "   length: %d\n%!" length;
  let actual_page_range = IO.page_range_of_offset_length left_offset length in
  if actual_page_range.last > guessed_page_range.last then (
    incr folder.stats.blindfolded_not_enough;
    (* We missed loading enough. Let's start again without reusing what was
       just loaded. *)
    let page_range_right_offset =
      IO.right_offset_of_page_idx folder.io actual_page_range.last
    in
    Revbuffer.reset folder.buf page_range_right_offset;
    IO.load_pages folder.io actual_page_range (Revbuffer.ingest folder.buf))
  else if actual_page_range.last < guessed_page_range.last then
    (* Loaded too much *)
    incr folder.stats.blindfolded_too_much
  else (* Loaded just what was needed *)
    incr folder.stats.blindfolded_perfect

let ensure_entry_is_in_buf folder left_offset =
  let left_page_idx = IO.page_idx_of_offset left_offset in
  match Revbuffer.first_offset_opt folder.buf with
  | None ->
      (* 1 - Nothing in rev buffer. This only happens the first time we enter
         [ensure_entry_is_in_buf]. *)
      blindfolded_load_entry_in_buf folder left_offset;
      Revbuffer.show folder.buf
  | Some first_loaded_offset ->
      let first_loaded_page_idx = IO.page_idx_of_offset first_loaded_offset in
      if left_page_idx > first_loaded_page_idx then
        (* We would have already loaded pages lower than page_range *)
        assert false
      else if left_page_idx = first_loaded_page_idx then (
        (* 2 - We have already loaded all the needed pages *)
        incr folder.stats.hit;
        ())
      else if left_page_idx = first_loaded_page_idx - 1 then (
        (* 3 - The beginning of the entry is in the next page on the left. We
           don't know if it is totally contained in that left page or if it also
           spans on [first_loaded_page_idx]. It doesn't matter, we can deal with
           both cases the same way. *)
        Fmt.epr "   mode 3 (in prev)\n%!";
        incr folder.stats.was_previous;
        IO.load_page folder.io left_page_idx (Revbuffer.ingest folder.buf);
        Revbuffer.show folder.buf)
      else (
        (* 4 - If the entry spans on 3 pages, we might have a suffix of it in
           buffer, nerver mind, let's discard everything in the buffer. *)
        Fmt.epr "   mode 4 (far)\n%!";
        blindfolded_load_entry_in_buf folder left_offset;
        Revbuffer.show folder.buf)

let decode_entry folder offset =
  (* let length = decode_entry_length folder offset in *)

  (* Using [min_bytes_needed_to_discover_length] just so [read] doesn't
     crash. TODO: [read] should be improved. *)
  Revbuffer.read ~mark_dirty:true folder.buf offset
    min_bytes_needed_to_discover_length
  @@ fun buf i0 ->
  (* Fmt.epr "   ** length:%d \n%!" length; *)
  (* Fmt.epr "   ** %S\n%!" (String.sub buf i0 length); *)
  let imagic = i0 + Hash.hash_size in

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
    Fmt.epr "> traverse %#d: bye bye\n%!" (Int63.to_int i);
    ())
  else
    let offset, _truc = Pq.pop_exn folder.pq in
    Fmt.epr "> traverse %#d: offset:%#14d, page:%d, pq:%#d (%a)\n%!"
      (Int63.to_int i) (Int63.to_int offset)
      (IO.page_idx_of_offset offset)
      (Pq.length folder.pq) pp_stats folder.stats;
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

  if true then failwith "super";

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
    Revbuffer.create ~capacity:buffer_capacity
      ~right_offset:root_page_right_offset
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
      let off_ref = ref offset in
      let v_with_corrupted_keys = to_raw string off_ref |> of_raw in
      Fmt.epr "   Read from %d to %d (length: %d)\n%!" offset !off_ref
        (!off_ref - offset);
      let l = !preds in
      preds := [];
      (v_with_corrupted_keys, l)
  in

  let stats = fresh_stats () in
  let folder = { buf; io; pq; decode_inode; stats } in
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
   - the traversal itself (c'est seulement utile si je traverse un seul commit lol)
     - traversal timings
     - stats on size of pq
     - distribution of situations on pull
       - also include number of pulled pages?
     - buffer blits

   to show:
   - (?) averaged on all ref commits
     - at which cycle-distance are all genre (entry weighted / bytes weighted)
     - at which cycle-distance are all paths (entry weighted / bytes weighted)
   - (camembert) averaged on all ref commits + (curves) evolution for all ref commits
     - at which cycle-distance are the entries (entry weighted / bytes weighted)
     - number in each genre  (entry weighted / bytes weighted)
     - number in each directory (entry weighted / bytes weighted)
     - which path grows the most (entry weighted / bytes weighted)



*)
