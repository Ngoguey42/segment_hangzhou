(** Maximum number of bytes from disk to keep in memory at once.

    A lower value uses less memory.

    A higher value minimises the number of [blit] in [buf].

    In an entry ever has a size over [buffer_capacity], the program will crash. *)
let buffer_capacity = 4096 * 100

(** The optimal [expected_entry_size] minimises
    [blindfolded_too_much + blindfolded_not_enough]. [50] showed good results
    experimentally. *)
let expected_entry_size = 50

open Import
module IO = Pack_file_ios
module Kind = Irmin_pack.Pack_value.Kind

type kind = Kind.t [@@deriving repr ~pp]

module Pq = struct
  include Priority_queue.Make (struct
    type t = int63

    let compare = Int63.compare
  end)

  let push t k x = update t k (function None -> [ x ] | Some l -> x :: l)
end

module Make (Conf : Irmin_pack.Conf.S) (Schema : Irmin.Schema.Extended) = struct
  module Maker = Irmin_pack.Maker (Conf)
  module Store = Maker.Make (Schema)
  module Hash = Store.Hash
  module Key = Irmin_pack.Pack_key.Make (Hash)

  type hash = Store.hash [@@deriving repr ~pp]
  type key = Key.t [@@deriving repr ~pp]

  module Inode = struct
    module Value = Schema.Node (Key) (Key)
    include Irmin_pack.Inode.Make_internal (Conf) (Hash) (Key) (Value)
  end

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

  type foldecr = {
    pq : int list Pq.t;
    buf : Revbuffer.t;
    io : IO.t;
    decode_inode : string -> int -> Inode.Val.t * int63 list;
    stats : stats;
  }

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

  let decode_entry_length folder offset =
    Revbuffer.read ~mark_dirty:false folder.buf offset @@ fun buf i0 ->
    let available_bytes = String.length buf - i0 in
    assert (available_bytes >= min_bytes_needed_to_discover_length);
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
        blindfolded_load_entry_in_buf folder left_offset
    | Some first_loaded_offset ->
        let first_loaded_page_idx = IO.page_idx_of_offset first_loaded_offset in
        if left_page_idx > first_loaded_page_idx then
          (* We would have already loaded pages lower than page_range *)
          assert false
        else if left_page_idx = first_loaded_page_idx then
          (* 2 - We have already loaded all the needed pages *)
          incr folder.stats.hit
        else if left_page_idx = first_loaded_page_idx - 1 then (
          (* 3 - The beginning of the entry is in the next page on the left. We
             don't know if it is totally contained in that left page or if it also
             spans on [first_loaded_page_idx]. It doesn't matter, we can deal with
             both cases the same way. *)
          incr folder.stats.was_previous;
          IO.load_page folder.io left_page_idx (Revbuffer.ingest folder.buf))
        else
          (* 4 - If the entry spans on 3 pages, we might have a suffix of it in
             buffer, nerver mind, let's discard everything in the buffer. *)
          blindfolded_load_entry_in_buf folder left_offset

  (* TODO: Decode blob *)
  (* TODO: Add commit *)
  type entry = {
    offset : int63;
    length : int;
    v : [ `Contents | `Corrupted_inode of Inode.Val.t ];
    preds : int63 list;
  }

  let decode_entry folder offset =
    let length = decode_entry_length folder offset in
    Revbuffer.read ~mark_dirty:true folder.buf offset @@ fun buf i0 ->
    let available_bytes = String.length buf - i0 in
    assert (available_bytes >= length);
    let imagic = i0 + Hash.hash_size in
    let kind = Kind.of_magic_exn buf.[imagic] in
    match kind with
    | Inode_v1_unstable | Inode_v1_stable | Commit_v1 | Commit_v2 ->
        Fmt.failwith "unhandled %a" pp_kind kind
    | Contents -> { offset; length; v = `Contents; preds = [] }
    | Inode_v2_root | Inode_v2_nonroot ->
        let v_with_corrupted_keys, preds = folder.decode_inode buf i0 in
        { offset; length; v = `Corrupted_inode v_with_corrupted_keys; preds }

  let rec traverse folder f acc =
    if Pq.is_empty folder.pq then acc
    else
      let offset, _truc = Pq.pop_exn folder.pq in
      ensure_entry_is_in_buf folder offset;
      let entry = decode_entry folder offset in
      List.iter (fun offset -> Pq.push folder.pq offset 42) entry.preds;
      traverse folder f (f acc entry)

  let fold path max_offsets f acc =
    (match Conf.contents_length_header with
    | None ->
        failwith "Traverse can't work with Contents not prefixed by length"
    | Some _ -> ());
    let io = IO.v (Filename.concat path "store.pack") in
    let pq = Pq.create () in
    let buf = Revbuffer.create ~capacity:buffer_capacity in
    List.iter (fun o -> Pq.push pq o 42) max_offsets;
    let decode_inode =
      let preds = ref [] in
      let dict =
        let open Irmin_pack.Dict in
        v ~fresh:false ~readonly:true path |> find
      in
      let key_of_offset offset =
        preds := offset :: !preds;
        (* Generating a corrupted key *)
        (* TODO: Put fake hash *)
        Irmin_pack.Pack_key.v_direct ~hash:(Obj.magic ()) ~length:0 ~offset
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
        let l = !preds in
        preds := [];
        (v_with_corrupted_keys, l)
    in
    let stats = fresh_stats () in
    let folder = { buf; io; pq; decode_inode; stats } in
    traverse folder f acc
end
