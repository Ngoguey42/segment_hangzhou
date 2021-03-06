(** Traverse

    {2 Naming of Ranges}

    Throughout the code [left] and [right] are the names used to designate
    classic ranges where [left] is the beginning of a range and [right] is
    [left + length].

    Throughout the code [first] and [last] are the names used to designate
    non-empty ranges where [first] is the index of the first element of the
    range and [last] is [first + length - 1].

    Finally, ranges may alternatively be designated under [left, length] or
    [first, length]. *)

let buffer_capacity = 4096 * 4096

(** The optimal [expected_entry_size] minimises
    [stats.blindfolded_too_much + stats.blindfolded_not_enough].

    [50] showed good results experimentally. *)
let expected_entry_size = 50

open Import
module IO = Pack_file_ios
module Kind = Irmin_pack.Pack_value.Kind

type kind = Kind.t [@@deriving repr ~pp]

module Pq = Priority_queue.Make (struct
  type t = int63

  let compare = Int63.compare
end)

(* TODO: Reuse the one from pack_store *)
module Varint = struct
  type t = int [@@deriving repr ~decode_bin]

  let min_encoded_size = 1

  (** LEB128 stores 7 bits per byte. An OCaml [int] has at most 63 bits.
      [63 / 7] equals [9]. *)
  let max_encoded_size = 9
end

(* TODO: Change the params so that it can be instanciated from ext or pack_store *)
module Make (Conf : Irmin_pack.Conf.S) (Schema : Irmin.Schema.Extended) = struct
  module Maker = Irmin_pack.Maker (Conf)
  module Store = Maker.Make (Schema)
  module Hash = Store.Hash
  module Key = Irmin_pack.Pack_key.Make (Hash)

  type hash = Store.hash [@@deriving repr ~pp]
  type key = Key.t [@@deriving repr ~pp]
  type 'payload predecessors = (int63 * 'payload) list

  module Inode = struct
    module Value = Schema.Node (Key) (Key)
    include Irmin_pack.Inode.Make_internal (Conf) (Hash) (Key) (Value)

    type compress = Compress.t [@@deriving repr ~decode_bin]
  end

  type 'pl folder = {
    pq : 'pl Pq.t;
    buf : Revbuffer.t;
    io : IO.t;
    merge_payloads : int63 -> older:'pl -> newer:'pl -> 'pl;
    timings : Timings.t;
    stats : Stats.t;
  }

  type 'pl entry = {
    offset : int63;
    length : int;
    v : [ `Contents | `Inode of Inode.compress ];
    payload : 'pl;
  }

  let min_bytes_needed_to_discover_length =
    Hash.hash_size + 1 + Varint.min_encoded_size

  let max_bytes_needed_to_discover_length =
    Hash.hash_size + 1 + Varint.max_encoded_size

  (** Read the length of an entry without tagging the read in [buf]. *)
  let decode_entry_length folder offset =
    Timings.(with_section folder.timings Decode_length) @@ fun () ->
    Revbuffer.read folder.buf offset @@ fun buf i0 ->
    let available_bytes = String.length buf - i0 in
    assert (available_bytes >= min_bytes_needed_to_discover_length);
    let ilength = i0 + Hash.hash_size + 1 in
    let pos_ref = ref ilength in
    let suffix_length = Varint.decode_bin buf pos_ref in
    let length_length = !pos_ref - ilength in
    (Hash.hash_size + 1 + length_length + suffix_length, 0)

  (** Reset [folder.buf] and load the entry at [left_offset] without knowing its
      length in advance.

      Doing this with v2 entries is pretty easy as the length is stored in the
      beginning of the entry. *)
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
      (* We are very unlucky. We missed loading enough at the first read. Let's
         start over without reusing stuff from the first read (it is easier). *)
      incr folder.stats.blindfolded_not_enough;
      let page_range_right_offset =
        IO.right_offset_of_page_idx folder.io actual_page_range.last
      in
      Revbuffer.reset folder.buf page_range_right_offset;
      IO.load_pages folder.io actual_page_range (Revbuffer.ingest folder.buf))
    else if actual_page_range.last < guessed_page_range.last then
      (* We are a bit unlucky. We loaded too much. *)
      incr folder.stats.blindfolded_too_much
    else
      (* We are lucky. We loaded just what was needed. *)
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
        if left_page_idx > first_loaded_page_idx then assert false
        else if left_page_idx = first_loaded_page_idx then
          (* 2 - We have already loaded all the needed pages *)
          incr folder.stats.hit
        else if left_page_idx = first_loaded_page_idx - 1 then (
          (* 3 - The beginning of the entry is in the next page on the left. We
             don't know if it is totally contained in that left page or if it
             also spans on [first_loaded_page_idx]. It doesn't matter, we can
             deal with both cases the same way.

             If [folder.buf] is almost full, it will either trigger a soft blit
             if we are not currently reading a very long chunk or a hard blit
             (calling [on_chunk] in the process). *)
          incr folder.stats.was_previous;
          IO.load_page folder.io left_page_idx (Revbuffer.ingest folder.buf))
        else
          (* 4 - If the entry spans on 3 pages, we might already have a suffix
             of it in buffer, nerver mind, let's discard everything in the
             buffer. *)
          blindfolded_load_entry_in_buf folder left_offset

  let decode_entry folder offset length =
    Revbuffer.read folder.buf offset @@ fun buf i0 ->
    let available_bytes = String.length buf - i0 in
    assert (available_bytes >= length);
    let imagic = i0 + Hash.hash_size in
    let kind = Kind.of_magic_exn buf.[imagic] in
    match kind with
    | Inode_v1_unstable | Inode_v1_stable | Commit_v1 ->
        (* TODO: Handle v1 entries *)
        Fmt.failwith "unhandled %a" pp_kind kind
    | Commit_v2 ->
        (* TODO: decode commit v2 *)
        Fmt.failwith "unhandled %a" pp_kind kind
    | Contents ->
        (* TODO: decode blobs with timings, only if user wishes to
           (use GADT conf) *)
        (* TODO: Maybe a mode where we return key of contents and
           another where we return completely decoded contents. *)
        (`Contents, length)
    | Inode_v2_root | Inode_v2_nonroot ->
        let v =
          Timings.(with_section folder.timings Decode_inode) @@ fun () ->
          Inode.decode_bin_compress buf (ref i0)
        in
        (`Inode v, length)

  let push_entry pq merge_payloads off newer =
    Pq.update pq off (function
      | None -> newer
      | Some older -> merge_payloads off ~older ~newer)

  let rec traverse folder on_entry =
    if Pq.is_empty folder.pq then (
      (* reset buffer to trigger a last [on_chunk] *)
      Revbuffer.reset folder.buf Int63.zero;

      (* TODO: Use log debug *)
      Fmt.epr "%a\n%!" Stats.pp folder.stats;
      Fmt.epr "%a\n%!" Timings.pp folder.timings)
    else
      let offset, payload =
        Timings.(with_section folder.timings Priority_queue) @@ fun () ->
        Pq.pop_exn folder.pq
      in
      ensure_entry_is_in_buf folder offset;
      let length = decode_entry_length folder offset in
      let v = decode_entry folder offset length in
      let entry = { offset; length; v; payload } in
      let preds = on_entry entry in
      let () =
        Timings.(with_section folder.timings Priority_queue) @@ fun () ->
        List.iter
          (fun (off, payload) ->
            if Int63.(off >= offset) then
              failwith "Precedessors should be to the left of their parent";
            push_entry folder.pq folder.merge_payloads off payload)
          preds
      in
      folder.stats.peak_pq := max !(folder.stats.peak_pq) (Pq.length folder.pq);
      traverse folder on_entry

  (* TODO: A clean mli with re-exported types from sub files *)
  let fold :
      string ->
      'pl predecessors ->
      merge_payloads:(int63 -> older:'pl -> newer:'pl -> 'pl) ->
      on_entry:('a -> 'pl entry -> 'a * 'pl predecessors) ->
      on_chunk:('a -> Revbuffer.chunk -> 'a) ->
      on_read:('a -> IO.page_range -> 'a) ->
      'a ->
      'a =
   fun path max_offsets ~merge_payloads ~on_entry ~on_chunk ~on_read acc ->
    (* TODO: Change [path] to [io] *)
    (match Conf.contents_length_header with
    | None ->
        failwith "Traverse can't work with Contents not prefixed by length"
    | Some _ -> ());
    let timings = Timings.(v Overhead) in
    let stats = Stats.v () in

    let acc = ref acc in
    let on_chunk (x : Revbuffer.chunk) =
      Timings.(with_section timings Callbacks) @@ fun () ->
      acc := on_chunk !acc x
    in
    let on_entry (x : 'pl entry) =
      Timings.(with_section timings Callbacks) @@ fun () ->
      let acc', preds = on_entry !acc x in
      acc := acc';
      preds
    in
    let on_read (x : IO.page_range) =
      Timings.(with_section timings Callbacks) @@ fun () ->
      acc := on_read !acc x
    in

    let pq = Pq.create () in
    let push_entry = push_entry pq merge_payloads in
    List.iter (fun (off, payload) -> push_entry off payload) max_offsets;
    stats.peak_pq := Pq.length pq;

    (* The choice for [right_offset] here is not important as the buffer will
       get reset for the first entry *)
    let buf =
      Revbuffer.create ~on_chunk ~capacity:buffer_capacity
        ~right_offset:Int63.zero ~timings ~stats
    in

    let io = IO.v ~on_read (Filename.concat path "store.pack") timings in
    traverse { buf; io; pq; stats; timings; merge_payloads } on_entry;
    !acc
end
