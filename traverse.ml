(** Traverse *)

(** Maximum number of bytes from disk to keep in memory at once.

    A lower value uses less memory.

    A higher value minimises the number of [blit] in [buf].

    In an entry ever has a size over [buffer_capacity], the program will crash.

    Throughout the code [left] and [right] are the names used to designate
    classic ranges where [left] is the beginning of a range and [right] is
    [left + length].

    Throughout the code [first] and [last] are the names used to designate
    non-empty ranges where [first] is the index of the first element of the
    range and [last] is [first + length - 1]. *)
let buffer_capacity = 4096 * 100

(** The optimal [expected_entry_size] minimises
    [blindfolded_too_much + blindfolded_not_enough]. [50] showed good results
    experimentally. *)
let expected_entry_size = 50

open Import
module IO = Pack_file_ios
module Kind = Irmin_pack.Pack_value.Kind

type kind = Kind.t [@@deriving repr ~pp]

module Pq = Priority_queue.Make (struct
  type t = int63

  let compare = Int63.compare
end)

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

  module Timings = struct
    type section =
      | Read
      | Decode_inode
      | Decode_length
      | Priority_queue
      | Overhead
      | Callback
    [@@deriving repr ~pp]

    let all =
      [ Read; Decode_inode; Decode_length; Priority_queue; Overhead; Callback ]

    module M = Map.Make (struct
      type t = section

      let compare = compare
    end)

    type t = {
      mutable current_section : section;
      mutable totals : Mtime.Span.t M.t;
      mutable counter : Mtime_clock.counter;
    }

    let pp : t Repr.pp =
     fun ppf t ->
      let elapsed = Mtime_clock.count t.counter in
      let totals =
        M.update t.current_section
          (function
            | None -> assert false
            | Some so_far -> Some (Mtime.Span.add so_far elapsed))
          t.totals
      in
      let total_span =
        M.fold (fun _ -> Mtime.Span.add) totals Mtime.Span.zero
      in
      let total = Mtime.Span.to_s total_span in
      Format.fprintf ppf "{\"Total\":%a(100%%)," Mtime.Span.pp total_span;

      let pp_one ppf (section, elapsed) =
        Format.fprintf ppf "%a:%a(%.1f%%)" pp_section section Mtime.Span.pp
          elapsed
          (Mtime.Span.to_s elapsed /. total *. 100.)
      in
      Format.fprintf ppf "%a}"
        Fmt.(list ~sep:(any ",") pp_one)
        (totals |> M.to_seq |> List.of_seq)

    let v current_section =
      let totals =
        List.map (fun v -> (v, Mtime.Span.zero)) all |> List.to_seq |> M.of_seq
      in
      let counter = Mtime_clock.counter () in
      { totals; counter; current_section }

    let switch t next_section =
      (* TODO: Use [with_section] *)
      let elapsed = Mtime_clock.count t.counter in
      t.totals <-
        M.update t.current_section
          (function
            | None -> assert false
            | Some so_far -> Some (Mtime.Span.add so_far elapsed))
          t.totals;
      t.counter <- Mtime_clock.counter ();
      t.current_section <- next_section
  end

  (** [Chunk_watcher] calls [on_chunk] for all the consecutive read blocks
      encountered during the the traversal. *)
  module Chunk_watcher = struct
    type t = {
      mutable is_inside_chunk : bool;
      mutable left_offset : int63;
      mutable right_offset : int63;
      on_chunk : approximate:bool -> string -> i:int -> length:int -> offset:int63 -> unit;
    }

    let v on_chunk =
      {
        is_inside_chunk = false;
        left_offset = Int63.zero;
        right_offset = Int63.zero;
        on_chunk;
      }

    let notify_chunk ~approximate t (buf_cursor : Revbuffer.cursor) =
      assert (Int63.(buf_cursor.offset <= t.left_offset));
      let buf = buf_cursor.buf in
      let i =
        buf_cursor.i + Int63.distance ~lo:buf_cursor.offset ~hi:t.left_offset
      in
      let length = Int63.distance ~lo:t.left_offset ~hi:t.right_offset in
      let offset = t.left_offset in
      (* TODO: [on_chunk] changes acc *)
      (* TODO: watch timings on callback *)
      t.on_chunk ~approximate buf ~i ~offset ~length

    (** The function gets called for every single entry part of the traversal *)
    let on_entry t (buf_cursor : Revbuffer.cursor) entry_length =
      let entry_right_offset =
        Int63.add_distance buf_cursor.offset entry_length
      in
      if not t.is_inside_chunk then (
        (* 1 - Beginning of chunk *)
        t.is_inside_chunk <- true;
        t.left_offset <- buf_cursor.offset;
        t.right_offset <- entry_right_offset)
      else if Int63.(entry_right_offset > t.left_offset) then assert false
      else if Int63.(entry_right_offset = t.left_offset) then
        (* 2 - Continuity of chunk *)
        t.left_offset <- buf_cursor.offset
      else (
        (* 3 - End of chunk (exact) *)
        notify_chunk ~approximate:false t buf_cursor;

        (* 4 - Beginning of chunk *)
        t.left_offset <- buf_cursor.offset;
        t.right_offset <- entry_right_offset)

    (** This function gets called by [Revbuffer] every time it is about to erase
        old entries in its internal buffer on [reset], [ingest] or [clear]. *)
    let on_buffer_erase ~auto t (buf_cursor : Revbuffer.cursor) =
      if t.is_inside_chunk then (
        (* 4 - End of chunk (approximate) *)
        notify_chunk ~approximate:auto t buf_cursor;
        t.is_inside_chunk <- false)
  end

  type stats = {
    hit : int ref;
    blindfolded_perfect : int ref;
    blindfolded_too_much : int ref;
    blindfolded_not_enough : int ref;
    was_previous : int ref;
    peak_pq : int ref;
    buffer_shrinks : int ref;
  }
  [@@deriving repr ~pp]

  let fresh_stats () =
    {
      hit = ref 0;
      blindfolded_perfect = ref 0;
      blindfolded_too_much = ref 0;
      blindfolded_not_enough = ref 0;
      was_previous = ref 0;
      peak_pq = ref 0;
      buffer_shrinks = ref 0;
    }

  type 'pl folder = {
    pq : 'pl Pq.t;
    buf : Revbuffer.t;
    io : IO.t;
    merge_payloads : int63 -> older:'pl -> newer:'pl -> 'pl;
    stats : stats;
    timings : Timings.t;
        (* Can't put timings in stats because of repr bug on custom pp *)
    chunk_watcher : Chunk_watcher.t;
  }

  (* TODO: Decode blob *)
  (* TODO: Add commit *)
  type 'pl entry = {
    offset : int63;
    length : int;
    v : [ `Contents | `Inode of Inode.compress ];
    payload : 'pl;
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
    Timings.(switch folder.timings Decode_length);
    Revbuffer.read ~mark_dirty:false folder.buf offset @@ fun buf i0 ->
    let available_bytes = String.length buf - i0 in
    assert (available_bytes >= min_bytes_needed_to_discover_length);
    let ilength = i0 + Hash.hash_size + 1 in
    let pos_ref = ref ilength in
    let suffix_length = Varint.decode_bin buf pos_ref in
    let length_length = !pos_ref - ilength in
    Timings.(switch folder.timings Overhead);
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
    Timings.(switch folder.timings Read);
    IO.load_pages folder.io guessed_page_range (Revbuffer.ingest folder.buf);
    Timings.(switch folder.timings Overhead);
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
      Timings.(switch folder.timings Read);
      IO.load_pages folder.io actual_page_range (Revbuffer.ingest folder.buf);
      Timings.(switch folder.timings Overhead))
    else if actual_page_range.last < guessed_page_range.last then
      (* Loaded too much *)
      incr folder.stats.blindfolded_too_much
    else (* Loaded just what was needed *)
      incr folder.stats.blindfolded_perfect

  let ensure_entry_is_in_buf folder left_offset =
    (* TODO: Some way of notifying when contiguous blocks are going to be
       flushed. We only learn that an entry is the leftmost one of a chunk
       at the next iteration loop. *)
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
          Timings.(switch folder.timings Read);
          IO.load_page folder.io left_page_idx (Revbuffer.ingest folder.buf);
          Timings.(switch folder.timings Overhead))
        else
          (* 4 - If the entry spans on 3 pages, we might have a suffix of it in
             buffer, nerver mind, let's discard everything in the buffer. *)
          blindfolded_load_entry_in_buf folder left_offset

  let decode_entry folder offset length =
    Revbuffer.read ~mark_dirty:true folder.buf offset @@ fun buf i0 ->
    Chunk_watcher.on_entry folder.chunk_watcher
      Revbuffer.{ buf; i = i0; offset }
      length;
    let available_bytes = String.length buf - i0 in
    assert (available_bytes >= length);
    let imagic = i0 + Hash.hash_size in
    let kind = Kind.of_magic_exn buf.[imagic] in
    match kind with
    | Inode_v1_unstable | Inode_v1_stable | Commit_v1 | Commit_v2 ->
        Fmt.failwith "unhandled %a" pp_kind kind
    | Contents -> `Contents
    | Inode_v2_root | Inode_v2_nonroot ->
        Timings.(switch folder.timings Decode_inode);
        let v = Inode.decode_bin_compress buf (ref i0) in
        Timings.(switch folder.timings Overhead);
        `Inode v

  let push_entry pq merge_payloads off newer =
    Pq.update pq off (function
      | None -> newer
      | Some older -> merge_payloads off ~older ~newer)

  let rec traverse folder f acc =
    if Pq.is_empty folder.pq then (
      (* [clear] the revbuff so that [on_erase] gets calls one last time *)
      Revbuffer.clear folder.buf;
      Fmt.epr "%a\n%!" pp_stats folder.stats;
      Revbuffer.(Fmt.epr "%a\n%!" pp_stats folder.buf.stats);
      Fmt.epr "%a\n%!" Timings.pp folder.timings;
      acc)
    else (
      Timings.(switch folder.timings Priority_queue);
      let offset, payload = Pq.pop_exn folder.pq in
      Timings.(switch folder.timings Overhead);
      ensure_entry_is_in_buf folder offset;
      let length = decode_entry_length folder offset in
      let v = decode_entry folder offset length in
      let entry = { offset; length; v; payload } in
      Timings.(switch folder.timings Callback);
      let acc, predecessors = f acc entry in
      Timings.(switch folder.timings Overhead);
      Timings.(switch folder.timings Priority_queue);
      List.iter
        (fun (off, payload) ->
          if Int63.(off >= offset) then
            failwith "Precedessors should be to the left of their parent";
          push_entry folder.pq folder.merge_payloads off payload)
        predecessors;
      Timings.(switch folder.timings Overhead);
      folder.stats.peak_pq := max !(folder.stats.peak_pq) (Pq.length folder.pq);
      traverse folder f acc)

  let fold :
      string ->
      'pl predecessors ->
      (approximate:bool -> string -> i:int -> length:int -> offset:int63 -> unit) ->
      (int63 -> older:'pl -> newer:'pl -> 'pl) ->
      ('a -> 'pl entry -> 'a * 'pl predecessors) ->
      'a ->
      'a =
   fun path max_offsets on_chunk merge_payloads f acc ->
    (match Conf.contents_length_header with
    | None ->
        failwith "Traverse can't work with Contents not prefixed by length"
    | Some _ -> ());
    let io = IO.v (Filename.concat path "store.pack") in
    let stats = fresh_stats () in
    let timings = Timings.(v Overhead) in
    let chunk_watcher = Chunk_watcher.v on_chunk in

    let pq = Pq.create () in
    let push_entry = push_entry pq merge_payloads in
    List.iter (fun (off, payload) -> push_entry off payload) max_offsets;
    stats.peak_pq := Pq.length pq;

    let folder_ref = ref None in
    let on_erase ~auto buf_cursor =
      incr stats.buffer_shrinks;
      let folder = Option.get !folder_ref in
      Chunk_watcher.on_buffer_erase ~auto folder.chunk_watcher buf_cursor
    in
    let buf = Revbuffer.create ~capacity:buffer_capacity ~on_erase in

    let folder =
      { buf; io; pq; stats; timings; merge_payloads; chunk_watcher }
    in
    folder_ref := Some folder;
    traverse folder f acc
end
