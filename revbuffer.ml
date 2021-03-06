(** A read buffer optimised for traversing a file by jumping from offset to
    decreasing offset.

    TODO: Rewrite all the doc, maybe move to [create]

    Features:

    - A fixed size buffer
    - Only supports insertions of chunks from right to left.
    - The insertion API is
      [(t * byte_count) -> (bytes * where_to_write_in_bytes)].
    - Keeps a virtual offset.
    - The read API is
      [(t * offset_to_read) -> (string * where_to_read_in_string)].
    - Forces reads to occur in a strictly decreasing order.
    - When inserting would make the buffer full, automatically discard data
      already read.

    See how the data structure behaves concretely in this example:

    {v
    # First create an empty revbuffer
    > create ~capacity:6 ~right_offset:100
    buffer state " ?  ?  ?  ?  ?  ? "
    readable       y  y  y  y  y  y
    offsets        94 95 96 97 98 99

    # Push 2 bytes
    > ingest "aa"
    buffer state " ?  ?  ?  ?  a  a "
    readable       y  y  y  y  y  y
    offsets        94 95 96 97 98 99

    # Push 3 bytes
    > ingest "bbb"
    buffer state " ?  b  b  b  a  a "
    readable       y  y  y  y  y  y
    offsets        94 95 96 97 98 99

    # Reading the last 2. It wont be possible to read these again in the future.
    # Attempting to read the first byte would result in an exception.
    > read ~offset:98
    "aa"
    buffer state " ?  b  b  b  a  a "
    readable       y  y  y  y  n  n
    offsets        94 95 96 97 98 99

    # Pushing 2 bytes, which necessitates a discard of bytes read. Pushing 4
    # bytes instead of 2 would have resulted in an exception.
    > ingest "cc"
    buffer state " ?  c  c  b  b  b "
    readable       y  y  y  y  y  y
    offsets        92 93 94 95 96 97

    # Reading the last 4.
    > read ~offset:94
    "cb"
    buffer state " ?  c  c  b  b  b "
    readable       y  y  n  n  n  n
    offsets        92 93 94 95 96 97
    v} *)

open Import

type int63 = Int63.t [@@deriving repr]
type current_chunk = { left : int63; right : int63 }

type emission_reason = [ `Reset | `Witnessed_discontinuity | `Out_of_space ]
[@@deriving repr ~pp]

type chunk = {
  buf : string;
  i : int;
  length : int;
  offset : int63;
  emission_reason : emission_reason;
}
[@@deriving repr]
(** A chunk of consecutive bytes that were read by the user. It is emited to the
    user through [on_chunk].

    [buf] is the buffer containg the bytes.

    [i] is the starting index in [buf] of the chunk.

    [length] is the length of the chunk starting at [i].

    [offset] is the virtual offset of [i].

    [break_type] informs of the reason for the emission of the chunk.

    - [`Reset] if it was emited during a [reset].
    - [`Witnessed_discontinuity] if it was emited during a [read] that jumped
      away from the a [current_chunk].
    - [`Out_of_space] in case of early emission caused by an [ingest] on a full
      buffer (i.e. [Revbuffer] can't emit chunks larger than the size of [buf])
      (i.e. the hard blit case). *)

let pp_chunk ppf c =
  let right = Int63.add_distance c.offset c.length in
  Format.fprintf ppf "i:%9d, len:%9d, left:%#14d, right:%#14d, reason:%a" c.i c.length
    (Int63.to_int c.offset) (Int63.to_int right) pp_emission_reason c.emission_reason

let chunk_t =
  Repr.like chunk_t ~pp:pp_chunk

type t = {
  buf : bytes;
  mutable occupied : int;
  mutable right_offset : int63;
  mutable current_chunk : current_chunk option;
  on_chunk : chunk -> unit;
  timings : Timings.t;
  stats : Stats.t;
}

(** Hello

    {2 Capacity}

    [capacity] is the maximum number of bytes from disk to keep in memory at
    once.

    A lower value uses less memory.

    A higher value minimises the number of [blit] and the number of calls to
    [on_chunk] with [emission_reason = `Out_of_space].

    If an entry ever has a size over [buffer_capacity], the program will crash.

    {2 Virtual offsets}

    {2 Read chunks} *)
let create ~on_chunk ~capacity ~right_offset ~timings ~stats =
  {
    buf = Bytes.create capacity;
    occupied = 0;
    right_offset;
    current_chunk = None;
    on_chunk;
    timings;
    stats;
  }

let capacity { buf; _ } = Bytes.length buf [@@inline always]

let left_offset t = Int63.sub_distance t.right_offset t.occupied
  [@@inline always]

let emit_chunk emission_reason t c =
  let dist_from_left = Int63.distance_exn ~lo:(left_offset t) ~hi:c.left in
  t.on_chunk
    {
      buf = Bytes.unsafe_to_string t.buf;
      i = capacity t - t.occupied + dist_from_left;
      length = Int63.distance_exn ~lo:c.left ~hi:c.right;
      offset = c.left;
      emission_reason;
    }

let reset t right_offset =
  Option.iter (emit_chunk `Reset t) t.current_chunk;
  t.occupied <- 0;
  t.right_offset <- right_offset;
  t.current_chunk <- None

let show t =
  Fmt.epr
    "     buf:        capa:%#14d\n\
    \     buf:    occupied:%#14d\n\
    \     buf:        left:%#14d\n\
    \     buf:  chunk.left:%#14d\n\
    \     buf: chunk.right:%#14d\n\
    \     buf:       right:%#14d\n\
     %!"
    (capacity t) t.occupied
    (Int63.to_int (left_offset t))
    (match t.current_chunk with None -> -1 | Some c -> Int63.to_int c.left)
    (match t.current_chunk with None -> -1 | Some c -> Int63.to_int c.right)
    (Int63.to_int t.right_offset)

let first_offset_opt t =
  if t.occupied = 0 then None
  else Some (Int63.sub_distance t.right_offset t.occupied)

let test_invariants t =
  assert (t.occupied <= capacity t);
  match t.current_chunk with
  | None -> ()
  | Some c ->
      assert (Int63.(left_offset t <= c.left));
      assert (Int63.(c.left < c.right));
      assert (Int63.(c.right <= t.right_offset))

let perform_blit t unfreeable_bytes new_right_offset =
  let old_left_idx = capacity t - t.occupied in
  let new_left_idx = capacity t - unfreeable_bytes in
  Timings.(with_section t.timings Blit) @@ fun () ->
  Bytes.blit t.buf old_left_idx t.buf new_left_idx unfreeable_bytes;
  t.right_offset <- new_right_offset;
  t.occupied <- unfreeable_bytes

let blit t missing_bytes byte_count =
  assert (missing_bytes > 0);
  match t.current_chunk with
  | None ->
      (* 1. Discard everything *)
      t.occupied <- 0
  | Some c ->
      (* 2. Try to blit by preserving [c] in buf *)
      let unfreeable_bytes =
        Int63.distance_exn ~lo:(left_offset t) ~hi:c.right
      in
      let freeable_bytes = Int63.distance_exn ~lo:c.right ~hi:t.right_offset in
      assert (freeable_bytes >= 0);
      assert (unfreeable_bytes >= 0);
      assert (freeable_bytes + unfreeable_bytes = t.occupied);
      if freeable_bytes >= missing_bytes then (
        incr t.stats.soft_blit;
        t.stats.soft_blit_bytes := !(t.stats.soft_blit_bytes) + unfreeable_bytes;
        perform_blit t unfreeable_bytes c.right)
      else
        (* 3. Try to blit by ejecting [c] from buf *)
        let unfreeable_bytes =
          Int63.distance_exn ~lo:(left_offset t) ~hi:c.left
        in
        let freeable_bytes = Int63.distance_exn ~lo:c.left ~hi:t.right_offset in
        assert (freeable_bytes >= 0);
        assert (unfreeable_bytes >= 0);
        assert (freeable_bytes + unfreeable_bytes = t.occupied);
        if freeable_bytes >= missing_bytes then (
          emit_chunk `Out_of_space t c;
          t.current_chunk <- None;
          incr t.stats.hard_blit;
          t.stats.hard_blit_bytes :=
            !(t.stats.hard_blit_bytes) + unfreeable_bytes;
          perform_blit t unfreeable_bytes c.left)
        else
          (* 4 - fail. We are trying to ingest more bytes than [buf] can
             contain (while keeping the few unfreeable_bytes left). Instead of
             crashing we could growp [buf]. *)
          Fmt.failwith
            "Failed revbuffer ingestion.\n\
            \     buf:        capa:%#14d\n\
            \     buf:    occupied:%#14d\n\
            \     buf:        left:%#14d\n\
            \     buf:  chunk.left:%#14d\n\
            \     buf: chunk.right:%#14d\n\
            \     buf:       right:%#14d\n\
            \ pushing:       bytes:%#14d\n\
            \ pushing:     missing:%#14d\n\
            \ pushing:    freeable:%#14d\n\
            \ pushing:  unfreeable:%#14d" (capacity t) t.occupied
            (Int63.to_int (left_offset t))
            (Int63.to_int c.left) (Int63.to_int c.right)
            (Int63.to_int t.right_offset)
            byte_count missing_bytes freeable_bytes unfreeable_bytes

let ingest : t -> int -> (bytes -> int -> unit) -> unit =
 fun t byte_count f ->
  test_invariants t;
  let old_occupied = t.occupied in
  let new_occupied = old_occupied + byte_count in
  if new_occupied > capacity t then (
    let missing_bytes = new_occupied - capacity t in
    blit t missing_bytes byte_count;
    test_invariants t);
  let old_occupied = t.occupied in
  let new_occupied = old_occupied + byte_count in
  let i = capacity t - new_occupied in
  f t.buf i;
  t.occupied <- new_occupied;
  test_invariants t

let read : t -> int63 -> (string -> int -> 'a * int) -> 'a =
 fun t offset f ->
  test_invariants t;
  let overshoot_left = Int63.(offset < left_offset t) in
  let overshoot_right = Int63.(offset > t.right_offset) in
  if overshoot_left || overshoot_right then
    Fmt.failwith
      "Illegal read attempt in revbuffer\n\
      \     buf:        capa:%#14d\n\
      \     buf:    occupied:%#14d\n\
      \     buf:        left:%#14d\n\
      \     buf:  chunk.left:%#14d\n\
      \     buf: chunk.right:%#14d\n\
      \     buf:       right:%#14d\n\
      \   asked:        left:%#14d" (capacity t) t.occupied
      (Int63.to_int (left_offset t))
      (match t.current_chunk with None -> -1 | Some c -> Int63.to_int c.left)
      (match t.current_chunk with None -> -1 | Some c -> Int63.to_int c.right)
      (Int63.to_int t.right_offset)
      (Int63.to_int offset);
  let dist_from_left = Int63.distance_exn ~lo:(left_offset t) ~hi:offset in
  let i = capacity t - t.occupied + dist_from_left in
  let res, bytes_read = f (Bytes.unsafe_to_string t.buf) i in
  if bytes_read < 0 then assert false
  else if bytes_read = 0 then (* User doesn't want to signal this read *)
    res
  else
    (* Mark dirty *)
    let right_read_offset = Int63.add_distance offset bytes_read in
    match t.current_chunk with
    | None ->
        if not Int63.(right_read_offset <= t.right_offset) then
          failwith "Wrong bytes_read";
        t.current_chunk <- Some { left = offset; right = right_read_offset };
        res
    | Some c ->
        if not Int63.(right_read_offset <= c.right) then
          failwith "Wrong bytes_read";
        if Int63.(right_read_offset > c.left) then failwith "Wrong bytes_read"
        else if Int63.(right_read_offset = c.left) then
          (* Continuity of chunk *)
          t.current_chunk <- Some { left = offset; right = c.right }
        else (
          (* Discontinuity of previous chunk *)
          emit_chunk `Witnessed_discontinuity t c;
          t.current_chunk <- Some { left = offset; right = right_read_offset });
        test_invariants t;
        res

let test () =
  let to63 = Int63.of_int in
  let read_fail ?(len = 0) t offset =
    let failed =
      try
        read t offset (fun _buf _idx -> ((), len));
        false
      with Failure _ -> true
    in
    if not failed then failwith "Failed to raise"
  in
  let ingest_fail a b =
    let failed =
      try
        ingest a b (fun _buf _idx -> ());
        false
      with Failure _ -> true
    in
    if not failed then failwith "Failed to raise"
  in
  let check t left_offset current_chunk_opt right_offset ~blit =
    let left_offset = to63 left_offset in
    let right_offset = to63 right_offset in
    let current_chunk_opt =
      Option.bind current_chunk_opt (fun (left, right) ->
          let left = to63 left in
          let right = to63 right in
          Some { left; right })
    in
    let left_offset' = Int63.sub_distance right_offset t.occupied in
    let blit' = !(t.stats.soft_blit) + !(t.stats.hard_blit) in
    assert (
      (left_offset, current_chunk_opt, right_offset, blit)
      === (left_offset', t.current_chunk, t.right_offset, blit'))
  in
  let latest_chunk = ref None in
  let on_chunk c = latest_chunk := Some (c.i, c.length, c.offset) in
  let ingest ?chunk t count f =
    latest_chunk := None;
    let chunk = Option.bind chunk (fun (a, b, c) -> Some (a, b, to63 c)) in
    ingest t count f;
    assert (!latest_chunk === chunk)
  in
  let read ?chunk t o f =
    latest_chunk := None;
    let chunk = Option.bind chunk (fun (a, b, c) -> Some (a, b, to63 c)) in
    read t o f;
    assert (!latest_chunk === chunk)
  in
  let check_read ?(len = 0) exp_idx exp_string buf idx =
    let available_bytes = String.length buf - idx in
    assert (exp_idx = idx);
    assert (String.sub buf idx available_bytes === exp_string);
    ((), len)
  in

  let t =
    create ~capacity:10 ~right_offset:(to63 1000) ~on_chunk
      ~timings:Timings.(v Overhead)
      ~stats:(Stats.v ())
  in
  assert (capacity t = 10);

  (* On empty buffer *)
  read t (to63 1000) (check_read 10 "");
  read_fail t (to63 1000) ~len:1;
  read_fail t (to63 1001);
  read_fail t (to63 999);
  ingest t 0 (fun _buf idx -> assert (idx = 10));
  check t 1000 None 1000 ~blit:0;

  (* ingest 4 *)
  ingest t 2 (fun buf idx ->
      assert (idx = 8);
      for i = idx to idx + 1 do
        Bytes.set_uint8 buf i 42
      done);
  check t 998 None 1000 ~blit:0;
  ingest t 2 (fun buf idx ->
      assert (idx = 6);
      for i = idx to idx + 1 do
        Bytes.set_uint8 buf i 43
      done);
  check t 996 None 1000 ~blit:0;

  (* On buffer of size 4 *)
  read_fail t (to63 1001);
  read_fail t (to63 995);
  read t (to63 996) (check_read 6 "++**");
  check t 996 None 1000 ~blit:0;
  read_fail t (to63 998) ~len:3;
  read t (to63 998) (check_read ~len:2 8 "**");
  check t 996 (Some (998, 1000)) 1000 ~blit:0;
  read t (to63 998) (check_read ~len:0 8 "**");
  read_fail t (to63 998) ~len:1;

  (* ingest 6 *)
  ingest t 6 (fun buf idx ->
      assert (idx = 0);
      for i = idx to idx + 5 do
        Bytes.set_uint8 buf i 44
      done);
  check t 990 (Some (998, 1000)) 1000 ~blit:0;

  (* On buffer of size 10 *)
  read t (to63 990) (check_read ~len:0 0 ",,,,,,++**");
  read t (to63 992) (check_read ~len:2 2 ",,,,++**") ~chunk:(8, 2, 998);
  check t 990 (Some (992, 994)) 1000 ~blit:0;

  (* ingest 1 *)
  ingest t 1 (fun buf idx ->
      assert (idx = 5);
      for i = idx to idx do
        Bytes.set_uint8 buf i 45
      done);
  check t 989 (Some (992, 994)) 994 ~blit:1;

  (* On buffer of size 5 *)
  read t (to63 989) (check_read 5 "-,,,,");

  (* ingest 7 *)
  ingest t 7
    (fun buf idx ->
      assert (idx = 0);
      for i = idx to idx + 7 do
        Bytes.set_uint8 buf i 46
      done)
    ~chunk:(8, 2, 992);
  check t 982 None 992 ~blit:2;

  Fmt.epr "???? Passed all revbuffer tests\n%!"

let () = test ()
