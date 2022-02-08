(** A read buffer optimised for traversing a file by jumping from offset to
    decreasing offset.

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
type stats = { mutable blit_count : int }

type t = {
  buf : bytes;
  mutable occupied : int;
  mutable right_offset : int63;
  mutable read_offset : int63;
  stats : stats;
  mutable primed : bool;
}

let create ~capacity =
  {
    buf = Bytes.create capacity;
    occupied = 0;
    right_offset = Int63.zero;
    read_offset = Int63.zero;
    stats = { blit_count = 0 };
    primed = false;
  }

let reset t right_offset =
  t.occupied <- 0;
  t.primed <- true;
  t.right_offset <- right_offset;
  t.read_offset <- right_offset

let capacity { buf; _ } = Bytes.length buf

let show t =
  let right_offset = t.right_offset in
  let left_offset = Int63.sub_distance right_offset t.occupied in
  Fmt.epr
    "   Buf: %d/%d\n\
    \              left:%#14d\n\
    \              read:%#14d\n\
    \             right:%#14d\n\
     %!"
    t.occupied (capacity t) (Int63.to_int left_offset)
    (Int63.to_int t.read_offset)
    (Int63.to_int right_offset)

let first_offset_opt t =
  if t.occupied = 0 then None
  else Some (Int63.sub_distance t.right_offset t.occupied)

let test_invariants t =
  assert t.primed;
  assert (Int63.(t.right_offset >= t.read_offset));
  assert (t.occupied <= capacity t);
  assert (Int63.distance ~hi:t.right_offset ~lo:t.read_offset <= t.occupied)

let ingest : t -> int -> (bytes -> int -> unit) -> unit =
 fun t byte_count f ->
  test_invariants t;
  let old_occupied = t.occupied in
  let new_occupied = old_occupied + byte_count in
  if new_occupied > capacity t then (
    let missing_bytes = new_occupied - capacity t in
    assert (missing_bytes > 0);
    let freeable_bytes = Int63.distance ~hi:t.right_offset ~lo:t.read_offset in
    assert (freeable_bytes >= 0);
    let right_offset = t.right_offset in
    let left_offset = Int63.sub_distance right_offset t.occupied in
    let unfreeable_bytes = Int63.distance ~hi:t.read_offset ~lo:left_offset in
    assert (freeable_bytes + unfreeable_bytes = t.occupied);
    if freeable_bytes < missing_bytes then
      Fmt.failwith
        "Failed revbuffer ingestion. \n\
        \     buf: capa:%#14d occupied:%#14d \n\
        \     buf: left:%#14d right:%#14d\n\
        \     buf:                   read:%#14d\n\
        \ pushing: %d bytes, %d missing_bytes\n\
        \          %d freeable_bytes %d unfreeable_bytes" (capacity t)
        t.occupied (Int63.to_int left_offset)
        (Int63.to_int right_offset)
        (Int63.to_int t.read_offset)
        byte_count missing_bytes freeable_bytes unfreeable_bytes;
    let old_left_idx = capacity t - t.occupied in
    let new_left_idx = capacity t - unfreeable_bytes in
    Bytes.blit t.buf old_left_idx t.buf new_left_idx unfreeable_bytes;
    t.stats.blit_count <- t.stats.blit_count + 1;
    t.right_offset <- t.read_offset;
    t.occupied <- unfreeable_bytes;
    test_invariants t);
  let old_occupied = t.occupied in
  let new_occupied = old_occupied + byte_count in
  f t.buf (capacity t - new_occupied);
  (* only mutate [t] after successful [f] call *)
  t.occupied <- new_occupied;
  test_invariants t

let read : mark_dirty:bool -> t -> int63 -> (string -> int -> 'a) -> 'a =
 fun ~mark_dirty t offset f ->
  test_invariants t;
  let right_offset = t.right_offset in
  let left_offset = Int63.sub_distance right_offset t.occupied in
  let overshoot_left = Int63.(offset < left_offset) in
  let overshoot_right = Int63.(offset > right_offset) in
  if overshoot_left || overshoot_right then
    Fmt.failwith
      "Illegal read attempt in revbuffer. \n\
      \   buf: capa:%#14d occupied:%#14d \n\
      \   buf: left:%#14d right:%#14d\n\
      \   buf:                   read:%#14d\n\
      \ asked: left:%#14d " (capacity t) t.occupied (Int63.to_int left_offset)
      (Int63.to_int right_offset)
      (Int63.to_int t.read_offset)
      (Int63.to_int offset);
  let dist_from_left = Int63.distance ~hi:offset ~lo:left_offset in
  let idx = capacity t - t.occupied + dist_from_left in
  let res = f (Bytes.unsafe_to_string t.buf) idx in
  (* only mutate [t] after successful [f] call *)
  if mark_dirty then t.read_offset <- offset;
  test_invariants t;
  res

let test () =
  let to63 = Int63.of_int in
  let read = read ~mark_dirty:true in
  let read_fail a b =
    let failed =
      try
        read a b (fun _buf _idx -> ());
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
  let check_ints t occupied right_offset read_offset blit_count =
    let right_offset = to63 right_offset in
    let read_offset = to63 read_offset in
    if
      (occupied, right_offset, read_offset, blit_count)
      <<>> (t.occupied, t.right_offset, t.read_offset, t.stats.blit_count)
    then
      Fmt.failwith
        "occupied:%d/%d, right_offset:%d/%d, read_offset:%d/%d, \
         blit_count:%d/%d"
        t.occupied occupied
        (Int63.to_int t.right_offset)
        (Int63.to_int right_offset)
        (Int63.to_int t.read_offset)
        (Int63.to_int read_offset) t.stats.blit_count blit_count
  in
  let check_read exp_idx exp_string buf idx =
    let available_bytes = String.length buf - idx in
    assert (exp_idx = idx);
    assert (String.sub buf idx available_bytes === exp_string)
  in

  let t = create ~capacity:10 in
  reset t (to63 1000);
  assert (capacity t = 10);

  (* On empty buffer *)
  read t (to63 1000) (check_read 10 "");
  ingest t 0 (fun _buf idx -> assert (idx = 10));
  read_fail t (to63 1001);
  read_fail t (to63 999);
  ingest_fail t 11;
  check_ints t 0 1000 1000 0;

  (* ingest 4 *)
  ingest t 2 (fun buf idx ->
      assert (idx = 8);
      for i = idx to idx + 1 do
        Bytes.set_uint8 buf i 42
      done);
  check_ints t 2 1000 1000 0;
  ingest t 2 (fun buf idx ->
      assert (idx = 6);
      for i = idx to idx + 1 do
        Bytes.set_uint8 buf i 43
      done);
  check_ints t 4 1000 1000 0;

  (* On buffer of size 4 *)
  read_fail t (to63 1001);
  read_fail t (to63 995);
  read t (to63 996) (check_read 6 "++**");
  check_ints t 4 1000 996 0;

  (* ingest 6 *)
  ingest t 6 (fun buf idx ->
      assert (idx = 0);
      for i = idx to idx + 5 do
        Bytes.set_uint8 buf i 44
      done);
  check_ints t 10 1000 996 0;

  (* On buffer of size 10 *)
  read t (to63 990) (check_read 0 ",,,,,,++**");
  check_ints t 10 1000 990 0;

  (* ingest 1 *)
  ingest t 1 (fun buf idx ->
      assert (idx = 9);
      for i = idx to idx do
        Bytes.set_uint8 buf i 45
      done);
  check_ints t 1 990 990 1;

  (* On buffer of size 1 *)
  read t (to63 989) (check_read 9 "-");
  check_ints t 1 990 989 1;

  (* ingest 10 *)
  ingest t 10 (fun buf idx ->
      assert (idx = 0);
      for i = idx to idx + 9 do
        Bytes.set_uint8 buf i 46
      done);
  check_ints t 10 989 989 2;

  (* On buffer of size 10, again *)
  read t (to63 979) (check_read 0 "..........");
  check_ints t 10 989 979 2;

  Fmt.epr "👍 Passed all revbuffer tests\n%!"

let () = test ()
