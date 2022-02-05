type t = {
  mutable buf : bytes;
  mutable occupied : int;
  mutable right_offset : int;
}

let create start_capa right_offset =
  { buf = Bytes.create start_capa; occupied = 0; right_offset }

let reset t right_offset =
  t.occupied <- 0;
  t.right_offset <- right_offset

let capacity { buf; _ } = Bytes.length buf

let grow t min_new_capacity =
  let old_capacity = capacity t in
  let new_capacity = max (old_capacity * 2) min_new_capacity in
  let old_buf = t.buf in
  let new_buf = Bytes.create new_capacity in
  let old_idx = old_capacity - t.occupied in
  let new_idx = new_capacity - t.occupied in
  Bytes.blit old_buf old_idx new_buf new_idx t.occupied;
  t.buf <- new_buf

let ingest t byte_count f =
  let old_occupied = t.occupied in
  let new_occupied = old_occupied + byte_count in
  if new_occupied > capacity t then grow t new_occupied;
  f t.buf (capacity t - new_occupied);
  t.occupied <- new_occupied

let read t offset length f =
  let right_offset = t.right_offset in
  let left_offset = right_offset - t.occupied in
  if offset < left_offset || offset + length > right_offset then
    Fmt.failwith
      "Attempting out of bounds read in revbuffer. \n\
      \   buf: capa:%#11d occupied:%#11d \n\
      \   buf: left:%#11d right:%#11d\n\
      \ asked: left:%#11d right:%#11d (len:%d)" (capacity t) t.occupied
      left_offset right_offset offset (offset + length) length;
  let dist_from_left = offset - left_offset in
  let idx = capacity t - t.occupied + dist_from_left in
  f (Bytes.unsafe_to_string t.buf) idx

let test () =
  let read_fail a b c =
    let failed =
      try
        read a b c (fun _buf _idx -> ());
        false
      with Failure _ -> true
    in
    if not failed then failwith "Failed to raise"
  in
  Fmt.epr "\n%!";
  let t = create 10 1000 in
  assert (capacity t = 10);

  (* On empty buffer *)
  read t 1000 0 (fun _buf idx -> assert (idx = 10));
  read_fail t 1001 0;
  read_fail t 999 0;
  read_fail t 1000 1;

  ingest t 2 (fun buf idx ->
      assert (idx = 8);
      for i = idx to idx + 1 do
        Bytes.set_uint8 buf i 42
      done);
  ingest t 2 (fun buf idx ->
      assert (idx = 6);
      for i = idx to idx + 1 do
        Bytes.set_uint8 buf i 43
      done);
  assert (capacity t = 10);

  (* On buffer of size 4 *)
  read_fail t 1001 0;
  read_fail t 995 0;
  read t 996 4 (fun buf idx ->
      assert (idx = 6);
      assert (String.sub buf idx 4 = "++**"));
  read t 997 2 (fun buf idx ->
      assert (idx = 7);
      assert (String.sub buf idx 2 = "+*"));

  ingest t 17 (fun buf idx ->
      assert (idx = 0);
      for i = idx to idx + 16 do
        Bytes.set_uint8 buf i 44
      done);
  assert (capacity t = 21);

  (* On buffer of size 21 *)
  read t 979 21 (fun buf idx ->
      assert (idx = 0);
      assert (String.sub buf idx 21 = ",,,,,,,,,,,,,,,,,++**"));

  Fmt.epr "👍 Passed all revbuffer tests\n%!"

let () = test ()

(* *)
