open Import

let ( ++ ) = Int63.add
let ( -- ) = Int63.sub

(** Physical offset world *)
let really_read_physical_offsets ~fd ~buffer ~fd_offset ~buffer_offset ~length =
  let rec aux fd_offset buffer_offset length read_count =
    let r =
      Index_unix.Syscalls.pread ~fd ~fd_offset ~buffer ~buffer_offset ~length
    in
    let read_count = read_count + r in
    if r = 0 then read_count (* end of file *)
    else if r = length then read_count
    else
      (aux [@tailcall])
        (Int63.add_distance fd_offset r)
        (buffer_offset + r) (length - r) read_count
  in
  aux fd_offset buffer_offset length 0

type t = { fd : Unix.file_descr; right_offset : int63; version : [ `V1 | `V2 ] }
(** [right_offset] is a virtual offset *)

module Raw = Index_unix.Private.Raw
module Version = Irmin_pack.Version

let v path =
  Fmt.epr "Pack_file_ios: Opening %S\n%!" path;
  if not @@ Sys.file_exists path then failwith "Pack file doesn't exist";
  let fd = Unix.openfile path Unix.[ O_EXCL; O_RDONLY; O_CLOEXEC ] 0o644 in
  let raw = Raw.v fd in
  let right_offset = Raw.Offset.get raw in
  let version =
    let v_string = Raw.Version.get raw in
    match Version.of_bin v_string with
    | Some v -> v
    | None -> Version.invalid_arg v_string
  in
  { fd; right_offset; version }

(** Conversions between virtual and physical offsets *)
module Arithmetic = struct
  let header_size = (* offset + version *) Int63.of_int 16
  let page_len_int = 4096
  let page_len = Int63.of_int page_len_int
  let virt_of_phy v = v -- header_size
  let phy_of_virt v = v ++ header_size

  let page_idx_of_offset : int63 -> int =
   fun virt_off ->
    let phy_off = phy_of_virt virt_off in
    Int63.(div phy_off page_len |> to_int)

  let left_offset_of_page_idx : int -> int63 =
   fun page_idx ->
    let open Int63 in
    let phy_off = of_int page_idx * page_len in
    virt_of_phy phy_off

  let bytes_remaning_in_page_from_offset : int63 -> int =
   fun virt_off ->
    let phy_off = phy_of_virt virt_off in
    let page_idx = page_idx_of_offset virt_off in
    let open Int63 in
    ((of_int page_idx |> succ) * page_len) - phy_off |> to_int

  (** i.e. first offset of the following page, or t.right_offset *)
  let right_offset_of_page_idx : t -> int -> int63 =
   fun t page_idx ->
    let virt_maxoff = t.right_offset in
    let open Int63 in
    let open Infix in
    let virt_endoff = virt_of_phy ((of_int page_idx |> succ) * page_len) in
    if virt_endoff <= virt_maxoff then virt_endoff else virt_maxoff

  let right_offset_of_page_idx_from_offset : t -> int63 -> int63 =
   fun t virt_off ->
    virt_off |> page_idx_of_offset |> right_offset_of_page_idx t
end

(* TODO: Check all the arithmetics regarding [page_last] and [page_end] *)

let really_read_virtual_offsets ~fd_offset =
  let fd_offset = Arithmetic.phy_of_virt fd_offset in
  really_read_physical_offsets ~fd_offset

type page_range = { first : int; last : int }

let page_range_of_offset_and_guessed_length t offset guessed_length =
  assert (guessed_length > 0);
  let first_offset = offset in
  let right_offset =
    let open Int63 in
    min (add_distance offset guessed_length) t.right_offset
  in
  let last_offset = right_offset -- Int63.one in
  (* let last_offset = Int63.add_distance offset guessed_length -- Int63.one in *)
  let first = Arithmetic.page_idx_of_offset first_offset in
  let last = Arithmetic.page_idx_of_offset last_offset in
  assert (first <= last);
  { first; last }

let page_range_of_offset_length offset length =
  assert (length > 0);
  let first_offset = offset in
  let last_offset = Int63.add_distance offset length -- Int63.one in
  let first = Arithmetic.page_idx_of_offset first_offset in
  let last = Arithmetic.page_idx_of_offset last_offset in
  assert (first <= last);
  { first; last }

let page_idx_of_offset = Arithmetic.page_idx_of_offset

let right_offset_of_page_idx_from_offset =
  Arithmetic.right_offset_of_page_idx_from_offset

let right_offset_of_page_idx = Arithmetic.right_offset_of_page_idx

let load_page t page_idx (f : int -> (bytes -> int -> unit) -> unit) =
  let left_offset = Arithmetic.left_offset_of_page_idx page_idx in
  let right_offset = Arithmetic.right_offset_of_page_idx t page_idx in
  assert (Int63.(right_offset <= t.right_offset));
  let length = Int63.distance ~hi:right_offset ~lo:left_offset in
  f length @@ fun buffer buffer_offset ->
  let bytes_read =
    really_read_virtual_offsets ~fd:t.fd ~fd_offset:left_offset ~buffer
      ~buffer_offset ~length
  in
  if bytes_read <> length then
    Fmt.failwith
      "read failed. \n\
       buffer_offset:%#14d\n\
       buffer_length:%#14d\n\
      \   bytes-read:%#14d\n\
      \       length:%#14d\n\
      \  left_offset:%#14d\n\
      \ right_offset:%#14d" buffer_offset (Bytes.length buffer) bytes_read
      length (Int63.to_int left_offset)
      (Int63.to_int right_offset);
  Fmt.epr "IO: Loaded %d bytes for page %#d \n%!" length page_idx;
  (* Fmt.epr "%S\n%!" (String.sub (Bytes.unsafe_to_string buffer) buffer_offset length); *)
  ()

let load_pages t { first; last } (f : int -> (bytes -> int -> unit) -> unit) =
  let left_offset = Arithmetic.left_offset_of_page_idx first in
  let right_offset = Arithmetic.right_offset_of_page_idx t last in
  assert (Int63.(right_offset <= t.right_offset));
  let length = Int63.distance ~hi:right_offset ~lo:left_offset in
  f length @@ fun buffer buffer_offset ->
  let bytes_read =
    really_read_virtual_offsets ~fd:t.fd ~fd_offset:left_offset ~buffer
      ~buffer_offset ~length
  in
  if bytes_read <> length then
    Fmt.failwith
      "read failed. \n\
       buffer_offset:%#14d\n\
       buffer_length:%#14d\n\
      \   bytes-read:%#14d\n\
      \       length:%#14d\n\
      \  left_offset:%#14d\n\
      \ right_offset:%#14d" buffer_offset (Bytes.length buffer) bytes_read
      length (Int63.to_int left_offset)
      (Int63.to_int right_offset);
  Fmt.epr "IO: Loaded %d bytes for page_range %#d-%#d \n%!" length first last;
  (* Fmt.epr "%S\n%!" (String.sub (Bytes.unsafe_to_string buffer) buffer_offset length); *)
  ()

(* o *)
