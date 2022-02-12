(** Pack file reading and offset/page arithmetic.

    Uses [Index_unix.Private.Raw] and [Index_unix.Syscalls]. *)

open Import

(** Stolen from [Raw] and tuned a bit *)
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

type page_range = { first : int; last : int }

type t = {
  fd : Unix.file_descr;
  right_offset : int63;
  version : [ `V1 | `V2 ];
  timings : Timings.t;
  on_read : page_range -> unit;
}
(** [right_offset] is a virtual offset *)

module Raw = Index_unix.Private.Raw
module Version = Irmin_pack.Version

let v ~on_read path timings =
  (* TODO: log debug *)
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
  { fd; right_offset; version; timings; on_read }

(** Conversions between virtual and physical offsets *)
module Conversions = struct
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

  (** i.e. first offset of the following page, or t.right_offset *)
  let right_offset_of_page_idx : t -> int -> int63 =
   fun t page_idx ->
    let virt_maxoff = t.right_offset in
    let open Int63 in
    let virt_endoff = virt_of_phy ((of_int page_idx |> succ) * page_len) in
    min virt_endoff virt_maxoff

  let right_offset_of_page_idx_from_offset : t -> int63 -> int63 =
   fun t virt_off ->
    virt_off |> page_idx_of_offset |> right_offset_of_page_idx t
end

let really_read_virtual_offsets ~fd_offset =
  let fd_offset = Conversions.phy_of_virt fd_offset in
  really_read_physical_offsets ~fd_offset

let page_range_of_offset_and_guessed_length t offset guessed_length =
  assert (guessed_length > 0);
  let first_offset = offset in
  let right_offset =
    let open Int63 in
    min (add_distance offset guessed_length) t.right_offset
  in
  let last_offset = right_offset -- Int63.one in
  let first = Conversions.page_idx_of_offset first_offset in
  let last = Conversions.page_idx_of_offset last_offset in
  assert (first <= last);
  { first; last }

let page_range_of_offset_length offset length =
  assert (length > 0);
  let first_offset = offset in
  let last_offset = Int63.add_distance offset length -- Int63.one in
  let first = Conversions.page_idx_of_offset first_offset in
  let last = Conversions.page_idx_of_offset last_offset in
  assert (first <= last);
  { first; last }

let page_idx_of_offset = Conversions.page_idx_of_offset

let right_offset_of_page_idx_from_offset =
  Conversions.right_offset_of_page_idx_from_offset

let right_offset_of_page_idx = Conversions.right_offset_of_page_idx

let load_pages t ({ first; last } as range)
    (f : int -> (bytes -> int -> unit) -> unit) =
  t.on_read range;
  let left_offset = Conversions.left_offset_of_page_idx first in
  let right_offset = Conversions.right_offset_of_page_idx t last in
  assert (Int63.(right_offset <= t.right_offset));
  let length = Int63.distance_exn ~hi:right_offset ~lo:left_offset in
  f length @@ fun buffer buffer_offset ->
  let bytes_read =
    Timings.(with_section t.timings Read) @@ fun () ->
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
      (Int63.to_int right_offset)

let load_page t page_idx = load_pages t { first = page_idx; last = page_idx }
