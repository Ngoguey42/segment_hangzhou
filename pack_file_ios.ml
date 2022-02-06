module Int63 = Optint.Int63

type int63 = Int63.t

let ( ++ ) = Int63.add
let ( -- ) = Int63.sub

(** Physical offset world *)
let really_read_physical_offsets ~fd ~fd_offset ~buffer ~buffer_offset ~length =
  let rec aux fd_offset buffer_offset length =
    let r =
      Index_unix.Syscalls.pread ~fd ~fd_offset ~buffer ~buffer_offset ~length
    in
    if r = 0 then buffer_offset (* end of file *)
    else if r = length then buffer_offset + r
    else
      (aux [@tailcall])
        (fd_offset ++ Int63.of_int r)
        (buffer_offset + r) (length - r)
  in
  aux fd_offset buffer_offset length

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
  let page_len = Int63.of_int 4096
  let virt_of_phy v = v -- header_size
  let phy_of_virt v = v ++ header_size

  let page_idx_of_offset virt_off =
    let phy_off = phy_of_virt virt_off in
    Int63.(div phy_off page_len)

  let start_offset_of_page_idx page_idx =
    let open Int63 in
    let open Infix in
    let phy_off = page_idx * page_len in
    virt_of_phy phy_off

  let bytes_remaning_in_page_from_offset virt_off =
    let phy_off = phy_of_virt virt_off in
    let page_idx = page_idx_of_offset virt_off in
    let open Int63 in
    let open Infix in
    (succ page_idx * page_len) - phy_off

  (** i.e. first offset of the following page, or t.right_offset *)
  let end_offset_of_page_idx t page_idx =
    let virt_maxoff = t.right_offset in
    let open Int63 in
    let open Infix in
    let virt_endoff = virt_of_phy (succ page_idx * page_len) in
    if Int63.compare virt_endoff virt_maxoff <= 0 then virt_endoff
    else virt_maxoff
end

let really_read_virtual_offsets ~fd_offset =
  let fd_offset = Arithmetic.phy_of_virt fd_offset in
  really_read_physical_offsets ~fd_offset

let page_idx_of_offset = Arithmetic.page_idx_of_offset

let load_pages t first_page_idx last_page_idx
    (f : int -> (bytes -> int -> unit) -> unit) =
  assert (first_page_idx <= last_page_idx);
  let left_offset = Arithmetic.start_offset_of_page_idx first_page_idx in
  let right_offset = Arithmetic.end_offset_of_page_idx t last_page_idx in
  assert (right_offset <= t.right_offset);
  let length = right_offset -- left_offset |> Int63.to_int in
  f length @@ fun buffer buffer_offset ->
  let bytes_read =
    really_read_virtual_offsets ~fd:t.fd ~fd_offset:left_offset ~buffer
      ~buffer_offset ~length
  in
  assert (bytes_read = length);
  ()

(* o *)
