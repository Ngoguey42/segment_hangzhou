(* Quick and dirty *)

open Import
module IO = Pack_file_ios
module Kind = Irmin_pack.Pack_value.Kind

type kind = Kind.t [@@deriving repr ~pp]

module Varint = struct
  type t = int [@@deriving repr ~decode_bin]

  let min_encoded_size = 1

  (** LEB128 stores 7 bits per byte. An OCaml [int] has at most 63 bits.
      [63 / 7] equals [9]. *)
  let max_encoded_size = 9
end

module Make (Conf : Irmin_pack.Conf.S) (Schema : Irmin.Schema.Extended) = struct
  module Maker = Irmin_pack.Maker (Conf)
  module Store = Maker.Make (Schema)
  module Hash = Store.Hash
  module Key = Irmin_pack.Pack_key.Make (Hash)

  module Inode = struct
    module Value = Schema.Node (Key) (Key)
    include Irmin_pack.Inode.Make_internal (Conf) (Hash) (Key) (Value)

    type compress = Compress.t [@@deriving repr ~decode_bin]
  end

  type hash = Store.hash [@@deriving repr ~pp]
  type key = Key.t [@@deriving repr ~pp]
  type entry = [ `Contents | `Inode of Inode.compress | `Commit ]

  let max_bytes_needed_to_discover_length =
    Hash.hash_size + 1 + Varint.max_encoded_size

  let decode_entry_header fd buffer offset =
    (* [min_bytes_needed_to_discover_length] might be more than what the file
       may contain *)
    let _read_count =
      IO.really_read_virtual_offsets ~fd ~buffer ~fd_offset:offset
        ~buffer_offset:0 ~length:max_bytes_needed_to_discover_length
    in
    let buffer = Bytes.unsafe_to_string buffer in
    let i0 = 0 in

    (* Fmt.epr "%128S  %S  \n%!" (String.sub buffer 0 32) (String.sub buffer 32 ); *)
    let imagic = i0 + Hash.hash_size in
    let kind = Kind.of_magic_exn buffer.[imagic] in

    let ilength = i0 + Hash.hash_size + 1 in
    let pos_ref = ref ilength in
    let suffix_length = Varint.decode_bin buffer pos_ref in
    let length_length = !pos_ref - ilength in

    (kind, Hash.hash_size + 1 + length_length + suffix_length)

  let rec traverse fd buffer on_entry acc off =
    let kind, length = decode_entry_header fd buffer off in
    let entry =
      match kind with
      | Inode_v1_unstable | Inode_v1_stable | Commit_v1 -> assert false
      | Commit_v2 -> `Commit
      | Contents -> `Contents
      | Inode_v2_root | Inode_v2_nonroot ->
          let _read_count =
            IO.really_read_virtual_offsets ~fd ~buffer ~fd_offset:off
              ~buffer_offset:0 ~length
          in
          let buffer = Bytes.unsafe_to_string buffer in
          `Inode (Inode.decode_bin_compress buffer (ref 0))
    in

    let acc, continue = on_entry acc off length entry in
    if continue then
      traverse fd buffer on_entry acc (Int63.add_distance off length)
    else acc

  let fold :
      string -> on_entry:('a -> int63 -> int -> entry -> 'a * bool) -> 'a -> 'a
      =
   fun path ~on_entry acc ->
    let fd =
      Unix.openfile
        (Filename.concat path "store.pack")
        Unix.[ O_EXCL; O_RDONLY; O_CLOEXEC ]
        0o644
    in
    let buffer = Bytes.create (4096 * 4096) in
    traverse fd buffer on_entry acc Int63.zero
end
