open Import
module Hash = Irmin_tezos.Schema.Hash
module Maker = Irmin_pack.Maker (Irmin_tezos.Conf)
module Store = Maker.Make (Irmin_tezos.Schema)
module IO = Pack_file_ios
module Traverse = Traverse.Make (Irmin_tezos.Conf) (Irmin_tezos.Schema)
module Dict = Irmin_pack.Dict

module Key = struct
  include Irmin_pack.Pack_key
  include Irmin_pack.Pack_key.Make (Hash)

  type t = Store.node_key [@@deriving repr]
end

type hash = Store.hash [@@deriving repr ~pp]

let loc =
  match Unix.gethostname () with
  | "DESKTOP-S4MOBKQ" -> `Home
  | "comanche" -> `Com
  | s -> Fmt.failwith "Unknown hostname %S\n%!" s

let path =
  match loc with
  | `Home ->
      "/home/nico/r/irmin/_artefacts/1f1548b9-b7d2-433b-b199-ef376b567951/store"
  | `Com ->
      "/home/ngoguey/bench/ro/hangzu_plus2_1916931_BLu79NTncAFXHiwoHDwir4BDjh2Bdc7jgL71QYGkjv2c2oD8FwZ/store_post_node_run/context/"

let hash_of_string =
  let f = Repr.of_string Irmin_tezos.Schema.Hash.t in
  fun x -> match f x with Error (`Msg x) -> failwith x | Ok v -> v

let hash_to_bin_string = Repr.to_bin_string hash_t |> Repr.unstage

let offset_of_address =
  let open Traverse.Inode.Compress in
  function
  | Offset x -> x
  | Hash _ -> failwith "traverse doesn't handle inode children by hash"

let expand_name dict name =
  let open Traverse.Inode.Compress in
  match name with
  | Direct s -> s, `Direct
  | Indirect key ->
    match   Dict.find dict key with
    | None -> assert false
    | Some s -> s, `Indirect

let preds_of_inode dict v =
  let open Traverse.Inode.Compress in
  let v =
    match v.tv with
    | V0_stable v | V0_unstable v | V1_root { v; _ } | V1_nonroot { v; _ } -> v
  in
  match v with
  | Values l ->
      List.map
        (function
          | Contents (name, addr, _) -> Some (expand_name dict name), offset_of_address addr
          | Node (name, addr) -> Some (expand_name dict name), offset_of_address addr)
        l
  | Tree { entries; _ } ->
      List.map (fun (e : ptr) -> None, offset_of_address e.hash) entries

let root_node_offset_of_commit commit =
  let k =
    match Store.Commit.tree commit |> Store.Tree.key with
    | None -> assert false
    | Some (`Contents _) -> assert false
    | Some (`Node k) -> k
  in
  let offset =
    match Key.inspect k with
    | Indexed _ -> assert false
    | Direct { offset; _ } -> offset
  in
  offset

let lookup_cycle_starts_in_repo repo =
  let+ l =
    Lwt_list.fold_left_s
      (fun acc (cycle : Cycle_start.t) ->
        let h = hash_of_string cycle.context_hash in
        let* commit_opt = Store.Commit.of_hash repo h in
        let acc =
          Option.fold ~none:acc
            ~some:(fun commit ->
              let offset = root_node_offset_of_commit commit in
              (* Fmt.epr "pack store contains %a at offset %#14d\n%!" Cycle_start.pp *)
              (* cycle (Int63.to_int offset); *)
              (cycle, offset) :: acc)
            commit_opt
        in
        Lwt.return acc)
      [] Cycle_start.all
  in
  List.rev l
