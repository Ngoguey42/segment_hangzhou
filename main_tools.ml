open Import
module Hash = Irmin_tezos.Schema.Hash
module Maker = Irmin_pack.Maker (Irmin_tezos.Conf)
module Store = Maker.Make (Irmin_tezos.Schema)
module IO = Pack_file_ios
module Traverse = Traverse.Make (Irmin_tezos.Conf) (Irmin_tezos.Schema)

module Seq_traverse =
  Sequential_traverse.Make (Irmin_tezos.Conf) (Irmin_tezos.Schema)

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
  | Direct s -> (s, `Direct)
  | Indirect key -> (
      match Dict.find dict key with
      | None -> assert false
      | Some s -> (s, `Indirect))

let v_of_compress v =
  let open Traverse.Inode.Compress in
  match v.tv with
  | V0_stable v | V0_unstable v | V1_root { v; _ } | V1_nonroot { v; _ } -> v

let preds_of_inode dict v : ((_ * _) option * int63) list =
  let open Traverse.Inode.Compress in
  let v = v_of_compress v in
  match v with
  | Values l ->
      List.map
        (function
          | Contents (name, addr, _) ->
              (Some (expand_name dict name), offset_of_address addr)
          | Node (name, addr) ->
              (Some (expand_name dict name), offset_of_address addr))
        l
  | Tree { entries; _ } ->
      List.map (fun (e : ptr) -> (None, offset_of_address e.hash)) entries

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
              let commit_offset =
                match Key.inspect (Store.Commit.key commit) with
                | Indexed _ -> assert false
                | Direct { offset; _ } -> offset
              in
              let node_offset = root_node_offset_of_commit commit in
              assert (Int63.(node_offset < commit_offset));
              Fmt.epr "%a, node:%#14d, commit:%#14d\n%!" Cycle_start.pp cycle
                (Int63.to_int node_offset)
                (Int63.to_int commit_offset);
              (cycle, node_offset, commit_offset) :: acc)
            commit_opt
        in
        Lwt.return acc)
      []
      (match loc with
      | `Com -> Cycle_start.all_cycle_seconds
      | `Home -> Cycle_start.all_cycle_firsts)
  in
  List.rev l

let digits s =
  let rec aux seq =
    match seq () with
    | Seq.Nil -> true
    | Seq.Cons ('0' .. '9', seq) -> aux seq
    | Seq.Cons _ -> false
  in
  aux (String.to_seq s)

let shorten_path l =
    match String.split_on_char '/' l with
    | ["";""]->"/"
    | ["";"protocol"]->"/protocol"
    | ["";"predecessor_block_metadata_hash"]->"/predecessor_block_metadata_hash"
    | ["";"predecessor_ops_metadata_hash"]->"/predecessor_ops_metadata_hash"
    | ["";"test_chain"]->"/test_chain"

    | ["";"data"]->"/data"
    | ["";"data";"cache"]->"/data/cache"
    | ["";"data";"cache";_]->"/data/cache/*"
    | ["";"data";"cache";_;"limit"] -> "/data/cache/*/limit"
    | ["";"data";"version"]->"/data/version"
    | ["";"data";"v1"]->"/data/v1"
    | ["";"data";"v1";"cycle_eras"]->"/data/v1/cycle_eras"
    | ["";"data";"v1";"constants"]->"/data/v1/constants"
    | ["";"data";"domain"]->"/data/domain"
    | ["";"data";"liquidity_baking_escape_ema"]->"/data/liquidity_baking_escape_ema"
    | ["";"data";"liquidity_baking_cpmm_address"]->"/data/liquidity_baking_cpmm_address"
    | ["";"data";"commitments"]->"/data/commitments"
    | ["";"data";"commitments";_]->"/data/commitments/*"
    | ["";"data";"block_priority"]->"/data/block_priority"
    | ["";"data";"number_of_caches"]->"/data/number_of_caches"

    | ["";"data";"cycle"]->"/data/cycle"
    | ["";"data";"cycle";_]->"/data/cycle/*"
    | ["";"data";"cycle";_;"random_seed"]->"/data/cycle/*/random_seed"
    | ["";"data";"cycle";_;"nonces"]->"/data/cycle/*/nonces"
    | ["";"data";"cycle";_;"nonces";_]->"/data/cycle/*/nonces/*"
    | ["";"data";"cycle";_;"last_roll"]->"/data/cycle/*/last_roll"
    | ["";"data";"cycle";_;"last_roll";_]->"/data/cycle/*/last_roll/*"
    | ["";"data";"cycle";_;"roll_snapshot"]->"/data/cycle/*/roll_snapshot"

    | ["";"data";"rolls"]->"/data/rolls"
    | ["";"data";"rolls";"index"]->"/data/rolls/index"
    | ["";"data";"rolls";"limbo"]->"/data/rolls/limbo"
    | ["";"data";"rolls";"next"]->"/data/rolls/next"
    | ["";"data";"rolls";"index";_]->"/data/rolls/index/*"
    | ["";"data";"rolls";"index";_;"successor"]->"/data/rolls/index/*/successor"
    | ["";"data";"rolls";"owner"]->"/data/rolls/owner"
    | ["";"data";"rolls";"owner";"current"]->"/data/rolls/owner/current"
    | ["";"data";"rolls";"owner";"current";_]->"/data/rolls/owner/current/*"
    | ["";"data";"rolls";"owner";"snapshot"]->"/data/rolls/owner/snapshot"
    | ["";"data";"rolls";"owner";"snapshot";_]->"/data/rolls/owner/snapshot/*"
    | ["";"data";"rolls";"owner";"snapshot";_;_]->"/data/rolls/owner/snapshot/*/*"
    | ["";"data";"rolls";"owner";"snapshot";_;_;_]->"/data/rolls/owner/snapshot/*/*/*"

    | ["";"data";"big_maps"]->"/data/big_maps"
    | ["";"data";"big_maps";"index"]->"/data/big_maps/index"
    | ["";"data";"big_maps";"next"]->"/data/big_maps/next"
    | ["";"data";"big_maps";"index";_]->"/data/big_maps/index/*"
    | ["";"data";"big_maps";"index";_;"key_type"]->"/data/big_maps/index/*/key_type"
    | ["";"data";"big_maps";"index";_;"value_type"]->"/data/big_maps/index/*/value_type"
    | ["";"data";"big_maps";"index";_;"total_bytes"]->"/data/big_maps/index/*/total_bytes"
    | ["";"data";"big_maps";"index";_;"contents"]->"/data/big_maps/index/*/contents"
    | ["";"data";"big_maps";"index";_;"contents";_]->"/data/big_maps/index/*/contents/*"
    | ["";"data";"big_maps";"index";_;"contents";_;"len"]->"/data/big_maps/index/*/contents/*/len"
    | ["";"data";"big_maps";"index";_;"contents";_;"data"]->"/data/big_maps/index/*/contents/*/data"

    | ["";"data";"contracts"]->"/data/contracts"
    | ["";"data";"contracts";"global_counter"]->"/data/contracts/global_counter"
    | ["";"data";"contracts";"index"]->"/data/contracts/index"
    | ["";"data";"contracts";"index";_]->"/data/contracts/index/*"
    | ["";"data";"contracts";"index";_;"data"]->"/data/contracts/index/*/data"
    | ["";"data";"contracts";"index";_;"data";"code"]->"/data/contracts/index/*/data/code"
    | ["";"data";"contracts";"index";_;"data";"storage"]->"/data/contracts/index/*/data/storage"
    | ["";"data";"contracts";"index";_;"len";"code"]->"/data/contracts/index/*/len/code"
    | ["";"data";"contracts";"index";_;"len";"storage"]->"/data/contracts/index/*/len/storage"

    | ["";"data";"contracts";"index";_;"frozen_balance"]->"/data/contracts/index/*/frozen_balance"
    | ["";"data";"contracts";"index";_;"frozen_balance";_]->"/data/contracts/index/*/frozen_balance/*"
    | ["";"data";"contracts";"index";_;"frozen_balance";_;"deposits"]->"/data/contracts/index/*/frozen_balance/*/deposits"
    | ["";"data";"contracts";"index";_;"frozen_balance";_;"rewards"]->"/data/contracts/index/*/frozen_balance/*/rewards"
    | ["";"data";"contracts";"index";_;"frozen_balance";_;"fees"]->"/data/contracts/index/*/frozen_balance/*/fees"

    | ["";"data";"contracts";"index";_;"deposits"]->"/data/contracts/index/*/deposits"
    | ["";"data";"contracts";"index";_;"rewards"]->"/data/contracts/index/*/rewards"
    | ["";"data";"contracts";"index";_;"fees"]->"/data/contracts/index/*/fees"
    | ["";"data";"contracts";"index";_;"manager"]->"/data/contracts/index/*/manager"
    | ["";"data";"contracts";"index";_;"counter"]->"/data/contracts/index/*/counter"
    | ["";"data";"contracts";"index";_;"balance"]->"/data/contracts/index/*/balance"
    | ["";"data";"contracts";"index";_;"len"]->"/data/contracts/index/*/len"
    | ["";"data";"contracts";"index";_;"used_bytes"]->"/data/contracts/index/*/used_bytes"
    | ["";"data";"contracts";"index";_;"paid_bytes"]->"/data/contracts/index/*/paid_bytes"
    | ["";"data";"contracts";"index";_;"roll_list"]->"/data/contracts/index/*/roll_list"
    | ["";"data";"contracts";"index";_;"change"]->"/data/contracts/index/*/change"
    | ["";"data";"contracts";"index";_;"delegate"]->"/data/contracts/index/*/delegate"
    | ["";"data";"contracts";"index";_;"delegate_activation"]->"/data/contracts/index/*/delegate_activation"
    | ["";"data";"contracts";"index";_;"delegate_desactivation"]->"/data/contracts/index/*/delegate_desactivation"
    | ["";"data";"contracts";"index";_;"inactive_delegate"]->"/data/contracts/index/*/inactive_delegate"
    | ["";"data";"contracts";"index";_;"spendable"]->"/data/contracts/index/*/spendable"
    | ["";"data";"contracts";"index";_;"delegated"]->"/data/contracts/index/*/delegated"
    | ["";"data";"contracts";"index";_;"delegated";_]->"/data/contracts/index/*/delegated/*"

    | ["";"data";"sapling"]->"/data/sapling/index"
    | ["";"data";"sapling";"index"]->"/data/sapling/index"
    | ["";"data";"sapling";"index";_]->"/data/sapling/index/*"
    | ["";"data";"sapling";"index";_;"roots_level"]->"/data/sapling/index/*/roots_level"
    | ["";"data";"sapling";"index";_;"roots"]->"/data/sapling/index/*/roots"
    | ["";"data";"sapling";"index";_;"roots";_]->"/data/sapling/index/*/roots/*"
    | ["";"data";"sapling";"index";_;"nullifiers_hashed"]->"/data/sapling/index/*/nullifiers_hashed"
    | ["";"data";"sapling";"index";_;"nullifiers_ordered"]->"/data/sapling/index/*/nullifiers_ordered"
    | ["";"data";"sapling";"index";_;"nullifiers_ordered";_]->"/data/sapling/index/*/nullifiers_ordered/*"
    | ["";"data";"sapling";"index";_;"ciphertexts"]->"/data/sapling/index/*/ciphertexts"
    | ["";"data";"sapling";"index";_;"ciphertexts";_]->"/data/sapling/index/*/ciphertexts/*"
    | ["";"data";"sapling";"index";_;"ciphertexts";_;"data"]->"/data/sapling/index/*/ciphertexts/*/data"
    | ["";"data";"sapling";"index";_;"commitments"]->"/data/sapling/index/*/commitments"
    | ["";"data";"sapling";"index";_;"commitments";_]->"/data/sapling/index/*/commitments/*"
    | ["";"data";"sapling";"index";_;"commitments";_;"data"]->"/data/sapling/index/*/commitments/*/data"
    | ["";"data";"sapling";"index";_;"commitments_size"] -> "/data/sapling/index/*/commitments_size"

    | ["";"data";"sapling";"index";_; a ] when digits a -> "/data/sapling/index/*/\\d+"
    | ["";"data";"sapling";"index";_;"\\d+";"commitments_size"] -> "/data/sapling/index/*/\\d+/commitments_size"
    | ["";"data";"sapling";"index";_;"\\d+";"memo_size"] -> "/data/sapling/index/*/\\d+/memo_size"
    | ["";"data";"sapling";"index";_;"\\d+";"nullifiers_size"] -> "/data/sapling/index/*/\\d+/nullifiers_size"
    | ["";"data";"sapling";"index";_;"\\d+";"roots_level"] -> "/data/sapling/index/*/\\d+/roots_level"
    | ["";"data";"sapling";"index";_;"\\d+";"roots_pos"] -> "/data/sapling/index/*/\\d+/roots_pos"
    | ["";"data";"sapling";"index";_;"\\d+";"total_bytes"] -> "/data/sapling/index/*/\\d+/total_bytes"
    | ["";"data";"sapling";"index";_;"\\d+";"roots"] -> "/data/sapling/index/*/\\d+/roots"
    | ["";"data";"sapling";"index";_;"\\d+";"roots";_] -> "/data/sapling/index/*/\\d+/roots/*"
    | ["";"data";"sapling";"index";_;"\\d+";"nullifiers_ordered"] -> "/data/sapling/index/*/\\d+/nullifiers_ordered"
    | ["";"data";"sapling";"index";_;"\\d+";"nullifiers_ordered";_] -> "/data/sapling/index/*/\\d+/nullifiers_ordered/*"
    | ["";"data";"sapling";"index";_;"\\d+";"nullifiers_hashed"] -> "/data/sapling/index/*/\\d+/nullifiers_hashed"
    | ["";"data";"sapling";"index";_;"\\d+";"nullifiers_hashed";_] -> "/data/sapling/index/*/\\d+/nullifiers_hashed/*"
    | ["";"data";"sapling";"index";_;"\\d+";"nullifiers_hashed";_;"len"] -> "/data/sapling/index/*/\\d+/nullifiers_hashed/*/len"
    | ["";"data";"sapling";"index";_;"\\d+";"nullifiers_hashed";_;"data"] -> "/data/sapling/index/*/\\d+/nullifiers_hashed/*/data"
    | ["";"data";"sapling";"index";_;"\\d+";"commitments"] -> "/data/sapling/index/*/\\d+/commitments"
    | ["";"data";"sapling";"index";_;"\\d+";"commitments";_] -> "/data/sapling/index/*/\\d+/commitments/*"
    | ["";"data";"sapling";"index";_;"\\d+";"commitments";_;"len"] -> "/data/sapling/index/*/\\d+/commitments/*/len"
    | ["";"data";"sapling";"index";_;"\\d+";"commitments";_;"data"] -> "/data/sapling/index/*/\\d+/commitments/*/data"
    | ["";"data";"sapling";"index";_;"\\d+";"ciphertexts"] -> "/data/sapling/index/*/\\d+/ciphertexts"
    | ["";"data";"sapling";"index";_;"\\d+";"ciphertexts";_] -> "/data/sapling/index/*/\\d+/ciphertexts/*"
    | ["";"data";"sapling";"index";_;"\\d+";"ciphertexts";_;"len"] -> "/data/sapling/index/*/\\d+/ciphertexts/*/len"
    | ["";"data";"sapling";"index";_;"\\d+";"ciphertexts";_;"data"] -> "/data/sapling/index/*/\\d+/ciphertexts/*/data"

    | ["";"data";"votes"]->"/data/votes"
    | ["";"data";"votes";"proposals"]->"/data/votes/proposals"
    | ["";"data";"votes";"proposals";_]->"/data/votes/proposals/*"
    | ["";"data";"votes";"proposals";_;"ed25519"]->"/data/votes/proposals/*/ed25519"
    | ["";"data";"votes";"proposals";_;"ed25519";_]->"/data/votes/proposals/*/ed25519/*"
    | ["";"data";"votes";"proposals";_;"p256"]->"/data/votes/proposals/*/p256"
    | ["";"data";"votes";"proposals";_;"p256";_]->"/data/votes/proposals/*/p256/*"
    | ["";"data";"votes";"proposals";_;"secp256k1"]->"/data/votes/proposals/*/secp256k1"
    | ["";"data";"votes";"proposals";_;"secp256k1";_]->"/data/votes/proposals/*/secp256k1/*"
    | ["";"data";"votes";"proposals_count"]->"/data/votes/proposals_count"
    | ["";"data";"votes";"proposals_count";"p256"] -> "/data/votes/proposals_count/p256"
    | ["";"data";"votes";"proposals_count";"secp256k1"]->"/data/votes/proposals_count/secp256k1"
    | ["";"data";"votes";"proposals_count";"ed25519"] -> "/data/votes/proposals_count/ed25519"
    | ["";"data";"votes";"proposals_count";"p256";_] -> "/data/votes/proposals_count/p256/*"
    | ["";"data";"votes";"proposals_count";"secp256k1";_]->"/data/votes/proposals_count/secp256k1/*"
    | ["";"data";"votes";"proposals_count";"ed25519";_] -> "/data/votes/proposals_count/ed25519/*"
    | ["";"data";"votes";"listings"]->"/data/votes/listings"
    | ["";"data";"votes";"listings"; "p256"]->"/data/votes/listings/p256"
    | ["";"data";"votes";"listings"; "ed25519"]->"/data/votes/listings/ed25519"
    | ["";"data";"votes";"listings"; "secp256k1"]->"/data/votes/listings/secp256k1"
    | ["";"data";"votes";"ballots"] -> "/data/votes/ballots"
    | ["";"data";"votes";"ballots";"ed25519"] -> "/data/votes/ballots/ed25519"
    | ["";"data";"votes";"ballots";"ed25519";_] -> "/data/votes/ballots/ed25519/*"
    | ["";"data";"votes";"ballots";"secp256k1"] -> "/data/votes/ballots/secp256k1"
    | ["";"data";"votes";"ballots";"secp256k1";_] -> "/data/votes/ballots/secp256k1/*"
    | ["";"data";"votes";"ballots";"p256"] -> "/data/votes/ballots/p256"
    | ["";"data";"votes";"ballots";"p256";_] -> "/data/votes/ballots/p256/*"
    | ["";"data";"votes";"current_period"] -> "/data/votes/current_period"
    | ["";"data";"votes";"current_proposal"] -> "/data/votes/current_proposal"
    | ["";"data";"votes";"listings_size"]->"/data/votes/listings_size"
    | ["";"data";"votes";"participation_ema"]->"/data/votes/participation_ema"
    | ["";"data";"votes";"pred_period_kind"]->"/data/votes/pred_period_kind"
    | ["";"data";"votes";"listings";"secp256k1";_]->"/data/votes/listings/secp256k1/*"
    | ["";"data";"votes";"listings";"p256";_]->"/data/votes/listings/p256/*"
    | ["";"data";"votes";"listings";"ed25519";_]->"/data/votes/listings/ed25519/*"

    | ["";"data";"active_delegates_with_rolls"]->"/data/active_delegates_with_rolls"
    | ["";"data";"active_delegates_with_rolls";"p256"]->"/data/active_delegates_with_rolls/p256"
    | ["";"data";"active_delegates_with_rolls";"ed25519"]->"/data/active_delegates_with_rolls/ed25519"
    | ["";"data";"active_delegates_with_rolls";"secp256k1"]->"/data/active_delegates_with_rolls/secp256k1"
    | ["";"data";"active_delegates_with_rolls";"p256";_]->"/data/active_delegates_with_rolls/p256/*"
    | ["";"data";"active_delegates_with_rolls";"ed25519";_]->"/data/active_delegates_with_rolls/ed25519/*"
    | ["";"data";"active_delegates_with_rolls";"secp256k1";_]->"/data/active_delegates_with_rolls/secp256k1/*"
    | ["";"data";"delegates_with_frozen_balance"]->"/data/delegates_with_frozen_balance"
    | ["";"data";"delegates_with_frozen_balance";_]->"/data/delegates_with_frozen_balance/*"
    | ["";"data";"delegates_with_frozen_balance";_;"ed25519"]->"/data/delegates_with_frozen_balance/*/ed25519"
    | ["";"data";"delegates_with_frozen_balance";_;"secp256k1"]->"/data/delegates_with_frozen_balance/*/secp256k1"
    | ["";"data";"delegates_with_frozen_balance";_;"p256"]->"/data/delegates_with_frozen_balance/*/p256"
    | ["";"data";"delegates_with_frozen_balance";_;"ed25519";_]->"/data/delegates_with_frozen_balance/*/ed25519/*"
    | ["";"data";"delegates_with_frozen_balance";_;"secp256k1";_]->"/data/delegates_with_frozen_balance/*/secp256k1/*"
    | ["";"data";"delegates_with_frozen_balance";_;"p256";_]->"/data/delegates_with_frozen_balance/*/p256/*"
    | ["";"data";"delegates"]->"/data/delegates"
    | ["";"data";"delegates";"ed25519"]->"/data/delegates/ed25519"
    | ["";"data";"delegates";"secp256k1"]->"/data/delegates/secp256k1"
    | ["";"data";"delegates";"p256"]->"/data/delegates/p256"
    | ["";"data";"delegates";"ed25519";_]->"/data/delegates/ed25519/*"
    | ["";"data";"delegates";"secp256k1";_]->"/data/delegates/secp256k1/*"
    | ["";"data";"delegates";"p256";_]->"/data/delegates/p256/*"

    | _ -> Fmt.epr "!!! Unkown pattern %S\n%!" l; "lol"
    [@@ocamlformat "disable=true"]
