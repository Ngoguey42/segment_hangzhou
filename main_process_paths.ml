let loc =
  match Unix.gethostname () with
  | "DESKTOP-S4MOBKQ" -> `Home
  | "comanche" -> `Com
  | s -> Fmt.failwith "Unknown hostname %S\n%!" s

let digits s =
  let rec aux seq =
    match seq () with
    | Seq.Nil -> true
    | Seq.Cons ('0' .. '9', seq) -> aux seq
    | Seq.Cons _ -> false
  in
  aux (String.to_seq s)

let fourty_chars s = String.length s = 40
let sixtyfour_chars s = String.length s = 64

let () =
  let chan =
    open_in
    (match loc with `Home -> "csv/entries.csv" | `Com -> "entries.csv")
  in
  Fmt.epr "opened\n%!";
  let (_ : string) = input_line chan in

  let occs = Hashtbl.create 1_000 in

  let rec aux i : 'a =
    let l = input_line chan in
    let l =
      match String.split_on_char ',' l with
      | _ :: _ :: path :: _ -> path
      | _ -> assert false
    in
    (* if i mod 5000 = 0 then Fmt.epr "%d - %s\n%!" i l; *)


    let k = match String.split_on_char '/' l with

    | ["multiple"]->"multiple"
    | ["";""]->"/"
    | ["";"protocol"]->"/protocol"

    | ["";"data";"cycle"]->"/data/cycle"
    | ["";"data";"cycle";_]->"/data/cycle/*"
    | ["";"data";"cycle";_;"random_seed"]->"/data/cycle/*/random_seed"
    | ["";"data";"cycle";_;"nonces";_]->"/data/cycle/*/nonces/*"
    | ["";"data";"cycle";_;"last_roll"]->"/data/cycle/*/last_roll"

    | ["";"data";"commitments"]->"/data/commitments"
    | ["";"data";"commitments";_]->"/data/commitments/*"

    | ["";"data";"rolls"]->"/data/rolls"
    | ["";"data";"rolls";"index"]->"/data/rolls/index"
    | ["";"data";"rolls";"limbo"]->"/data/rolls/limbo"
    | ["";"data";"rolls";"index";_]->"/data/rolls/index/*"
    | ["";"data";"rolls";"index";_;"successor"]->"/data/rolls/index/*/successor"
    | ["";"data";"rolls";"owner";"current"]->"/data/rolls/owner/current"
    | ["";"data";"rolls";"owner";"snapshot"]->"/data/rolls/owner/snapshot"
    | ["";"data";"rolls";"owner";"snapshot";_]->"/data/rolls/owner/snapshot/*"
    | ["";"data";"rolls";"owner";"snapshot";_;_]->"/data/rolls/owner/snapshot/*/*"
    | ["";"data";"rolls";"owner";"snapshot";_;_;_]->"/data/rolls/owner/snapshot/*/*/*"

    | ["";"data";"big_maps"]->"/data/big_maps"
    | ["";"data";"big_maps";"index"]->"/data/big_maps/index"
    | ["";"data";"big_maps";"index";_]->"/data/big_maps/index/*"
    | ["";"data";"big_maps";"index";_;"key_type"]->"/data/big_maps/index/*/key_type"
    | ["";"data";"big_maps";"index";_;"contents"]->"/data/big_maps/index/*/contents"
    | ["";"data";"big_maps";"index";_;"contents";_]->"/data/big_maps/index/*/contents/*"
    | ["";"data";"big_maps";"index";_;"value_type"]->"/data/big_maps/index/*/value_type"
    | ["";"data";"big_maps";"index";_;"total_bytes"]->"/data/big_maps/index/*/total_bytes"
    | ["";"data";"big_maps";"index";_;"contents";_;"data"]->"/data/big_maps/index/*/contents/*/data"

    | ["";"data";"contracts";"index"]->"/data/contracts/index"
    | ["";"data";"contracts";"index";_]->"/data/contracts/index/*"
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
    | ["";"data";"contracts";"index";_;"data"]->"/data/contracts/index/*/data"
    | ["";"data";"contracts";"index";_;"delegated"]->"/data/contracts/index/*/delegated"
    | ["";"data";"contracts";"index";_;"used_bytes"]->"/data/contracts/index/*/used_bytes"
    | ["";"data";"contracts";"index";_;"paid_bytes"]->"/data/contracts/index/*/paid_bytes"
    | ["";"data";"contracts";"index";_;"roll_list"]->"/data/contracts/index/*/roll_list"
    | ["";"data";"contracts";"index";_;"change"]->"/data/contracts/index/*/change"
    | ["";"data";"contracts";"index";_;"delegate"]->"/data/contracts/index/*/delegate"

    | ["";"data";"contracts";"index";_;"data";"code"]->"/data/contracts/index/*/data/code"
    | ["";"data";"contracts";"index";_;"data";"storage"]->"/data/contracts/index/*/data/storage"

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

    | ["";"data";"active_delegates_with_rolls";"p256"]->"/data/active_delegates_with_rolls/p256"
    | ["";"data";"active_delegates_with_rolls";"ed25519"]->"/data/active_delegates_with_rolls/ed25519"
    | ["";"data";"delegates_with_frozen_balance";_;"ed25519"]->"/data/delegates_with_frozen_balance/*/ed25519"
    | ["";"data";"delegates_with_frozen_balance";_;"secp256k1"]->"/data/delegates_with_frozen_balance/*/secp256k1"
    | ["";"data";"delegates_with_frozen_balance";_;"p256"]->"/data/delegates_with_frozen_balance/*/p256"

    | ["";"data";"delegates"]->"/data/delegates"
    | ["";"data";"delegates";"ed25519"]->"/data/delegates/ed25519"
    | ["";"data";"cache";_]->"/data/cache/*"
    | ["";"data";"votes";"proposals_count";"secp256k1"]->"/data/votes/proposals_count/secp256k1"
    | ["";"data";"votes";"listings"]->"/data/votes/listings"
    | ["";"data";"votes";"listings"; "p256"]->"/data/votes/listings/p256"
    | ["";"data";"v1";"cycle_eras"]->"/data/v1/cycle_eras"
    | ["";"data";"v1";"constants"]->"/data/v1/constants"
    | ["";"data";"domain"]->"/data/domain"

    | _ -> Fmt.failwith "Unkown pattern %s" l
    in
       (match Hashtbl.find_opt occs k with
       | None -> Hashtbl.add occs k 1
       | Some i -> Hashtbl.replace occs k (i + 1));

    aux (i + 1)
  in
  (try aux 0
  with End_of_file ->
    Fmt.epr "eof\n%!");

  close_in chan;
  Fmt.epr "closed\n%!";

  Hashtbl.iter  (fun k v ->
   Fmt.epr "%#10d %s\n%!" v k;


    ) occs;

  ()
[@@ocamlformat "disable=true"]
