let loc =
  match Unix.gethostname () with
  | "DESKTOP-S4MOBKQ" -> `Home
  | s -> Fmt.failwith "Unknown hostname %S\n%!" s

let path =
  match loc with
  | `Home ->
      "/home/nico/tz/hangzu_plus2_1916931_BLu79NTncAFXHiwoHDwir4BDjh2Bdc7jgL71QYGkjv2c2oD8FwZ/store_post_node_run/context"
  | `Com ->
      "/home/ngoguey/bench/ro/hangzu_plus2_1916931_BLu79NTncAFXHiwoHDwir4BDjh2Bdc7jgL71QYGkjv2c2oD8FwZ/store_post_node_run/context/"

module Maker = Irmin_pack.Maker (Irmin_tezos.Conf)
module Store = Maker.Make (Irmin_tezos.Schema)

(* Etapes:
   - Pulls la liste de tous les cycles de tzstats.com
   - Filtrer cette liste en fonction de l'index, recup des commit_key
   - S'assurrer que cette liste est non vide et sans trous

   - init [pq]
   - init [results]
   - Pour chaque cycle, du plus grand au plus petit: [current_cycle]
      - Inserer dans [pq] la tuple [key du root node du commit du cycle], [cycle_id], ["/"]
      - Tant que pq n'est pas vide et que [max pq] est plus grand que l'offset du commit du cycle precedent
         - [key, parent_cycles, path = pop_max pq]
         - Apprendre: length, genre (blob|inode-{root,inner}-{tree,val}), preds, step_opt
         - [path'] c'est le prefix de taille 2 de [path / step_opt]
         - pour chaque [parent_cycle]
           - [k = parent_cycle, current_cycle, path', genre]
           - [results_count[k] += 1]
           - [results_bytes[k] += length]
         - inserer les [preds] dans [pq] annotes avec [parent_cycles] et [path']

   missing infos:
   - quantity of entries/bytes "belonging" to each cycle commit
   - quantity of entries/bytes per cycle
   - number of contiguous chunks of entries/bytes
   - pages
   - blobs size

 *)

let f = Revbuffer.create

(* let () =
 *   Fmt.epr "Hello World\n%!";
 *   let conf = Irmin_pack.config ~fresh:false ~readonly:true path in
 *   let repo = Store.Repo.v conf in
 *   let rb = Revbuffer.create 10 in
 *   Fmt.epr "Bye World\n%!" *)
