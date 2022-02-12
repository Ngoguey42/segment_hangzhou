(** Hello

    Requires https://github.com/Ngoguey42/irmin/pull/new/expose-compress *)

(*
   - genre: (blob-{0-31,32-127,128,511,512+}|inode-{root,inner}-{tree,val})

   collected infos:
   - "per commit tree" x "per pack-file-area" x "per path prefix" x "per genre"
     - # of entries
     - # of bytes used
   - "per commit tree" x "per pack-file-area" x "per path prefix" x ()
     - # of bytes used by hard-coded steps
     - # of hard-coded steps
     - # of dict steps
   - "per commit tree" x "per pack-file-area" x ()                x ()
     - # of pages touched
     - # of chunks (contiguous groups)
   - () x                "per pack-file-area" x ()                x "per genre + commit"
     - # of total entries (needed for leftover calculation)
     - # of total bytes used (needed for leftover calculation)

   missing infos:
   - which area references which area? (i.e. analysis of pq when changing area)
   - intersection between trees

   to show:
   - (leftward horizontal histogram?) averaged on all ref commits
     - at which cycle-distance are all genre (entry weighted / bytes weighted)
     - at which cycle-distance are all paths (entry weighted / bytes weighted)
   - (camembert) averaged on all ref commits + (curves) evolution for all ref commits
     - at which cycle-distance are the entries (entry weighted / bytes weighted)
     - number in each genre  (entry weighted / bytes weighted)
     - number in each directory (entry weighted / bytes weighted)
     - which path grows the most (entry weighted / bytes weighted)

    morallement, quelles info je veux:
    -
    -
    -
    -
    -
    -

*)
open Import
open Main_tools

module Intset = Set.Make (struct
  type t = int

  let compare = compare
end)

type acc = { dict : Dict.t; entry_count : int; area_of_offset : int63 -> int }
type p = { truncated_path_rev : string list; ancestor_cycle_starts : Intset.t }

let lol = Hashtbl.create 100

let on_entry acc (entry : _ Traverse.entry) =
  if acc.entry_count mod 50_000 = 0 then
    Fmt.epr "on_entry: %#d\n%!" acc.entry_count;

  let ancestor_cycle_starts = entry.payload.ancestor_cycle_starts in
  let prefix = entry.payload.truncated_path_rev in
  (* if not @@ Hashtbl.mem lol prefix then (
   *   Fmt.epr "/%s \n%!" (prefix |> List.rev |> String.concat "/");
   *   Hashtbl.add lol prefix ()); *)

  let prefix_full = List.length prefix >= 2 in
  let preds =
    match entry.v with
    | `Contents -> []
    | `Inode t ->
        preds_of_inode acc.dict t
        |> List.map (fun (step_opt, off) ->
               let truncated_path_rev =
                 if prefix_full then prefix
                 else
                   match step_opt with
                   | None -> prefix
                   | Some step -> step :: prefix
               in
               let payload = { truncated_path_rev; ancestor_cycle_starts } in
               (off, payload))
  in

  (* let preds = List.map (fun off -> (off, entry.payload)) preds in *)
  ({ acc with entry_count = acc.entry_count + 1 }, preds)

let on_chunk acc (chunk : Revbuffer.chunk) =
  ignore chunk;
  acc

let on_read acc ({ first; last } : IO.page_range) =
  ignore (first, last);
  acc

let merge_payloads _off
    ~older:{ truncated_path_rev = l; ancestor_cycle_starts = s }
    ~newer:{ truncated_path_rev = l'; ancestor_cycle_starts = s' } =
  let truncated_path_rev =
    if l == l' then l else if l === l' then l else []
    (* Fmt.failwith "Which path to choose? /%s /%s" ( l |> List.rev |> String.concat "/") *)
    (* ( l' |> List.rev |> String.concat "/") *)
  in
  (* TODO: truncated_path_rev     *)
  { truncated_path_rev; ancestor_cycle_starts = Intset.union s s' }

let main () =
  Fmt.epr "Hello World\n%!";

  let conf = Irmin_pack.config ~fresh:false ~readonly:true path in
  let* repo = Store.Repo.v conf in
  let dict = Dict.v ~fresh:false ~readonly:true path in
  let* cycle_starts = lookup_cycle_starts_in_repo repo in
  Fmt.epr "pack-store contains %d cycle starts\n%!" (List.length cycle_starts);

  let max_offsets =
    List.map (fun ((c : Cycle_start.t), off) -> (off, c.cycle_idx)) cycle_starts
  in

  let area_of_offset _ = 42 in

  let acc0 = { dict; entry_count = 0; area_of_offset } in

  let max_offsets =
    List.map
      (fun (off, cycle_idx) ->
        ( off,
          {
            truncated_path_rev = [];
            ancestor_cycle_starts =
              [ cycle_idx ] |> List.to_seq |> Intset.of_seq;
          } ))
      max_offsets
  in

  let acc =
    Traverse.fold path max_offsets ~merge_payloads ~on_entry ~on_chunk ~on_read
      acc0
  in

  Fmt.epr "Bye World\n%!";
  Lwt.return_unit

let () = Lwt_main.run (main ())
