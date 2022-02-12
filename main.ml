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

type acc = { entry_count : int; area_of_offset : int63 -> int }
type p = { ancestor_cycle_starts : Intset.t }

let on_entry acc (entry : _ Traverse.entry) =
  if acc.entry_count mod 50_000 = 0 then
    Fmt.epr "on_entry: %#d\n%!" acc.entry_count;

  let preds =
    match entry.v with `Contents -> [] | `Inode t -> preds_of_inode t
  in
  let preds = List.map (fun off -> (off, entry.payload)) preds in
  ({ acc with entry_count = acc.entry_count + 1 }, preds)

let on_chunk acc (chunk : Revbuffer.chunk) =
  ignore chunk;
  acc

let on_read acc ({ first; last } : IO.page_range) =
  ignore (first, last);
  acc

let merge_payloads _off ~older:{ ancestor_cycle_starts = s }
    ~newer:{ ancestor_cycle_starts = s' } =
  { ancestor_cycle_starts = Intset.union s s' }

let main () =
  Fmt.epr "Hello World\n%!";

  let conf = Irmin_pack.config ~fresh:false ~readonly:true path in
  let* repo = Store.Repo.v conf in
  let* cycle_starts = lookup_cycle_starts_in_repo repo in
  Fmt.epr "pack-store contains %d cycle starts\n%!" (List.length cycle_starts);

  let max_offsets =
    List.map (fun ((c : Cycle_start.t), off) -> (off, c.cycle_idx)) cycle_starts
  in

  let area_of_offset _ = 42 in

  let acc0 = { entry_count = 0; area_of_offset } in

  let max_offsets =
    List.map
      (fun (off, cycle_idx) ->
        ( off,
          {
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
