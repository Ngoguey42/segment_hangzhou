open Import
open Main_tools

type uint8_array =
  (int, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Array1.t

let uint8_array_t = Repr.map Repr.unit (fun _ -> assert false) (fun _ -> ())

type acc = {
  entry_count : int;
  tot_length : int;
  tot_chunk : int;
  tot_chunk_algo : int;
  tot_length_chunk : int;
  current_chunk : (int63 * int63) option;
  largest_algo_chunk : int;
  largest_chunk : int;
  emission_reset : int;
  emission_witnessed : int;
  emission_oos : int;
  page_seg : (uint8_array * uint8_array) option;
}
[@@deriving repr ~pp]

type p = Payload

let on_entry acc (entry : _ Traverse.entry) =
  (* if acc.entry_count mod 1_500_000 = 0 then *)
  (* Fmt.epr "on_entry: %#d\n%!" acc.entry_count; *)
  let preds =
    match entry.v with `Contents -> [] | `Inode t -> preds_of_inode t
  in
  let preds = List.map (fun off -> (off, Payload)) preds in

  let () =
    let IO.{ first; last } =
      IO.page_range_of_offset_length entry.offset entry.length
    in
    let needed =
      match acc.page_seg with
      | None -> assert false
      | Some (_, needed) -> needed
    in
    for i = first to last do
      assert (needed.{i} < 255);
      needed.{i} <- needed.{i} + 1
    done
  in

  let acc =
    {
      acc with
      entry_count = acc.entry_count + 1;
      tot_length = acc.tot_length + entry.length;
    }
  in
  (acc, preds)

let on_chunk acc (chunk : Revbuffer.chunk) =
  let left_offset = chunk.offset in
  let right_offset = Int63.add_distance left_offset chunk.length in
  let tot_chunk, current_chunk =
    match acc.current_chunk with
    | None ->
        assert (acc.tot_chunk = 0);
        (* Very first algo_chunk of very first chunk *)
        (1, (left_offset, right_offset))
    | Some (left, right) when Int63.(left = right_offset) ->
        (* Continuity of current_chunk *)
        (acc.tot_chunk, (left_offset, right))
    | Some _ ->
        (* Beginning of new chunk *)
        (acc.tot_chunk + 1, (left_offset, right_offset))
  in
  let largest_algo_chunk = max acc.largest_algo_chunk chunk.length in
  let largest_chunk =
    max acc.largest_chunk
      (Int63.distance_exn ~lo:(fst current_chunk) ~hi:(snd current_chunk))
  in
  let acc =
    match chunk.emission_reason with
    | `Reset -> { acc with emission_reset = acc.emission_reset + 1 }
    | `Witnessed_discontinuity ->
        { acc with emission_witnessed = acc.emission_witnessed + 1 }
    | `Out_of_space -> { acc with emission_oos = acc.emission_oos + 1 }
  in
  {
    acc with
    tot_chunk;
    tot_chunk_algo = acc.tot_chunk_algo + 1;
    tot_length_chunk = acc.tot_length_chunk + chunk.length;
    current_chunk = Some current_chunk;
    largest_algo_chunk;
    largest_chunk;
  }

let on_read acc ({ first; last } : IO.page_range) =
  let acc, read =
    match acc.page_seg with
    | None ->
        let read =
          Bigarray.Array1.create Bigarray.int8_unsigned Bigarray.c_layout
            (last + 1)
        in
        Bigarray.Array1.fill read 0;
        let needed =
          Bigarray.Array1.create Bigarray.int8_unsigned Bigarray.c_layout
            (last + 1)
        in
        Bigarray.Array1.fill needed 0;
        ({ acc with page_seg = Some (read, needed) }, read)
    | Some (read, _) -> (acc, read)
  in
  for i = first to last do
    read.{i} <- read.{i} + 1
  done;
  acc

let print_page_seg acc =
  let read, needed = Option.get acc.page_seg in
  let results = Hashtbl.create 5 in
  let any_page_read = ref false in
  for i = 0 to Bigarray.Array1.dim read - 1 do
    let count_read = read.{i} in
    any_page_read := !any_page_read || count_read > 0;
    if !any_page_read then
      let needed = needed.{i} > 0 in
      let key = (count_read, needed) in
      let new_count =
        match Hashtbl.find_opt results key with
        | None -> 1
        | Some count -> count + 1
      in
      Hashtbl.replace results key new_count
  done;
  Fmt.epr "* In the prefix of the pack file:\n";
  Hashtbl.iter
    (fun (count_read, needed) occurences ->
      let needed = if needed then "  needed  " else "not needed" in
      Fmt.epr "  * %#10d %s pages read %d times\n" occurences needed count_read)
    results;
  let fold f = Hashtbl.fold f results 0 |> float_of_int in
  let pages_read =
    fold (fun (count_read, _needed) occ acc -> (count_read * occ) + acc)
  in
  let pages_needed =
    fold (fun (_count_read, needed) occ acc ->
        if needed then occ + acc else acc)
  in
  let pages_in_the_section =
    fold (fun (_count_read, _needed) occ acc -> occ + acc)
  in
  let unique_pages_visited =
    fold (fun (count_read, _needed) occ acc ->
        if count_read > 0 then occ + acc else acc)
  in
  let pages_read_wasted = pages_read -. pages_needed in
  let bytes_read = pages_read *. 4096. in
  let bytes_needed = float_of_int acc.tot_length in
  let bytes_wasted = bytes_read -. bytes_needed in
  Fmt.epr
    "A \"used\" page is a page read that is actually used to produce entries. \
     The other pages read are the \"overhead\" ones.\n";
  Fmt.epr
    "A \"used\" byte is a byte read that is actually used to produce entries. \
     The other bytes read are the \"overhead\" ones.\n";
  Fmt.epr
    "* %6.2f%% of the bytes read were used (most of the overhead bytes are due \
     to the sparseness of used bytes inside used pages)\n"
    (bytes_wasted /. bytes_read *. 100.);
  Fmt.epr
    "* %6.2f%% of the pages read were used (the overhead pages are caused by \
     unlucky blindfolded reads)\n"
    (pages_needed /. pages_read *. 100.);
  Fmt.epr "* %6.2f%% of the bytes of the used pages were used bytes\n"
    (bytes_needed /. (pages_needed *. 4096.) *. 100.);
  Fmt.epr "* %6.2f%% of the overhead bytes are from overhead pages\n"
    (pages_read_wasted *. 4096. /. bytes_wasted *. 100.);
  Fmt.epr "* %6.2f%% of the pages are read in the prefix of the pack file\n"
    (unique_pages_visited /. pages_in_the_section *. 100.);
  Fmt.epr "* %6.2f%% of the pages are used in the prefix of the pack file\n"
    (pages_needed /. pages_in_the_section *. 100.);
  ()

let main () =
  Fmt.epr "Hello World\n%!";

  let conf = Irmin_pack.config ~fresh:false ~readonly:true path in
  let* repo = Store.Repo.v conf in
  let* cycle_starts = lookup_cycle_starts_in_repo repo in
  Fmt.epr "pack-store contains %d cycle starts\n%!" (List.length cycle_starts);

  (* let cycles = List.rev cycles in *)
  (* let cycles = [ List.nth cycles 6 ] in *)
  List.iter
    (fun (cycle, offset) ->
      Fmt.epr "pack store contains %a at offset %#14d\n%!" Cycle_start.pp cycle
        (Int63.to_int offset);
      let acc0 =
        {
          entry_count = 0;
          tot_length = 0;
          tot_chunk = 0;
          tot_chunk_algo = 0;
          tot_length_chunk = 0;
          current_chunk = None;
          largest_algo_chunk = 0;
          largest_chunk = 0;
          emission_reset = 0;
          emission_witnessed = 0;
          emission_oos = 0;
          page_seg = None;
        }
      in
      let acc =
        Traverse.fold path
          [ (offset, Payload) ]
          ~merge_payloads:(fun _off ~older:Payload ~newer:Payload -> Payload)
          ~on_entry ~on_chunk ~on_read acc0
      in
      Fmt.epr "%a\n%!" pp_acc acc;
      print_page_seg acc;

      Fmt.epr "\n%!";
      Fmt.epr "\n%!";
      (* if true then failwith "super"; *)
      Fmt.epr "\n%!")
    cycle_starts;

  Fmt.epr "Bye World\n%!";
  Lwt.return_unit

let () = Lwt_main.run (main ())
