type section =
  | Read
  | Decode_inode
  | Decode_length
  | Priority_queue
  | Blit
  | Overhead
  | Callback
[@@deriving repr ~pp]

let all =
  [
    Read; Decode_inode; Decode_length; Priority_queue; Blit; Overhead; Callback;
  ]
(* TODO: Limit read to really_read *)

module M = Map.Make (struct
  type t = section

  let compare = compare
end)

type t = {
  mutable current_section : section;
  mutable totals : Mtime.Span.t M.t;
  mutable counter : Mtime_clock.counter;
}

let pp : t Repr.pp =
 fun ppf t ->
  let elapsed = Mtime_clock.count t.counter in
  let totals =
    M.update t.current_section
      (function
        | None -> assert false
        | Some so_far -> Some (Mtime.Span.add so_far elapsed))
      t.totals
  in
  let total_span = M.fold (fun _ -> Mtime.Span.add) totals Mtime.Span.zero in
  let total = Mtime.Span.to_s total_span in
  Format.fprintf ppf "{\"Total\":%a(100%%)," Mtime.Span.pp total_span;

  let pp_one ppf (section, elapsed) =
    Format.fprintf ppf "%a:%a(%.1f%%)" pp_section section Mtime.Span.pp elapsed
      (Mtime.Span.to_s elapsed /. total *. 100.)
  in
  Format.fprintf ppf "%a}"
    Fmt.(list ~sep:(any ",") pp_one)
    (totals |> M.to_seq |> List.of_seq)

let v current_section =
  let totals =
    List.map (fun v -> (v, Mtime.Span.zero)) all |> List.to_seq |> M.of_seq
  in
  let counter = Mtime_clock.counter () in
  { totals; counter; current_section }

let switch t next_section =
  let elapsed = Mtime_clock.count t.counter in
  t.totals <-
    M.update t.current_section
      (function
        | None -> assert false
        | Some so_far -> Some (Mtime.Span.add so_far elapsed))
      t.totals;
  t.counter <- Mtime_clock.counter ();
  t.current_section <- next_section

let with_section t section f =
  let parent_section = t.current_section in
  switch t section;
  let res = f () in
  switch t parent_section;
  res
