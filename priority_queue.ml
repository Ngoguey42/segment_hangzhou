module Make (T : Map.OrderedType) = struct
  module M = Map.Make (T)

  type 'a t = { mutable map : 'a M.t; mutable length : int }

  let create () = { map = M.empty; length = 0 }

  let top_exn t = M.max_binding t.map

  let pop_exn t =
    let ((k, _) as pair) = top_exn t in
    t.map <- M.remove k t.map;
    t.length <- t.length - 1;
    pair

  let update t k f =
    let g v_opt =
      let v' = Some (f v_opt) in
      Option.iter (fun _ -> t.length <- t.length + 1) v_opt;
      v'
    in
    M.update k g t.map

  let is_empty t = t.length = 0
  let length t = t.length
end
