include Lwt.Syntax

let ref_t x = Repr.map x ref ( ! )

module Int63 = struct
  include Optint.Int63
  include Infix

  let ( = ) = equal
  let ( > ) a b = compare a b > 0
  let ( < ) a b = compare a b < 0
  let ( >= ) a b = compare a b >= 0
  let ( <= ) a b = compare a b <= 0
  let min x y = if x < y then x else y
  let max x y = if x > y then x else y

  let distance_exn ~lo ~hi =
    if lo > hi then failwith "negative distance";
    hi - lo |> to_int

  let sub_distance x y = x - of_int y
  let add_distance x y = x + of_int y
end

let ( ++ ) = Int63.add
let ( -- ) = Int63.sub

type int63 = Int63.t [@@deriving repr ~pp]

let ( === ) : 'a -> 'a -> bool = ( = )
let ( <<>> ) : 'a -> 'a -> bool = ( <> )

(* Disallow polymorphic comparisons *)
let ( < ) : int -> int -> bool = ( < )
let ( > ) : int -> int -> bool = ( > )
let ( <= ) : int -> int -> bool = ( <= )
let ( >= ) : int -> int -> bool = ( >= )
let ( = ) : int -> int -> bool = ( = )
let ( <> ) : int -> int -> bool = ( <> )

module Int = struct
  include Int

  let distance_exn ~lo ~hi =
    if lo > hi then failwith "negative distance";
    hi - lo
end
