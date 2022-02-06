include Lwt.Syntax

module Int63 = struct
  include Optint.Int63
  include Infix

  let ( = ) = equal
  let ( > ) a b = compare a b > 0
  let ( < ) a b = compare a b < 0
  let ( >= ) a b = compare a b >= 0
  let ( <= ) a b = compare a b <= 0

  let () =
    let ten = of_int 10 in
    let eleven = of_int 11 in
    assert (ten = ten);
    assert (ten >= ten);
    assert (ten <= ten);
    assert (not (ten < ten));
    assert (not (ten > ten));

    assert (not (eleven = ten));
    assert (eleven >= ten);
    assert (not (eleven <= ten));
    assert (not (eleven < ten));
    assert (eleven > ten);

    assert (not (ten = eleven));
    assert (not (ten >= eleven));
    assert (ten <= eleven);
    assert (ten < eleven);
    assert (not (ten > eleven));
    Fmt.epr "👍 Passed all optint tests\n%!"

  let distance ~lo ~hi = hi - lo |> to_int
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
