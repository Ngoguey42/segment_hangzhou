open Import

type t = {
  peak_pq : int ref;
  (* Lookups in revbuffer *)
  hit : int ref;
  blindfolded_perfect : int ref;
  blindfolded_too_much : int ref;
  blindfolded_not_enough : int ref;
  was_previous : int ref;
  (* Revbuffer stats *)
  soft_blit : int ref;
  hard_blit : int ref;
  soft_blit_bytes : int ref;
  hard_blit_bytes : int ref;
}
[@@deriving repr ~pp]

let v () =
  {
    hit = ref 0;
    blindfolded_perfect = ref 0;
    blindfolded_too_much = ref 0;
    blindfolded_not_enough = ref 0;
    was_previous = ref 0;
    peak_pq = ref 0;
    soft_blit = ref 0;
    hard_blit = ref 0;
    soft_blit_bytes = ref 0;
    hard_blit_bytes = ref 0;
  }
