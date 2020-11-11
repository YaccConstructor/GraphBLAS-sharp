namespace GraphBLAS.FSharp

type Semiring<'a> = {
    PlusMonoid: Monoid<'a>
    Times: BinaryOp<'a, 'a, 'a>
}

module Semiring =
    let create<'a> (zero: 'a) (plus: BinaryOp<'a, 'a, 'a>) (times: BinaryOp<'a, 'a, 'a>) =
        {
            PlusMonoid = {
                Zero = zero
                Append = plus
            }
            Times = times
        }
