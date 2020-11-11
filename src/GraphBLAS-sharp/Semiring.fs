namespace GraphBLAS.FSharp

type Semiring<'T1, 'T2, 'TOut> = {
    PlusMonoid: Monoid<'TOut>
    Times: BinaryOp<'T1, 'T2, 'TOut>
}

type Semiring<'T> = Semiring<'T, 'T, 'T>

module Semiring =
    let create<'a> (zero: 'a) (plus: BinaryOp<'a, 'a, 'a>) (times: BinaryOp<'a, 'a, 'a>) : Semiring<'a> =
        {
            PlusMonoid = {
                Zero = zero
                Append = plus
            }
            Times = times
        }
