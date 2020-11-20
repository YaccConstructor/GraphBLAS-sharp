namespace GraphBLAS.FSharp

type Semiring<'T1, 'T2, 'TOut> = {
    PlusMonoid: Monoid<'TOut>
    Times: BinaryOp<'T1, 'T2, 'TOut>
}

type Semiring<'T> = Semiring<'T, 'T, 'T>
