namespace GraphBLAS.FSharp

type Semiring<'T> = {
    PlusMonoid: Monoid<'T>
    Times: BinaryOp<'T, 'T, 'T>
}
