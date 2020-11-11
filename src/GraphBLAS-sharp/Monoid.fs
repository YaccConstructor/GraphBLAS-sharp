namespace GraphBLAS.FSharp

type Monoid<'T> = {
    Zero: 'T
    Append: BinaryOp<'T, 'T, 'T>
}
