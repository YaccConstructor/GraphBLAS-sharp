namespace GraphBLAS.FSharp

type Monoid<'a> = {
    Zero: 'a
    Append: BinaryOp<'a, 'a, 'a>
}
