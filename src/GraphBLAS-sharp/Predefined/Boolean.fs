namespace GraphBLAS.FSharp

open GraphBLAS.FSharp

type BooleanMonoid =
    static member Or: Monoid<bool> = {
        Zero = false
        Append = BinaryOp <@ ( || ) @>
    }

type BooleanSemiring =
    static member OrAnd: Semiring<bool> = {
        PlusMonoid = BooleanMonoid.Or
        Times = BinaryOp <@ ( && ) @>
    }
