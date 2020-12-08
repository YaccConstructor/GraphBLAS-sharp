namespace GraphBLAS.FSharp.Predefined

open GraphBLAS.FSharp

module BooleanMonoid =
    let any: Monoid<bool> = {
        Zero = false
        Append = BinaryOp <@ ( || ) @>
    }

module BooleanSemiring =
    let anyAll: Semiring<bool> = {
        PlusMonoid = BooleanMonoid.any
        Times = BinaryOp <@ ( && ) @>
    }
