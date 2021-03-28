namespace GraphBLAS.FSharp.Predefined

open GraphBLAS.FSharp

module AnyAll =
    let bool: Semiring<bool> =
        {
            PlusMonoid = Any.bool
            TimesSemigroup = { AssociativeOp = ClosedBinaryOp <@ (&&) @> }
        }
