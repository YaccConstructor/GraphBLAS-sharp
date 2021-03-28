namespace GraphBLAS.FSharp.Predefined

open GraphBLAS.FSharp

module Any =
    let bool: Monoid<bool> =
        {
            AssociativeOp = ClosedBinaryOp <@ (||) @>
            Identity = false
        }
