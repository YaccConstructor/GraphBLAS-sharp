namespace GraphBLAS.FSharp.Predefined

open GraphBLAS.FSharp

module Add =
    let int: Monoid<int> = {
        Zero = 0
        Append = BinaryOp <@ (+) @>
    }

    let float: Monoid<float> = {
        Zero = 0.
        Append = BinaryOp <@ (+) @>
    }

    let float32: Monoid<float32> = {
        Zero = 0.f
        Append = BinaryOp <@ (+) @>
    }
