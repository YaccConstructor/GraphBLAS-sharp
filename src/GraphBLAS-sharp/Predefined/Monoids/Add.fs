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

    let sbyte: Monoid<sbyte> = {
        Zero = 0y
        Append = BinaryOp <@ (+) @>
    }

    let byte: Monoid<byte> = {
        Zero = 0uy
        Append = BinaryOp <@ (+) @>
    }

    let int16: Monoid<int16> = {
        Zero = 0s
        Append = BinaryOp <@ (+) @>
    }

    let uint16: Monoid<uint16> = {
        Zero = 0us
        Append = BinaryOp <@ (+) @>
    }
