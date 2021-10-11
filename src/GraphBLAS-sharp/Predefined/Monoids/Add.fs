namespace GraphBLAS.FSharp.Predefined

open GraphBLAS.FSharp

module Add =
    let int: Monoid<int> =
        {
            AssociativeOp = ClosedBinaryOp <@ (+) @>
            Identity = 0
        }

    let float: Monoid<float> =
        {
            AssociativeOp = ClosedBinaryOp <@ (+) @>
            Identity = 0.
        }

    let float32: Monoid<float32> =
        {
            AssociativeOp = ClosedBinaryOp <@ (+) @>
            Identity = 0.f
        }

    let sbyte: Monoid<sbyte> =
        {
            AssociativeOp = ClosedBinaryOp <@ (+) @>
            Identity = 0y
        }

    let byte: Monoid<byte> =
        {
            AssociativeOp = ClosedBinaryOp <@ (+) @>
            Identity = 0uy
        }

    let int16: Monoid<int16> =
        {
            AssociativeOp = ClosedBinaryOp <@ (+) @>
            Identity = 0s
        }

    let uint16: Monoid<uint16> =
        {
            AssociativeOp = ClosedBinaryOp <@ (+) @>
            Identity = 0us
        }
