namespace GraphBLAS.FSharp.Predefined

open GraphBLAS.FSharp

module Min =
    let int: Monoid<int> =
        {
            AssociativeOp = ClosedBinaryOp <@ fun x y -> System.Math.Min(x, y) @>
            Identity = System.Int32.MaxValue
        }

    let float: Monoid<float> =
        {
            AssociativeOp = ClosedBinaryOp <@ fun x y -> System.Math.Min(x, y) @>
            Identity = System.Double.PositiveInfinity
        }
