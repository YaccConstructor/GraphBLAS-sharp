namespace GraphBLAS.FSharp.Predefined

open GraphBLAS.FSharp

module Min =
    let int: Monoid<int> = {
        Zero = System.Int32.MaxValue
        Append = BinaryOp <@ fun x y -> System.Math.Min(x, y) @>
    }

    let float: Monoid<float> = {
        Zero = System.Double.PositiveInfinity
        Append = BinaryOp <@ fun x y -> System.Math.Min(x, y) @>
    }
