namespace GraphBLAS.FSharp.Predefined

open GraphBLAS.FSharp

module Min =
    let int =
        { new IMonoid<int> with
            member this.Zero = System.Int32.MaxValue
            member this.Plus = ClosedBinaryOp <@ fun x y -> System.Math.Min(x, y) @>
        }

    let float =
        { new IMonoid<float> with
            member this.Zero = System.Double.PositiveInfinity
            member this.Plus = ClosedBinaryOp <@ fun x y -> System.Math.Min(x, y) @>
        }
