namespace GraphBLAS.FSharp.Predefined

open GraphBLAS.FSharp

module Any =
    let bool =
        { new IMonoid<bool> with
            member this.Zero = false
            member this.Plus = ClosedBinaryOp <@ ( || ) @>
        }
