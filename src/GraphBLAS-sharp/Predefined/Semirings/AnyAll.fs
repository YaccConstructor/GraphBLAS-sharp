namespace GraphBLAS.FSharp.Predefined

open GraphBLAS.FSharp

module AnyAll =
    let bool =
        { new ISemiring<bool> with
            member this.Zero = Any.bool.Zero
            member this.Plus = Any.bool.Plus
            member this.Times = ClosedBinaryOp <@ ( && ) @>
        }
