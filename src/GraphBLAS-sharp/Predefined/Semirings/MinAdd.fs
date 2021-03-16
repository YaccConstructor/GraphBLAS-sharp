namespace GraphBLAS.FSharp.Predefined

open GraphBLAS.FSharp

module MinAdd =
    let float =
        { new ISemiring<float> with
            member this.Zero = Min.float.Zero
            member this.Plus = Min.float.Plus
            member this.Times = ClosedBinaryOp <@ (+) @>
        }
