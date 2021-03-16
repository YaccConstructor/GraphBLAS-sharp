namespace GraphBLAS.FSharp.Predefined

open GraphBLAS.FSharp

module AddMult =
    let int =
        { new ISemiring<int> with
            member this.Zero = Add.int.Zero
            member this.Plus = Add.int.Plus
            member this.Times = ClosedBinaryOp <@ (*) @>
        }

    let float =
        { new ISemiring<float> with
            member this.Zero = Add.float.Zero
            member this.Plus = Add.float.Plus
            member this.Times = ClosedBinaryOp <@ (*) @>
        }

    let float32 =
        { new ISemiring<float32> with
            member this.Zero = Add.float32.Zero
            member this.Plus = Add.float32.Plus
            member this.Times = ClosedBinaryOp <@ (*) @>
        }

    let sbyte =
        { new ISemiring<sbyte> with
            member this.Zero = Add.sbyte.Zero
            member this.Plus = Add.sbyte.Plus
            member this.Times = ClosedBinaryOp <@ (*) @>
        }

    let byte =
        { new ISemiring<byte> with
            member this.Zero = Add.byte.Zero
            member this.Plus = Add.byte.Plus
            member this.Times = ClosedBinaryOp <@ (*) @>
        }

    let int16 =
        { new ISemiring<int16> with
            member this.Zero = Add.int16.Zero
            member this.Plus = Add.int16.Plus
            member this.Times = ClosedBinaryOp <@ (*) @>
        }

    let uint16 =
        { new ISemiring<uint16> with
            member this.Zero = Add.uint16.Zero
            member this.Plus = Add.uint16.Plus
            member this.Times = ClosedBinaryOp <@ (*) @>
        }
