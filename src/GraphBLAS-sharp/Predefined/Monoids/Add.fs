namespace GraphBLAS.FSharp.Predefined

open GraphBLAS.FSharp

module Add =
    let int =
        { new IMonoid<int> with
            member this.Zero = 0
            member this.Plus = ClosedBinaryOp <@ (+) @>
        }

    let float =
        { new IMonoid<float> with
            member this.Zero = 0.
            member this.Plus = ClosedBinaryOp <@ (+) @>
        }

    let float32 =
        { new IMonoid<float32> with
            member this.Zero = 0f
            member this.Plus = ClosedBinaryOp <@ (+) @>
        }

    let sbyte =
        { new IMonoid<sbyte> with
            member this.Zero = 0y
            member this.Plus = ClosedBinaryOp <@ (+) @>
        }

    let byte =
        { new IMonoid<byte> with
            member this.Zero = 0uy
            member this.Plus = ClosedBinaryOp <@ (+) @>
        }

    let int16 =
        { new IMonoid<int16> with
            member this.Zero = 0s
            member this.Plus = ClosedBinaryOp <@ (+) @>
        }

    let uint16 =
        { new IMonoid<uint16> with
            member this.Zero = 0us
            member this.Plus = ClosedBinaryOp <@ (+) @>
        }
