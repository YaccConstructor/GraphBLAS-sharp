namespace GraphBLAS.FSharp

[<Struct>]
type MinPlusSemiring = MinPlusSemiring of float
with
    static member Zero = MinPlusSemiring System.Double.PositiveInfinity
    static member (+) (MinPlusSemiring x, MinPlusSemiring y) = System.Math.Min(x, y) |> MinPlusSemiring
    static member (*) (MinPlusSemiring x, MinPlusSemiring y) = x + y |> MinPlusSemiring
    static member op_Implicit (MinPlusSemiring source) = source
