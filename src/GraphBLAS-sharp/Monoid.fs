namespace GraphBLAS.FSharp

[<Struct>]
type MinPlusSemiring = MinPlusSemiring of float
with
    static member Zero = MinPlusSemiring System.Double.PositiveInfinity
    static member (+) (MinPlusSemiring x, MinPlusSemiring y) = System.Math.Min(x, y) |> MinPlusSemiring
    static member (*) (MinPlusSemiring x, MinPlusSemiring y) = x + y |> MinPlusSemiring
    static member op_Implicit (MinPlusSemiring source) : float = source

[<Struct>]
type StandardSemiring = StandardSemiring of float
with
    static member Zero = StandardSemiring System.Double.PositiveInfinity
    static member (+) (StandardSemiring x, StandardSemiring y) = x + y |> StandardSemiring
    static member (*) (StandardSemiring x, StandardSemiring y) = x * y |> StandardSemiring
    static member op_Implicit (StandardSemiring source) : float = source

module A =
    let inline (!>) (x: ^a) : ^b = ((^a or ^b) : (static member op_Implicit : ^a -> ^b) x)
    let a = MinPlusSemiring 1.0
    let b = StandardSemiring 5.0
    let c = StandardSemiring !> a
