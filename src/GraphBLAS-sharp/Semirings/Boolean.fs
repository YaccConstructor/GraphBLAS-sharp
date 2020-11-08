namespace GraphBLAS.FSharp

[<Struct>]
type OrAndSemiring = OrAndSemiring of bool
with
    static member Zero = OrAndSemiring false
    static member (+) (OrAndSemiring x, OrAndSemiring y) = (x || y) |> OrAndSemiring
    static member (*) (OrAndSemiring x, OrAndSemiring y) = (x && y) |> OrAndSemiring
    static member op_Implicit (OrAndSemiring source) = source
