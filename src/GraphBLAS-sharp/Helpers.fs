namespace GraphBLAS.FSharp

[<AutoOpen>]
module Helpers =
    let inline (!>) (x: ^a) : ^b = (^a: (static member op_Implicit : ^a -> ^b) x)

    let inline (^) f x = f x
