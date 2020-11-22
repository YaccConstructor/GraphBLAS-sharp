namespace GraphBLAS.FSharp

module Helpers =
    let inline (!>) (x: ^a) : ^b = (^a : (static member op_Implicit : ^a -> ^b) x)
