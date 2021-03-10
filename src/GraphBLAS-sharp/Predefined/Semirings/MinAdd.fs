namespace GraphBLAS.FSharp.Predefined

open GraphBLAS.FSharp

module MinAdd =
    let float: Semiring<float> = {
        PlusMonoid = Min.float
        Times = BinaryOp <@ (+) @>
    }
