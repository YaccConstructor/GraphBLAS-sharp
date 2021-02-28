namespace GraphBLAS.FSharp.Predefined

open GraphBLAS.FSharp

module AddMult =
    let int: Semiring<int> = {
        PlusMonoid = Add.int
        Times = BinaryOp <@ (*) @>
    }

    let float: Semiring<float> = {
        PlusMonoid = Add.float
        Times = BinaryOp <@ (*) @>
    }

    let float32: Semiring<float32> = {
        PlusMonoid = Add.float32
        Times = BinaryOp <@ (*) @>
    }
