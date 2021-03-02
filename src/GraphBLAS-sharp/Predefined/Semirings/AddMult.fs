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

    let sbyte: Semiring<sbyte> = {
        PlusMonoid = Add.sbyte
        Times = BinaryOp <@ (*) @>
    }

    let byte: Semiring<byte> = {
        PlusMonoid = Add.byte
        Times = BinaryOp <@ (*) @>
    }

    let int16: Semiring<int16> = {
        PlusMonoid = Add.int16
        Times = BinaryOp <@ (*) @>
    }

    let uint16: Semiring<uint16> = {
        PlusMonoid = Add.uint16
        Times = BinaryOp <@ (*) @>
    }
