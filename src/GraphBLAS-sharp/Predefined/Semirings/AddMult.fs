namespace GraphBLAS.FSharp.Predefined

open GraphBLAS.FSharp

module AddMult =
    let int: Semiring<int> =
        { PlusMonoid = Add.int
          TimesSemigroup = { AssociativeOp = ClosedBinaryOp <@ (*) @> } }

    let float: Semiring<float> =
        { PlusMonoid = Add.float
          TimesSemigroup = { AssociativeOp = ClosedBinaryOp <@ (*) @> } }

    let float32: Semiring<float32> =
        { PlusMonoid = Add.float32
          TimesSemigroup = { AssociativeOp = ClosedBinaryOp <@ (*) @> } }

    let sbyte: Semiring<sbyte> =
        { PlusMonoid = Add.sbyte
          TimesSemigroup = { AssociativeOp = ClosedBinaryOp <@ (*) @> } }

    let byte: Semiring<byte> =
        { PlusMonoid = Add.byte
          TimesSemigroup = { AssociativeOp = ClosedBinaryOp <@ (*) @> } }

    let int16: Semiring<int16> =
        { PlusMonoid = Add.int16
          TimesSemigroup = { AssociativeOp = ClosedBinaryOp <@ (*) @> } }

    let uint16: Semiring<uint16> =
        { PlusMonoid = Add.uint16
          TimesSemigroup = { AssociativeOp = ClosedBinaryOp <@ (*) @> } }
