namespace GraphBLAS.FSharp.Predefined

open GraphBLAS.FSharp

module Float32Monoid =
    let add: Monoid<float32> = {
        Zero = 0.f
        Append = BinaryOp <@ (+) @>
    }

module Float32Semiring =
    let addMult: Semiring<float32> = {
        PlusMonoid = Float32Monoid.add
        Times = BinaryOp <@ (*) @>
    }
