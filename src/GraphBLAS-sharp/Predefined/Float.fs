namespace GraphBLAS.FSharp.Predefined

open GraphBLAS.FSharp

module FloatMonoid =
    let min: Monoid<float> = {
        Zero = System.Double.PositiveInfinity
        Append = BinaryOp <@ fun x y -> System.Math.Min(x, y) @>
    }

module FloatSemiring =
    let minPlus: Semiring<float> = {
        PlusMonoid = FloatMonoid.min
        Times = BinaryOp <@ ( + ) @>
    }
