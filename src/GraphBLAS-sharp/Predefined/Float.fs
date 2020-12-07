namespace GraphBLAS.FSharp.Predefined

open GraphBLAS.FSharp

module FloatMonoid =
    let plus: Monoid<float> = {
        Zero = 0.
        Append = BinaryOp <@ ( + ) @>
    }

    let min: Monoid<float> = {
        Zero = System.Double.PositiveInfinity
        Append = BinaryOp <@ fun x y -> System.Math.Min(x, y) @>
    }

module FloatSemiring =
    let plusTimes: Semiring<float> = {
        PlusMonoid = FloatMonoid.plus
        Times = BinaryOp <@ ( * ) @>
    }

    let minPlus: Semiring<float> = {
        PlusMonoid = FloatMonoid.min
        Times = BinaryOp <@ ( + ) @>
    }
