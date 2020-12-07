namespace GraphBLAS.FSharp.Predefined

open GraphBLAS.FSharp

module FloatMonoid =
    let add: Monoid<float> = {
        Zero = 0.
        Append = BinaryOp <@ ( + ) @>
    }

    let min: Monoid<float> = {
        Zero = System.Double.PositiveInfinity
        Append = BinaryOp <@ fun x y -> System.Math.Min(x, y) @>
    }

module FloatSemiring =
    let addMult: Semiring<float> = {
        PlusMonoid = FloatMonoid.add
        Times = BinaryOp <@ ( * ) @>
    }

    let minAdd: Semiring<float> = {
        PlusMonoid = FloatMonoid.min
        Times = BinaryOp <@ ( + ) @>
    }
