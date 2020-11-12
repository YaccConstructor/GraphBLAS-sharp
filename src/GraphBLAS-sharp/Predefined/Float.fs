namespace GraphBLAS.FSharp.Predefined

open GraphBLAS.FSharp

type FloatMonoid =
    static member Min: Monoid<float> = {
        Zero = System.Double.PositiveInfinity
        Append = BinaryOp <@ fun x y -> System.Math.Min(x, y) @>
    }

type FloatSemiring =
    static member MinPlus: Semiring<float> = {
        PlusMonoid = FloatMonoid.Min
        Times = BinaryOp <@ ( + ) @>
    }
