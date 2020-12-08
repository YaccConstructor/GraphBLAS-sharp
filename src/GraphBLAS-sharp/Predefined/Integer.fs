namespace GraphBLAS.FSharp.Predefined

open GraphBLAS.FSharp

module IntegerMonoid =
    let add: Monoid<int> = {
        Zero = 0
        Append = BinaryOp <@ ( + ) @>
    }

    let min: Monoid<int> = {
        Zero = System.Int32.MaxValue
        Append = BinaryOp <@ fun x y -> System.Math.Min(x, y) @>
    }

module IntegerSemiring =
    let addMult: Semiring<int> = {
        PlusMonoid = IntegerMonoid.add
        Times = BinaryOp <@ ( * ) @>
    }

    let minFirst<'b> : Semiring<int, 'b, int> = {
        PlusMonoid = IntegerMonoid.min
        Times = BinaryOp <@ fun x y -> x @>
    }
