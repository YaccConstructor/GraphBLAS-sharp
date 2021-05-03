namespace GraphBLAS.FSharp.Predefined

open GraphBLAS.FSharp

module AnyAll =
    let bool: Semiring<bool> =
        {
            PlusMonoid = Any.bool
            TimesSemigroup = { AssociativeOp = ClosedBinaryOp <@ (&&) @> }
        }

    // type B private () =
    //     static let instance = B()
    //     static member Instance = instance
    //     member this.Pl() = bool.PlusMonoid.AssociativeOp


// type AnyAll2<'a> =
//     member this.P =
