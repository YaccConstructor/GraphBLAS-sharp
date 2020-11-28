namespace GraphBLAS.FSharp.Algorithms

open GraphBLAS.FSharp.Predefined
open GraphBLAS.FSharp

[<AutoOpen>]
module TriangleCounting =
    let sandiaTriangleCount (lowerTriangular: Matrix<bool>) =
        let c = (lowerTriangular .@ lowerTriangular.T) (Mask2D.regular lowerTriangular) BooleanSemiring.anyAll
        c.Reduce BooleanMonoid.any
