namespace GraphBLAS.FSharp

open GraphBLAS.FSharp.Predefined

module Algorithms =
    let sandiaTriangleCount (lowerTriangular: Matrix<bool>) =
        let c = (lowerTriangular +.* lowerTriangular.T) (Mask2D.regular lowerTriangular) BooleanSemiring.anyAll
        c.Reduce BooleanMonoid.any
