namespace GraphBLAS.FSharp.Algorithms

open GraphBLAS.FSharp.Predefined
open GraphBLAS.FSharp

[<AutoOpen>]
module TriangleCounting =
    let sandiaTriangleCount (lowerTriangular: Matrix<bool>) =
        let bool2int = function
            | true -> 1
            | false -> 0
        let convertedMatrix = lowerTriangular.Apply None (UnaryOp <@ bool2int @>)
        let result = (convertedMatrix @. convertedMatrix.T) lowerTriangular.Mask IntegerSemiring.addMult
        result.Reduce IntegerMonoid.add
