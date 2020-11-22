namespace GraphBLAS.FSharp

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Mask1D =
    let regular (vector: Vector<'a>) : Mask1D = failwith "Not Implemented"
    let complemented (vector: Vector<'a>) : Mask1D = failwith "Not Implemented"
    let none : Mask1D = failwith "Not Implemented"

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Mask2D =
    let regular (matrix: Matrix<'a>) : Mask2D = failwith "Not Implemented"
    let complemented (matrix: Matrix<'a>) : Mask2D = failwith "Not Implemented"
    let none : Mask2D = failwith "Not Implemented"
