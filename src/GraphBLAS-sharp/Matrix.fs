namespace GraphBLAS.FSharp

open Backend

[<AutoOpen>]
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Matrix =
    type Matrix<'a when 'a : struct and 'a : equality> with
        static member Build(matrix: 'b[,]) : Matrix<'b> = Matrix.Build(matrix, matrixBackendFormat)
        static member Build(filename: string) : Matrix<'b> = failwith "Not Implemented"
        static member ZeroCreate(rowCount: int, columnCount: int) : Matrix<'b> = Matrix.ZeroCreate(rowCount, columnCount)

