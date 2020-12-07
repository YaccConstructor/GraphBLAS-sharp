namespace GraphBLAS.FSharp

module MatrixBackend =
    // можно убрать перегрузку и добавить возможность partitial application
    type Matrix<'a when 'a : struct and 'a : equality> with
        static member Build(denseMatrix: 'T[,], zero: 'T, matrixType: MatrixBackendFormat) : Matrix<'T> =
            failwith "Not Implemented"
        static member Build(pathToMatrix: string, matrixType: MatrixBackendFormat) : Matrix<'T> =
            failwith "Not Implemented"
        static member Build(rowCount: int, columnCount: int, initializer: int -> int -> 'T, matrixType: MatrixBackendFormat) : Matrix<'T> =
            failwith "Not Implemented"
        static member ZeroCreate(rowCount: int, columnCount: int, matrixType: MatrixBackendFormat) : Matrix<'T> =
            failwith "Not Implemented"

[<AutoOpen>]
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Matrix =
    open MatrixBackend
    type Matrix<'a when 'a : struct and 'a : equality> with
        static member Build(denseMatrix: 'T[,], zero: 'T) : Matrix<'T> =
            Matrix.Build(denseMatrix, zero, matrixBackendFormat)
        static member Build(pathToMatrix: string) : Matrix<'T> =
            Matrix.Build(pathToMatrix, matrixBackendFormat)
        static member Build(rowCount: int, columnCount: int, initializer: int -> int -> 'T) : Matrix<'T> =
            Matrix.Build(rowCount, columnCount, initializer, matrixBackendFormat)
        static member ZeroCreate(rowCount: int, columnCount: int) : Matrix<'T> =
            Matrix.ZeroCreate(rowCount, columnCount)

