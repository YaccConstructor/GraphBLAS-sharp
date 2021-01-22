namespace GraphBLAS.FSharp

module MatrixBackend =
    type Matrix<'a when 'a : struct and 'a : equality> with
        static member Build(denseMatrix: 'T[,], zero: 'T, matrixType: MatrixBackendFormat) : Matrix<'T> =
            failwith "Not Implemented"

        static member Build(rowCount: int, columnCount: int, rows: int[], columns: int[], values: 'T[], matrixType: MatrixBackendFormat) : Matrix<'T> =
            match matrixType with
            | CSR -> upcast CSRMatrix(rows, columns, values)
            | _ -> failwith "Not Implemented"

        static member Build(pathToMatrix: string, matrixType: MatrixBackendFormat) : Matrix<'T> =
            failwith "Not Implemented"

        static member Build(rowCount: int, columnCount: int, initializer: int -> int -> 'T, matrixType: MatrixBackendFormat) : Matrix<'T> =
            failwith "Not Implemented"

        static member ZeroCreate(rowCount: int, columnCount: int, matrixType: MatrixBackendFormat) : Matrix<'T> =
            failwith "Not Implemented"

[<AutoOpen>]
module MatrixExtensions =
    open MatrixBackend
    type Matrix<'a when 'a : struct and 'a : equality> with
        static member Build(denseMatrix: 'T[,], zero: 'T) : Matrix<'T> =
            Matrix.Build(denseMatrix, zero, matrixBackendFormat)

        static member Build(rowCount: int, columnCount: int, rows: int[], columns: int[], values: 'T[]) : Matrix<'T> =
            Matrix.Build(rowCount, columnCount, rows, columns, values)

        static member Build(pathToMatrix: string) : Matrix<'T> =
            Matrix.Build(pathToMatrix, matrixBackendFormat)

        static member Build(rowCount: int, columnCount: int, initializer: int -> int -> 'T) : Matrix<'T> =
            Matrix.Build(rowCount, columnCount, initializer, matrixBackendFormat)

        static member ZeroCreate(rowCount: int, columnCount: int) : Matrix<'T> =
            Matrix.ZeroCreate(rowCount, columnCount)

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Matrix =
    let toSeq (matrix: Matrix<'a>) = failwith "Not Implemented"
