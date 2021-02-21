namespace GraphBLAS.FSharp

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Matrix =
    let toSeq (matrix: Matrix<'a>) = failwith "Not Implemented"

    let build (rowCount: int) (columnCount: int) (rows: int[]) (columns: int[]) (values: 'a[]) (matrixType: MatrixBackendFormat) : Matrix<'a> =
        match matrixType with
        | CSR -> upcast CSRMatrix(rows, columns, values)
        | COO -> upcast COOMatrix(rowCount, columnCount, rows, columns, values)
        | _ -> failwith "Not Implemented"

    let ofArray2D (array: 'a[,]) (zero: 'a) (matrixType: MatrixBackendFormat) : Matrix<'a> =
        failwith "Not Implemented yet"

    let fromFile (pathToMatrix: string) (matrixType: MatrixBackendFormat) : Matrix<'a> =
        failwith "Not Implemented yet"

    let init (rowCount: int) (columnCount: int) (initializer: int -> int -> 'a) (matrixType: MatrixBackendFormat) : Matrix<'a> =
        failwith "Not Implemented yet"

    let zeroCreate (rowCount: int) (columnCount: int) (matrixType: MatrixBackendFormat) : Matrix<'a> =
        failwith "Not Implemented yet"
