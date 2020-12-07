namespace GraphBLAS.FSharp

module Backend =
    type Matrix<'a when 'a : struct and 'a : equality> with
        static member Build(matrix: 'b[,], matrixType: MatrixBackendFormat) : Matrix<'b> = failwith "Not Implemented"
        static member ZeroCreate(rowCount: int, columnCount: int, matrixType: MatrixBackendFormat) : Matrix<'b> = failwith "Not Implemented"

    // type Vector<'a when 'a : struct and 'a : equality> with
    //     static member ZeroCreate
