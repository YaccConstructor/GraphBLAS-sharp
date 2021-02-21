namespace GraphBLAS.FSharp

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Matrix =
    let build (rowCount: int) (columnCount: int) (rows: int[]) (columns: int[]) (values: 'a[]) : Matrix<'a> =
        failwith "Not Implemented yet"

    let ofArray2D (array: 'a[,]) (checkZero: 'a -> bool) : Matrix<'a> =
        failwith "Not Implemented yet"

    let fromFile (pathToMatrix: string) : Matrix<'a> =
        failwith "Not Implemented yet"

    let init (rowCount: int) (columnCount: int) (initializer: int -> int -> 'a) : Matrix<'a> =
        failwith "Not Implemented yet"

    let zeroCreate (rowCount: int) (columnCount: int) : Matrix<'a> =
        failwith "Not Implemented yet"
