namespace GraphBLAS.FSharp

// matrixFormat
type MatrixType =
    | CSR
    | COO

type Matrix<'a when 'a : struct> =
    | MatrixCSR of CSRMatrix<'a>
    | MatrixCOO of COOMatrix<'a>

    member this.RowCount =
        match this with
        | MatrixCSR matrix -> matrix.RowCount
        | MatrixCOO matrix -> matrix.RowCount

    member this.ColumnCount =
        match this with
        | MatrixCSR matrix -> matrix.ColumnCount
        | MatrixCOO matrix -> matrix.ColumnCount

and CSRMatrix<'a> =
    {
        RowCount: int
        ColumnCount: int
        RowPointers: int[]
        ColumnIndices: int[]
        Values: 'a[]
    }

    static member FromFile(pathToMatrix: string) : CSRMatrix<'a> =
        failwith "Not Implemented yet"

and COOMatrix<'a> =
    {
        RowCount: int
        ColumnCount: int
        Rows: int[]
        Columns: int[]
        Values: 'a[]
    }

    override this.ToString() =
        [
            sprintf "COO Matrix     %ix%i \n" this.RowCount this.ColumnCount
            sprintf "RowIndices:    %A \n" this.Rows
            sprintf "ColumnIndices: %A \n" this.Columns
            sprintf "Values:        %A \n" this.Values
        ]
        |> String.concat ""

    static member FromTuples(rowCount: int, columnCount: int, rows: int[], columns: int[], values: 'a[]) =
        {
            RowCount = rowCount
            ColumnCount = columnCount
            Rows = rows
            Columns = columns
            Values = values
        }

    static member FromArray2D(array: 'a[,], isZero: 'a -> bool) =
        let (rows, cols, vals) =
            array
            |> Seq.cast<'a>
            |> Seq.mapi (fun idx v -> (idx / Array2D.length2 array, idx % Array2D.length2 array, v))
            |> Seq.filter (fun (_, _, v) -> not <| isZero v)
            |> Array.ofSeq
            |> Array.unzip3

        COOMatrix.FromTuples(Array2D.length1 array, Array2D.length2 array, rows, cols, vals)

type MatrixTuples<'a> =
    {
        RowIndices: int[]
        ColumnIndices: int[]
        Values: 'a[]
    }
