namespace GraphBLAS.FSharp

type Matrix<'a> =
    | MatrixCSR of CSRMatrix<'a>
    | MatrixCOO of COOMatrix<'a>

and CSRMatrix<'a> =
    {
        ColumnCount: int
        RowPointers: int[]
        ColumnIndices: int[]
        Values: 'a[]
    }

and COOMatrix<'a> =
    {
        RowCount: int
        ColumnCount: int
        Rows: int[]
        Columns: int[]
        Values: 'a[]
    }

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
            |> Seq.filter (fun (i, j, v) -> not <| isZero v)
            |> Array.ofSeq
            |> Array.unzip3

        COOMatrix.FromTuples(Array2D.length1 array, Array2D.length2 array, rows, cols, vals)
