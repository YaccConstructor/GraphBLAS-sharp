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
