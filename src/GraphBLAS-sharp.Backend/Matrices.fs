namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp.OpenCL

type MatrixFromat =
    | CSR
    | COO

type Matrix<'a when 'a: struct> =
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

and CSRMatrix<'elem when 'elem: struct> =
    { RowCount: int
      ColumnCount: int
      RowPointers: ClArray<int>
      Columns: ClArray<int>
      Values: ClArray<'elem> }

and COOMatrix<'elem when 'elem: struct> =
    { RowCount: int
      ColumnCount: int
      Rows: ClArray<int>
      Columns: ClArray<int>
      Values: ClArray<'elem> }

and TupleMatrix<'elem when 'elem: struct> =
    { RowIndices: ClArray<int>
      ColumnIndices: ClArray<int>
      Values: ClArray<'elem> }
