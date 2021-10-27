namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp.OpenCL

type CSRMatrix<'elem when 'elem: struct> =
    { RowCount: int
      ColumnCount: int
      RowPointers: ClArray<int>
      Columns: ClArray<int>
      Values: ClArray<'elem> }

type TupleMatrix<'elem when 'elem: struct> =
    { RowIndices: ClArray<int>
      ColumnIndices: ClArray<int>
      Values: ClArray<'elem> }

type COOMatrix<'elem when 'elem: struct> =
    { RowCount: int
      ColumnCount: int
      Rows: ClArray<int>
      Columns: ClArray<int>
      Values: ClArray<'elem> }
