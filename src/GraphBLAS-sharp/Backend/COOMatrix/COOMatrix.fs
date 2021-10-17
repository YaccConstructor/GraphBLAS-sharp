namespace GraphBLAS.FSharp.Backend.COOMatrix

open Brahma.FSharp.OpenCL

type GpuCOOMatrix<'elem when 'elem: struct> =
    { RowCount: int
      ColumnCount: int
      Rows: ClArray<int>
      Columns: ClArray<int>
      Values: ClArray<'elem> }
