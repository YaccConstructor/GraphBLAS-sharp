namespace GraphBLAS.FSharp

open Brahma.FSharp
open GraphBLAS.FSharp.Backend

type Mat<'a when 'a: struct> =
    abstract RowCount : int
    abstract ColumnCount: int
    abstract NNZCount : int
