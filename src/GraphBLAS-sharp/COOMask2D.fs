namespace GraphBLAS.FSharp

type COORegularMask2D(rows: array<int>, columns: array<int>, rowCount: int, columnCount: int) =
    inherit Mask2D()

    member this.Rows = rows
    member this.Columns = columns

    override this.RowCount = rowCount
    override this.ColumnCount = columnCount

    override this.Item
        with get (rowIdx: int, colIdx: int) : bool =
            Array.zip this.Rows this.Columns |> Array.exists ( (=) (rowIdx, colIdx))
