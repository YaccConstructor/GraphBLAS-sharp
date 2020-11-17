namespace GraphBLAS.FSharp

type COORegularMask2D(rows: array<int>, columns: array<int>, rowCount: int, columnCount: int) =
    inherit Mask2D()

    let mutable rows = Array.distinct rows
    let mutable columns = Array.distinct columns

    override this.Size = Some (rowCount, columnCount)

    override this.Item
        with get (rowIdx: int, colIdx: int) : bool =
            Array.zip rows columns |> Array.exists ( (=) (rowIdx, colIdx))
        and set (rowIdx: int, colIdx: int) (mustExist: bool) = failwith "Not implemented"
