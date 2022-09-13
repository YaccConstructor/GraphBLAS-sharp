namespace GraphBLAS.FSharp

// type MaskType =
//     | Regular
//     | Complemented
//     | NoMask

// type Mask1D(indices: int [], size: int, isComplemented: bool) =
//     member this.Indices = indices
//     member this.Size = size
//     member this.IsComplemented = isComplemented

// type Mask2D(rowIndices: int [], columnIndices: int [], rowCount: int, columnCount: int, isComplemented: bool) =
//     member this.RowIndices = rowIndices
//     member this.ColumnIndices = columnIndices
//     member this.RowCount = rowCount
//     member this.ColumnCount = columnCount
//     member this.IsComplemented = isComplemented

type MaskType1D =
    | Regular of Mask1D
    | Complemented of Mask1D
    | NoMask

and Mask1D =
    { Size: int
      Indices: int [] }


type MaskType2D =
    | Regular of Mask2D
    | Complemented of Mask2D
    | NoMask

and Mask2D =
    { RowCount: int
      ColumnCount: int
      Rows: int []
      Columns: int [] }

    static member FromArray2D(array: 'a [,], isZero: 'a -> bool) =
        let (rows, cols, _) =
            array
            |> Seq.cast<'a>
            |> Seq.mapi (fun idx v -> (idx / Array2D.length2 array, idx % Array2D.length2 array, v))
            |> Seq.filter (fun (_, _, v) -> not <| isZero v)
            |> Array.ofSeq
            |> Array.unzip3

        { RowCount = Array2D.length1 array
          ColumnCount = Array2D.length2 array
          Rows = rows
          Columns = cols }
