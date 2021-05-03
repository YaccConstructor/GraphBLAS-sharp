namespace GraphBLAS.FSharp

type MaskType =
    | Regular
    | Complemented
    | NoMask

// type Mask1D =
//     | MaskCOO of COOPattern * isComplemented: bool

// and COOPattern =
//     {
//         Size: int
//         Indices: int[]
//     }

type Mask1D(indices: int[], size: int, isComplemented: bool) =
    member this.Indices = indices
    member this.Size = size
    member this.IsComplemented = isComplemented


type Mask2D(rowIndices: int[], columnIndices: int[], rowCount: int, columnCount: int, isComplemented: bool) =
    member this.RowIndices = rowIndices
    member this.ColumnIndices = columnIndices
    member this.RowCount = rowCount
    member this.ColumnCount = columnCount
    member this.IsComplemented = isComplemented
