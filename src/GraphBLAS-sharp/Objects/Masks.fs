namespace GraphBLAS.FSharp.Objects

open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Objects

type MaskType =
    | Regular
    | Complemented
    | NoMask

type Mask1D =
    { IsComplemented: bool
      Size: int
      Indices: int [] }

    member this.NNZ = this.Indices.Length

    member this.ToBackend(context: ClContext) =
        let indices = context.CreateClArray this.Indices

        { ClMask1D.Context = context
          IsComplemented = this.IsComplemented
          Size = this.Size
          Indices = indices }

    static member FromBackend (q: MailboxProcessor<_>) (mask: ClMask1D) =
        let indices = Array.zeroCreate mask.NNZ

        let _ =
            q.PostAndReply(fun ch -> Msg.CreateToHostMsg(mask.Indices, indices, ch))

        { IsComplemented = mask.IsComplemented
          Size = mask.Size
          Indices = indices }

    static member FromArray(array: 'a [], isZero: 'a -> bool, isComplemented: bool) =
        let (indices, _) =
            array
            |> Seq.cast<'a>
            |> Seq.mapi (fun idx v -> (idx, v))
            |> Seq.filter (fun (_, v) -> not <| isZero v)
            |> Array.ofSeq
            |> Array.unzip

        { IsComplemented = isComplemented
          Size = Array.length array
          Indices = indices }

    static member FromArray(array: 'a [], isZero: 'a -> bool) = Mask1D.FromArray(array, isZero, false)

type Mask2D =
    { IsComplemented: bool
      RowCount: int
      ColumnCount: int
      Rows: int []
      Columns: int [] }

    member this.NNZ = this.Rows.Length

    member this.ToBackend(context: ClContext) =
        let rows = context.CreateClArray this.Rows
        let columns = context.CreateClArray this.Columns

        { ClMask2D.Context = context
          IsComplemented = this.IsComplemented
          RowCount = this.RowCount
          ColumnCount = this.ColumnCount
          Rows = rows
          Columns = columns }

    static member FromBackend (q: MailboxProcessor<_>) (mask: ClMask2D) =
        let rows = Array.zeroCreate mask.Rows.Length
        let columns = Array.zeroCreate mask.Columns.Length

        let _ =
            q.Post(Msg.CreateToHostMsg(mask.Rows, rows))

        let _ =
            q.PostAndReply(fun ch -> Msg.CreateToHostMsg(mask.Columns, columns, ch))

        { IsComplemented = mask.IsComplemented
          RowCount = mask.RowCount
          ColumnCount = mask.ColumnCount
          Rows = rows
          Columns = columns }

    static member FromArray2D(array: 'a [,], isZero: 'a -> bool, isComplemented: bool) =
        let (rows, cols, _) =
            array
            |> Seq.cast<'a>
            |> Seq.mapi (fun idx v -> (idx / Array2D.length2 array, idx % Array2D.length2 array, v))
            |> Seq.filter (fun (_, _, v) -> not <| isZero v)
            |> Array.ofSeq
            |> Array.unzip3

        { IsComplemented = isComplemented
          RowCount = Array2D.length1 array
          ColumnCount = Array2D.length2 array
          Rows = rows
          Columns = cols }

    static member FromArray2D(array: 'a [,], isZero: 'a -> bool) =
        Mask2D.FromArray2D(array, isZero, false)
