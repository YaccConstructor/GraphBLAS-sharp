namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp

type MaskType1D =
    | Regular of Mask1D
    | Complemented of Mask1D
    | NoMask

    member this.Size =
        match this with
        | Regular      mask -> mask.Size
        | Complemented mask -> mask.Size
        | NoMask            -> 0

    member this.NNZ =
        match this with
        | Regular      mask -> mask.Indices.Length
        | Complemented mask -> mask.Indices.Length
        | NoMask            -> 0

    member this.Dispose q =
        match this with
        | Regular      mask -> (mask :> IDeviceMemObject).Dispose q
        | Complemented mask -> (mask :> IDeviceMemObject).Dispose q
        | NoMask            -> ()

and Mask1D =
    { Context: ClContext
      Size: int
      Indices: ClArray<int> }

    interface IDeviceMemObject with
        member this.Dispose q =
            q.Post(Msg.CreateFreeMsg<_>(this.Indices))
            q.PostAndReply(Msg.MsgNotifyMe)


type MaskType2D =
    | Regular of Mask2D
    | Complemented of Mask2D
    | NoMask

    member this.RowCount =
        match this with
        | Regular      mask -> mask.RowCount
        | Complemented mask -> mask.RowCount
        | NoMask            -> 0

    member this.ColumnCount =
        match this with
        | Regular      mask -> mask.ColumnCount
        | Complemented mask -> mask.ColumnCount
        | NoMask            -> 0

    member this.NNZ =
        match this with
        | Regular      mask -> mask.Rows.Length
        | Complemented mask -> mask.Rows.Length
        | NoMask            -> 0

    member this.Dispose q =
        match this with
        | Regular      mask -> (mask :> IDeviceMemObject).Dispose q
        | Complemented mask -> (mask :> IDeviceMemObject).Dispose q
        | NoMask            -> ()

and Mask2D =
    { Context: ClContext
      RowCount: int
      ColumnCount: int
      Rows: ClArray<int>
      Columns: ClArray<int> }

    interface IDeviceMemObject with
        member this.Dispose q =
            q.Post(Msg.CreateFreeMsg<_>(this.Rows))
            q.Post(Msg.CreateFreeMsg<_>(this.Columns))
            q.PostAndReply(Msg.MsgNotifyMe)
