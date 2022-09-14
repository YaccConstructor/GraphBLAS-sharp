namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp

type MaskType =
    | Regular
    | Complemented
    | NoMask

and Mask1D =
    { Context: ClContext
      IsComplemented: bool
      Size: int
      Indices: ClArray<int> }

    member this.NNZ = this.Indices.Length

    interface IDeviceMemObject with
        member this.Dispose q =
            q.Post(Msg.CreateFreeMsg<_>(this.Indices))
            q.PostAndReply(Msg.MsgNotifyMe)

and Mask2D =
    { Context: ClContext
      IsComplemented: bool
      RowCount: int
      ColumnCount: int
      Rows: ClArray<int>
      Columns: ClArray<int> }

    member this.NNZ = this.Rows.Length

    interface IDeviceMemObject with
        member this.Dispose q =
            q.Post(Msg.CreateFreeMsg<_>(this.Rows))
            q.Post(Msg.CreateFreeMsg<_>(this.Columns))
            q.PostAndReply(Msg.MsgNotifyMe)
