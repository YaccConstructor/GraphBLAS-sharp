namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp

type MaskType =
    | Regular
    | Complemented
    | NoMask

type Mask1D =
    { Context: ClContext
      IsComplemented: bool
      Size: int
      Indices: ClArray<int> }

    member this.NNZ = this.Indices.Length

    member this.Dispose(q: MailboxProcessor<_>) =
        q.Post(Msg.CreateFreeMsg<_>(this.Indices))
        q.PostAndReply(Msg.MsgNotifyMe)

type Mask2D =
    { Context: ClContext
      IsComplemented: bool
      RowCount: int
      ColumnCount: int
      Rows: ClArray<int>
      Columns: ClArray<int> }

    member this.NNZ = this.Rows.Length

    member this.Dispose(q: MailboxProcessor<_>) =
        q.Post(Msg.CreateFreeMsg<_>(this.Rows))
        q.Post(Msg.CreateFreeMsg<_>(this.Columns))
        q.PostAndReply(Msg.MsgNotifyMe)
