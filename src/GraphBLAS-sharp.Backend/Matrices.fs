namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp.OpenCL

type CSRMatrix<'elem when 'elem: struct> =
    { Context: ClContext      
      RowCount: int
      ColumnCount: int
      RowPointers: ClArray<int>
      Columns: ClArray<int>
      Values: ClArray<'elem> }

      interface System.IDisposable with 
        member this.Dispose() = 
          let q = this.Context.Provider.CommandQueue
          q.Post(Msg.CreateFreeMsg<_>(this.Values))
          q.Post(Msg.CreateFreeMsg<_>(this.Columns))
          q.Post(Msg.CreateFreeMsg<_>(this.RowPointers))
          q.PostAndReply(Msg.MsgNotifyMe)
           
type TupleMatrix<'elem when 'elem: struct> =
    { Context: ClContext
      RowIndices: ClArray<int>
      ColumnIndices: ClArray<int>
      Values: ClArray<'elem> }
    
    interface System.IDisposable with 
        member this.Dispose() = 
          let q = this.Context.Provider.CommandQueue
          q.Post(Msg.CreateFreeMsg<_>(this.RowIndices))
          q.Post(Msg.CreateFreeMsg<_>(this.ColumnIndices))
          q.Post(Msg.CreateFreeMsg<_>(this.Values))
          q.PostAndReply(Msg.MsgNotifyMe)

type COOMatrix<'elem when 'elem: struct> =
    { Context: ClContext
      RowCount: int
      ColumnCount: int
      Rows: ClArray<int>
      Columns: ClArray<int>
      Values: ClArray<'elem> }
    
    interface System.IDisposable with 
        member this.Dispose() = 
          let q = this.Context.Provider.CommandQueue
          q.Post(Msg.CreateFreeMsg<_>(this.Values))
          q.Post(Msg.CreateFreeMsg<_>(this.Columns))
          q.Post(Msg.CreateFreeMsg<_>(this.Rows))
          q.PostAndReply(Msg.MsgNotifyMe)

