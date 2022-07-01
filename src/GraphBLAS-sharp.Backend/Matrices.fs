namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp

type IDeviceMemObject =
    abstract Dispose : unit -> unit

type MatrixFromat =
    | CSR
    | COO

type Matrix<'a when 'a: struct> =
    | MatrixCSR of CSRMatrix<'a>
    | MatrixCOO of COOMatrix<'a>

    member this.RowCount =
        match this with
        | MatrixCSR matrix -> matrix.RowCount
        | MatrixCOO matrix -> matrix.RowCount

    member this.ColumnCount =
        match this with
        | MatrixCSR matrix -> matrix.ColumnCount
        | MatrixCOO matrix -> matrix.ColumnCount

    member this.Dispose() =
        match this with
        | MatrixCSR matrix -> (matrix :> IDeviceMemObject).Dispose()
        | MatrixCOO matrix -> (matrix :> IDeviceMemObject).Dispose()

and CSRMatrix<'elem when 'elem: struct> =
    { Context: ClContext
      RowCount: int
      ColumnCount: int
      RowPointers: ClArray<int>
      Columns: ClArray<int>
      Values: ClArray<'elem> }

    interface IDeviceMemObject with
        member this.Dispose() =
            let q = this.Context.QueueProvider.CreateQueue()
            q.Post(Msg.CreateFreeMsg<_>(this.Values))
            q.Post(Msg.CreateFreeMsg<_>(this.Columns))
            q.Post(Msg.CreateFreeMsg<_>(this.RowPointers))
            q.PostAndReply(Msg.MsgNotifyMe)

and COOMatrix<'elem when 'elem: struct> =
    { Context: ClContext
      RowCount: int
      ColumnCount: int
      Rows: ClArray<int>
      Columns: ClArray<int>
      Values: ClArray<'elem> }

    interface IDeviceMemObject with
        member this.Dispose() =
            let q = this.Context.QueueProvider.CreateQueue()
            q.Post(Msg.CreateFreeMsg<_>(this.Values))
            q.Post(Msg.CreateFreeMsg<_>(this.Columns))
            q.Post(Msg.CreateFreeMsg<_>(this.Rows))
            q.PostAndReply(Msg.MsgNotifyMe)

and TupleMatrix<'elem when 'elem: struct> =
    { Context: ClContext
      RowIndices: ClArray<int>
      ColumnIndices: ClArray<int>
      Values: ClArray<'elem> }

    interface IDeviceMemObject with
        member this.Dispose() =
            let q = this.Context.QueueProvider.CreateQueue()
            q.Post(Msg.CreateFreeMsg<_>(this.RowIndices))
            q.Post(Msg.CreateFreeMsg<_>(this.ColumnIndices))
            q.Post(Msg.CreateFreeMsg<_>(this.Values))
            q.PostAndReply(Msg.MsgNotifyMe)
