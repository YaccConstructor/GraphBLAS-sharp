namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp

<<<<<<< HEAD:src/GraphBLAS-sharp.Backend/Matrices.fs
type IDeviceMemObject =
    abstract Dispose : MailboxProcessor<Msg> -> unit

type MatrixFormat =
=======
type MatrixFromat =
>>>>>>> 835e7bfee1507ee5c31f5613e175368e7daf00d7:src/GraphBLAS-sharp.Backend/Objects/Matrix.fs
    | CSR
    | COO
    | CSC

type Matrix<'a when 'a: struct> =
    | MatrixCSR of CSRMatrix<'a>
    | MatrixCOO of COOMatrix<'a>
    | MatrixCSC of CSCMatrix<'a>

    member this.RowCount =
        match this with
        | MatrixCSR matrix -> matrix.RowCount
        | MatrixCOO matrix -> matrix.RowCount
        | MatrixCSC matrix -> matrix.RowCount

    member this.ColumnCount =
        match this with
        | MatrixCSR matrix -> matrix.ColumnCount
        | MatrixCOO matrix -> matrix.ColumnCount
        | MatrixCSC matrix -> matrix.ColumnCount

    member this.Dispose q =
        match this with
        | MatrixCSR matrix -> (matrix :> IDeviceMemObject).Dispose q
        | MatrixCOO matrix -> (matrix :> IDeviceMemObject).Dispose q
        | MatrixCSC matrix -> (matrix :> IDeviceMemObject).Dispose q

and CSRMatrix<'elem when 'elem: struct> =
    { Context: ClContext
      RowCount: int
      ColumnCount: int
      RowPointers: ClArray<int>
      Columns: ClArray<int>
      Values: ClArray<'elem> }

    interface IDeviceMemObject with
        member this.Dispose q =
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
        member this.Dispose q =
            q.Post(Msg.CreateFreeMsg<_>(this.Values))
            q.Post(Msg.CreateFreeMsg<_>(this.Columns))
            q.Post(Msg.CreateFreeMsg<_>(this.Rows))
            q.PostAndReply(Msg.MsgNotifyMe)

and CSCMatrix<'elem when 'elem: struct> =
    { Context: ClContext
      RowCount: int
      ColumnCount: int
      Rows: ClArray<int>
      ColumnPointers: ClArray<int>
      Values: ClArray<'elem> }

    interface IDeviceMemObject with
        member this.Dispose q =
            q.Post(Msg.CreateFreeMsg<_>(this.Values))
            q.Post(Msg.CreateFreeMsg<_>(this.Rows))
            q.Post(Msg.CreateFreeMsg<_>(this.ColumnPointers))
            q.PostAndReply(Msg.MsgNotifyMe)

and TupleMatrix<'elem when 'elem: struct> =
    { Context: ClContext
      RowIndices: ClArray<int>
      ColumnIndices: ClArray<int>
      Values: ClArray<'elem> }

    interface IDeviceMemObject with
        member this.Dispose q =
            q.Post(Msg.CreateFreeMsg<_>(this.RowIndices))
            q.Post(Msg.CreateFreeMsg<_>(this.ColumnIndices))
            q.Post(Msg.CreateFreeMsg<_>(this.Values))
            q.PostAndReply(Msg.MsgNotifyMe)
