namespace GraphBLAS.FSharp.Backend.Objects

open Brahma.FSharp

type MatrixFormat =
    | CSR
    | COO
    | CSC

type ClMatrix<'a when 'a: struct> =
    | MatrixCSR of ClCSRMatrix<'a>
    | MatrixCOO of ClCOOMatrix<'a>
    | MatrixCSC of ClCSCMatrix<'a>

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

and ClCSRMatrix<'elem when 'elem: struct> =
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

and ClCOOMatrix<'elem when 'elem: struct> =
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

and ClCSCMatrix<'elem when 'elem: struct> =
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

and ClTupleMatrix<'elem when 'elem: struct> =
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
