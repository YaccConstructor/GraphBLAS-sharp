namespace GraphBLAS.FSharp.Backend.Objects

open Brahma.FSharp

type MatrixFormat =
    | CSR
    | COO
    | CSC
    | LIL

module ClMatrix =
    type CSR<'elem when 'elem: struct> =
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

        member this.Dispose q = (this :> IDeviceMemObject).Dispose q

        member this.NNZ = this.Values.Length

        member this.ToCSC =
            { Context = this.Context
              RowCount = this.ColumnCount
              ColumnCount = this.RowCount
              Rows = this.Columns
              ColumnPointers = this.RowPointers
              Values = this.Values }

    and CSC<'elem when 'elem: struct> =
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

        member this.Dispose q = (this :> IDeviceMemObject).Dispose q

        member this.NNZ = this.Values.Length

        member this.ToCSR =
            { Context = this.Context
              RowCount = this.ColumnCount
              ColumnCount = this.RowCount
              RowPointers = this.ColumnPointers
              Columns = this.Rows
              Values = this.Values }

    type COO<'elem when 'elem: struct> =
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

        member this.Dispose q = (this :> IDeviceMemObject).Dispose q

        member this.NNZ = this.Values.Length

    type LIL<'elem when 'elem: struct> =
        { Context: ClContext
          RowCount: int
          ColumnCount: int
          Rows: ClVector.Sparse<'elem> option list
          NNZ: int }

        interface IDeviceMemObject with
            member this.Dispose q =
                this.Rows
                |> Seq.choose id
                |> Seq.iter (fun vector -> vector.Dispose q)

    type Tuple<'elem when 'elem: struct> =
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

        member this.Dispose q = (this :> IDeviceMemObject).Dispose q

        member this.NNZ = this.Values.Length

[<RequireQualifiedAccess>]
type ClMatrix<'a when 'a: struct> =
    | CSR of ClMatrix.CSR<'a>
    | COO of ClMatrix.COO<'a>
    | CSC of ClMatrix.CSC<'a>
    | LIL of ClMatrix.LIL<'a>

    member this.RowCount =
        match this with
        | ClMatrix.CSR matrix -> matrix.RowCount
        | ClMatrix.COO matrix -> matrix.RowCount
        | ClMatrix.CSC matrix -> matrix.RowCount
        | ClMatrix.LIL matrix -> matrix.RowCount

    member this.ColumnCount =
        match this with
        | ClMatrix.CSR matrix -> matrix.ColumnCount
        | ClMatrix.COO matrix -> matrix.ColumnCount
        | ClMatrix.CSC matrix -> matrix.ColumnCount
        | ClMatrix.LIL matrix -> matrix.ColumnCount

    member this.Dispose q =
        match this with
        | ClMatrix.CSR matrix -> (matrix :> IDeviceMemObject).Dispose q
        | ClMatrix.COO matrix -> (matrix :> IDeviceMemObject).Dispose q
        | ClMatrix.CSC matrix -> (matrix :> IDeviceMemObject).Dispose q
        | ClMatrix.LIL matrix -> (matrix :> IDeviceMemObject).Dispose q

    member this.NNZ =
        match this with
        | ClMatrix.CSR matrix -> matrix.NNZ
        | ClMatrix.COO matrix -> matrix.NNZ
        | ClMatrix.CSC matrix -> matrix.NNZ
        | ClMatrix.LIL matrix -> matrix.NNZ
