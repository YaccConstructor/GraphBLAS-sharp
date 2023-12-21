namespace GraphBLAS.FSharp.Objects

open Brahma.FSharp
open GraphBLAS.FSharp.Objects

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
          Rows: ClVector.Sparse<'elem> option list }

        interface IDeviceMemObject with
            member this.Dispose q =
                this.Rows
                |> Seq.choose id
                |> Seq.iter (fun vector -> vector.Dispose q)

        member this.NNZ =
            this.Rows
            |> List.fold
                (fun acc row ->
                    match row with
                    | Some r -> acc + r.NNZ
                    | None -> acc)
                0

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

/// <summary>
/// Represents an abstraction over matrix, whose values and indices are in OpenCL device memory.
/// </summary>
[<RequireQualifiedAccess>]
type ClMatrix<'a when 'a: struct> =
    /// <summary>
    /// Represents an abstraction over matrix in CSR format, whose values and indices are in OpenCL device memory.
    /// </summary>
    | CSR of ClMatrix.CSR<'a>
    /// <summary>
    /// Represents an abstraction over matrix in COO format, whose values and indices are in OpenCL device memory.
    /// </summary>
    | COO of ClMatrix.COO<'a>
    /// <summary>
    /// Represents an abstraction over matrix in CSC format, whose values and indices are in OpenCL device memory.
    /// </summary>
    | CSC of ClMatrix.CSC<'a>
    /// <summary>
    /// Represents an abstraction over matrix in LIL format, whose values and indices are in OpenCL device memory.
    /// </summary>
    | LIL of ClMatrix.LIL<'a>

    /// <summary>
    /// Gets the number of rows in matrix.
    /// </summary>
    member this.RowCount =
        match this with
        | ClMatrix.CSR matrix -> matrix.RowCount
        | ClMatrix.COO matrix -> matrix.RowCount
        | ClMatrix.CSC matrix -> matrix.RowCount
        | ClMatrix.LIL matrix -> matrix.RowCount

    /// <summary>
    /// Gets the number of columns in matrix.
    /// </summary>
    member this.ColumnCount =
        match this with
        | ClMatrix.CSR matrix -> matrix.ColumnCount
        | ClMatrix.COO matrix -> matrix.ColumnCount
        | ClMatrix.CSC matrix -> matrix.ColumnCount
        | ClMatrix.LIL matrix -> matrix.ColumnCount

    /// <summary>
    /// Release device resources allocated for the matrix.
    /// </summary>
    member this.Dispose q =
        match this with
        | ClMatrix.CSR matrix -> (matrix :> IDeviceMemObject).Dispose q
        | ClMatrix.COO matrix -> (matrix :> IDeviceMemObject).Dispose q
        | ClMatrix.CSC matrix -> (matrix :> IDeviceMemObject).Dispose q
        | ClMatrix.LIL matrix -> (matrix :> IDeviceMemObject).Dispose q

    /// <summary>
    /// Gets the number of non-zero elements in matrix.
    /// </summary>
    member this.NNZ =
        match this with
        | ClMatrix.CSR matrix -> matrix.NNZ
        | ClMatrix.COO matrix -> matrix.NNZ
        | ClMatrix.CSC matrix -> matrix.NNZ
        | ClMatrix.LIL matrix -> matrix.NNZ
