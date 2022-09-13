namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp
open GraphBLAS.FSharp.Backend

type IDeviceMemObject =
    abstract Dispose : MailboxProcessor<Msg> -> unit

type MatrixFormat =
    | CSR
    | COO

type CSRMatrix<'a when 'a : struct> =
    {
        RowCount: int
        ColumnCount: int
        RowPointers: int []
        ColumnIndices: int []
        Values: 'a []
    }

    member this.ToDevice(context: ClContext) =
        let rows = context.CreateClArray this.RowPointers
        let columns = context.CreateClArray this.ColumnIndices
        let values = context.CreateClArray this.Values

        {
            Context = context
            RowCount = this.RowCount
            ColumnCount = this.ColumnCount
            RowPointers = rows
            Columns = columns
            Values = values
        }

    static member FromArray2D(array: 'a [,], isZero: 'a -> bool) =
        let rowsCount = array |> Array2D.length1
        let columnsCount = array |> Array2D.length2

        let convertedMatrix =
            [ for i in 0 .. rowsCount - 1 -> array.[i, *] |> List.ofArray ]
            |> List.map (fun row ->
                row
                |> List.mapi (fun i x -> (x, i))
                |> List.filter (fun pair -> not <| isZero (fst pair))
            )
            |> List.fold
                (fun (rowPtrs, valueInx) row -> ((rowPtrs.Head + row.Length) :: rowPtrs), valueInx @ row)
                ([ 0 ], [])

        {
            Values = convertedMatrix |> (snd >> List.unzip >> fst) |> List.toArray
            ColumnIndices = convertedMatrix |> (snd >> List.unzip >> snd) |> List.toArray
            RowPointers = convertedMatrix |> fst |> List.rev |> List.toArray
            RowCount = rowsCount
            ColumnCount = columnsCount
        }

and ClCsrMatrix<'elem when 'elem: struct> =
    {
        Context: ClContext
        RowCount: int
        ColumnCount: int
        RowPointers: ClArray<int>
        Columns: ClArray<int>
        Values: ClArray<'elem>
    }

    member this.ToHost(q: MailboxProcessor<_>) =
        let rows = Array.zeroCreate this.RowPointers.Length
        let columns = Array.zeroCreate this.Columns.Length
        let values = Array.zeroCreate this.Values.Length

        let _ = q.Post(Msg.CreateToHostMsg(this.RowPointers, rows))
        let _ = q.Post(Msg.CreateToHostMsg(this.Columns, columns))
        let _ = q.PostAndReply(fun ch -> Msg.CreateToHostMsg(this.Values, values, ch))

        {
            RowCount = this.RowCount
            ColumnCount = this.ColumnCount
            RowPointers = rows
            ColumnIndices = columns
            Values = values
        }

    interface IDeviceMemObject with
        member this.Dispose q =
            q.Post(Msg.CreateFreeMsg<_>(this.Values))
            q.Post(Msg.CreateFreeMsg<_>(this.Columns))
            q.Post(Msg.CreateFreeMsg<_>(this.RowPointers))
            q.PostAndReply(Msg.MsgNotifyMe)

    member this.Dispose(q) = (this :> IDeviceMemObject).Dispose q

type COOMatrix<'a when 'a : struct> =
    {
        RowCount: int
        ColumnCount: int
        Rows: int []
        Columns: int []
        Values: 'a []
    }

    member this.ToDevice(context: ClContext) =
        let rows = context.CreateClArray this.Rows
        let columns = context.CreateClArray this.Columns
        let values = context.CreateClArray this.Values

        {
            Context = context
            RowCount = this.RowCount
            ColumnCount = this.ColumnCount
            Rows = rows
            Columns = columns
            Values = values
        }

    override this.ToString() =
        [
            sprintf "COO Matrix     %ix%i \n" this.RowCount this.ColumnCount
            sprintf "RowIndices:    %A \n" this.Rows
            sprintf "ColumnIndices: %A \n" this.Columns
            sprintf "Values:        %A \n" this.Values
        ]
        |> String.concat ""

    static member FromTuples(rowCount: int, columnCount: int, rows: int [], columns: int [], values: 'a []) =
        {
            RowCount = rowCount
            ColumnCount = columnCount
            Rows = rows
            Columns = columns
            Values = values
        }

    static member FromArray2D(array: 'a [,], isZero: 'a -> bool) =
        let (rows, cols, vals) =
            array
            |> Seq.cast<'a>
            |> Seq.mapi (fun idx v -> (idx / Array2D.length2 array, idx % Array2D.length2 array, v))
            |> Seq.filter (fun (_, _, v) -> not <| isZero v)
            |> Array.ofSeq
            |> Array.unzip3

        COOMatrix.FromTuples(Array2D.length1 array, Array2D.length2 array, rows, cols, vals)

and ClCooMatrix<'elem when 'elem: struct> =
    {
        Context: ClContext
        RowCount: int
        ColumnCount: int
        Rows: ClArray<int>
        Columns: ClArray<int>
        Values: ClArray<'elem>
    }

    member this.ToHost(q: MailboxProcessor<_>) =
        let rows = Array.zeroCreate this.Rows.Length
        let columns = Array.zeroCreate this.Columns.Length
        let values = Array.zeroCreate this.Values.Length

        let _ = q.Post(Msg.CreateToHostMsg(this.Rows, rows))
        let _ = q.Post(Msg.CreateToHostMsg(this.Columns, columns))
        let _ = q.PostAndReply(fun ch -> Msg.CreateToHostMsg(this.Values, values, ch))

        {
            RowCount = this.RowCount
            ColumnCount = this.ColumnCount
            Rows = rows
            Columns = columns
            Values = values
        }

    interface IDeviceMemObject with
        member this.Dispose(q) =
            q.Post(Msg.CreateFreeMsg<_>(this.Values))
            q.Post(Msg.CreateFreeMsg<_>(this.Columns))
            q.Post(Msg.CreateFreeMsg<_>(this.Rows))
            q.PostAndReply(Msg.MsgNotifyMe)

    member this.Dispose(q) = (this :> IDeviceMemObject).Dispose q

type MatrixTuples<'a> =
    {
        RowIndices: int []
        ColumnIndices: int []
        Values: 'a []
    }

and ClTupleMatrix<'elem when 'elem: struct> =
    {
        Context: ClContext
        RowIndices: ClArray<int>
        ColumnIndices: ClArray<int>
        Values: ClArray<'elem>
    }

    interface IDeviceMemObject with
        member this.Dispose(q) =
            q.Post(Msg.CreateFreeMsg<_>(this.RowIndices))
            q.Post(Msg.CreateFreeMsg<_>(this.ColumnIndices))
            q.Post(Msg.CreateFreeMsg<_>(this.Values))
            q.PostAndReply(Msg.MsgNotifyMe)

// or HostMatrix?
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

    member this.NNZCount =
        match this with
        | MatrixCSR m -> m.Values.Length
        | MatrixCOO m -> m.Values.Length

    member this.ToDevice(context: ClContext) =
        match this with
        | MatrixCSR m -> ClMatrixCSR <| m.ToDevice(context)
        | MatrixCOO m -> ClMatrixCOO <| m.ToDevice(context)

// or DeviceMatrix?
and ClMatrix<'a when 'a: struct> =
    | ClMatrixCSR of ClCsrMatrix<'a>
    | ClMatrixCOO of ClCooMatrix<'a>

    member this.RowCount =
        match this with
        | ClMatrixCSR matrix -> matrix.RowCount
        | ClMatrixCOO matrix -> matrix.RowCount

    member this.ColumnCount =
        match this with
        | ClMatrixCSR matrix -> matrix.ColumnCount
        | ClMatrixCOO matrix -> matrix.ColumnCount

    member this.ToHost(q: MailboxProcessor<_>) =
        match this with
        | ClMatrixCSR matrix -> MatrixCSR <| matrix.ToHost(q)
        | ClMatrixCOO matrix -> MatrixCOO <| matrix.ToHost(q)

    member this.Dispose(q) =
        match this with
        | ClMatrixCSR matrix -> matrix.Dispose q
        | ClMatrixCOO matrix -> matrix.Dispose q
