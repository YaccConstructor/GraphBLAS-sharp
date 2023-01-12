namespace GraphBLAS.FSharp.Objects

open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Objects

type MatrixFormat =
    | CSR
    | COO
    | CSC

[<RequireQualifiedAccess>]
type Matrix<'a when 'a: struct> =
    | CSR of CSRMatrix<'a>
    | COO of COOMatrix<'a>
    | CSC of CSCMatrix<'a>

    member this.RowCount =
        match this with
        | CSR matrix -> matrix.RowCount
        | COO matrix -> matrix.RowCount
        | CSC matrix -> matrix.RowCount

    member this.ColumnCount =
        match this with
        | CSR matrix -> matrix.ColumnCount
        | COO matrix -> matrix.ColumnCount
        | CSC matrix -> matrix.ColumnCount

    member this.NNZCount =
        match this with
        | COO m -> m.Values.Length
        | CSR m -> m.Values.Length
        | CSC m -> m.Values.Length

    member this.ToBackend(context: ClContext) =
        match this with
        | COO m ->
            let rows = context.CreateClArray m.Rows
            let columns = context.CreateClArray m.Columns
            let values = context.CreateClArray m.Values

            let result =
                { ClCOOMatrix.Context = context
                  RowCount = m.RowCount
                  ColumnCount = m.ColumnCount
                  Rows = rows
                  Columns = columns
                  Values = values }

            ClMatrix.COO result
        | CSR m ->
            let rows = context.CreateClArray m.RowPointers
            let columns = context.CreateClArray m.ColumnIndices
            let values = context.CreateClArray m.Values

            let result =
                { ClCSRMatrix.Context = context
                  RowCount = m.RowCount
                  ColumnCount = m.ColumnCount
                  RowPointers = rows
                  Columns = columns
                  Values = values }

            ClMatrix.CSR result
        | CSC m ->
            let rows = context.CreateClArray m.RowIndices
            let columnPtrs = context.CreateClArray m.ColumnPointers
            let values = context.CreateClArray m.Values

            let result =
                { ClCSCMatrix.Context = context
                  RowCount = m.RowCount
                  ColumnCount = m.ColumnCount
                  Rows = rows
                  ColumnPointers = columnPtrs
                  Values = values }

            ClMatrix.CSC result

    static member FromBackend (q: MailboxProcessor<_>) matrix =
        match matrix with
        | ClMatrix.COO m ->
            let rows = Array.zeroCreate m.Rows.Length
            let columns = Array.zeroCreate m.Columns.Length
            let values = Array.zeroCreate m.Values.Length

            let _ =
                q.Post(Msg.CreateToHostMsg(m.Rows, rows))

            let _ =
                q.Post(Msg.CreateToHostMsg(m.Columns, columns))

            let _ =
                q.PostAndReply(fun ch -> Msg.CreateToHostMsg(m.Values, values, ch))

            let result =
                { RowCount = m.RowCount
                  ColumnCount = m.ColumnCount
                  Rows = rows
                  Columns = columns
                  Values = values }

            COO result
        | ClMatrix.CSR m ->
            let rows = Array.zeroCreate m.RowPointers.Length
            let columns = Array.zeroCreate m.Columns.Length
            let values = Array.zeroCreate m.Values.Length

            let _ =
                q.Post(Msg.CreateToHostMsg(m.RowPointers, rows))

            let _ =
                q.Post(Msg.CreateToHostMsg(m.Columns, columns))

            let _ =
                q.PostAndReply(fun ch -> Msg.CreateToHostMsg(m.Values, values, ch))

            let result =
                { RowCount = m.RowCount
                  ColumnCount = m.ColumnCount
                  RowPointers = rows
                  ColumnIndices = columns
                  Values = values }

            CSR result
        | ClMatrix.CSC m ->
            let rows = Array.zeroCreate m.Rows.Length
            let columns = Array.zeroCreate m.ColumnPointers.Length
            let values = Array.zeroCreate m.Values.Length

            let _ =
                q.Post(Msg.CreateToHostMsg(m.Rows, rows))

            let _ =
                q.Post(Msg.CreateToHostMsg(m.ColumnPointers, columns))

            let _ =
                q.PostAndReply(fun ch -> Msg.CreateToHostMsg(m.Values, values, ch))

            let result =
                { RowCount = m.RowCount
                  ColumnCount = m.ColumnCount
                  RowIndices = rows
                  ColumnPointers = columns
                  Values = values }

            CSC result

and CSRMatrix<'a> =
    { RowCount: int
      ColumnCount: int
      RowPointers: int []
      ColumnIndices: int []
      Values: 'a [] }

    static member FromArray2D(array: 'a [,], isZero: 'a -> bool) =
        let rowsCount = array |> Array2D.length1
        let columnsCount = array |> Array2D.length2

        // let (rows, cols, vals) =
        //     array
        //     |> Seq.cast<'a>
        //     |> Seq.mapi
        //         (fun idx v ->
        //             let i = idx / Array2D.length2 array
        //             let j = idx % Array2D.length2 array

        //             (i, j, v)
        //         )
        //     |> Seq.filter (fun (_, _, v) -> (not << isZero) v)
        //     |> Array.ofSeq
        //     |> Array.unzip3

        // let rowPointers = Array.zeroCreate<int> (rowsCount + 1)
        // rows
        // |> Array.Parallel.iter
        //     (fun x ->
        //         System.Threading.Interlocked.Increment(&rowPointers.[x + 1]) |> ignore
        //     )

        // {
        //     RowCount = rowsCount
        //     ColumnCount = columnsCount
        //     RowPointers = rowPointers
        //     ColumnIndices = cols
        //     Values = vals
        // }

        let convertedMatrix =
            [ for i in 0 .. rowsCount - 1 -> array.[i, *] |> List.ofArray ]
            |> List.map
                (fun row ->
                    row
                    |> List.mapi (fun i x -> (x, i))
                    |> List.filter (fun pair -> not <| isZero (fst pair)))
            |> List.fold
                (fun (rowPtrs, valueInx) row -> ((rowPtrs.Head + row.Length) :: rowPtrs), valueInx @ row)
                ([ 0 ], [])

        { Values =
              convertedMatrix
              |> (snd >> List.unzip >> fst)
              |> List.toArray
          ColumnIndices =
              convertedMatrix
              |> (snd >> List.unzip >> snd)
              |> List.toArray
          RowPointers = convertedMatrix |> fst |> List.rev |> List.toArray
          RowCount = rowsCount
          ColumnCount = columnsCount }

    static member ToBackend (context: ClContext) matrix =
        let rowIndices2rowPointers (rowIndices: int []) rowCount =
            let nnzPerRow = Array.zeroCreate rowCount
            let rowPointers = Array.zeroCreate rowCount

            Array.iter (fun rowIndex -> nnzPerRow.[rowIndex] <- nnzPerRow.[rowIndex] + 1) rowIndices

            for i in 1 .. rowCount - 1 do
                rowPointers.[i] <- rowPointers.[i - 1] + nnzPerRow.[i - 1]

            rowPointers

        match matrix with
        | Matrix.COO m ->
            let rowPointers =
                context.CreateClArray(
                    rowIndices2rowPointers m.Rows m.RowCount,
                    hostAccessMode = HostAccessMode.ReadOnly,
                    deviceAccessMode = DeviceAccessMode.ReadOnly,
                    allocationMode = AllocationMode.CopyHostPtr
                )

            let cols =
                context.CreateClArray(
                    m.Columns,
                    hostAccessMode = HostAccessMode.ReadOnly,
                    deviceAccessMode = DeviceAccessMode.ReadOnly,
                    allocationMode = AllocationMode.CopyHostPtr
                )

            let vals =
                context.CreateClArray(
                    m.Values,
                    hostAccessMode = HostAccessMode.ReadOnly,
                    deviceAccessMode = DeviceAccessMode.ReadOnly,
                    allocationMode = AllocationMode.CopyHostPtr
                )

            { ClCSRMatrix.Context = context
              RowCount = m.RowCount
              ColumnCount = m.ColumnCount
              RowPointers = rowPointers
              Columns = cols
              Values = vals }

        | x -> failwith "Unsupported matrix format: %A"

and COOMatrix<'a> =
    { RowCount: int
      ColumnCount: int
      Rows: int []
      Columns: int []
      Values: 'a [] }

    override this.ToString() =
        [ sprintf "COO Matrix     %ix%i \n" this.RowCount this.ColumnCount
          sprintf "RowIndices:    %A \n" this.Rows
          sprintf "ColumnIndices: %A \n" this.Columns
          sprintf "Values:        %A \n" this.Values ]
        |> String.concat ""

    static member FromTuples(rowCount: int, columnCount: int, rows: int [], columns: int [], values: 'a []) =
        { RowCount = rowCount
          ColumnCount = columnCount
          Rows = rows
          Columns = columns
          Values = values }

    static member FromArray2D(array: 'a [,], isZero: 'a -> bool) =
        let (rows, cols, vals) =
            array
            |> Seq.cast<'a>
            |> Seq.mapi (fun idx v -> (idx / Array2D.length2 array, idx % Array2D.length2 array, v))
            |> Seq.filter (fun (_, _, v) -> not <| isZero v)
            |> Array.ofSeq
            |> Array.unzip3

        COOMatrix.FromTuples(Array2D.length1 array, Array2D.length2 array, rows, cols, vals)

    static member ToBackend (context: ClContext) matrix =
        match matrix with
        | Matrix.COO m ->
            let rows =
                context.CreateClArray(
                    m.Rows,
                    hostAccessMode = HostAccessMode.ReadOnly,
                    deviceAccessMode = DeviceAccessMode.ReadOnly,
                    allocationMode = AllocationMode.CopyHostPtr
                )

            let cols =
                context.CreateClArray(
                    m.Columns,
                    hostAccessMode = HostAccessMode.ReadOnly,
                    deviceAccessMode = DeviceAccessMode.ReadOnly,
                    allocationMode = AllocationMode.CopyHostPtr
                )

            let vals =
                context.CreateClArray(
                    m.Values,
                    hostAccessMode = HostAccessMode.ReadOnly,
                    deviceAccessMode = DeviceAccessMode.ReadOnly,
                    allocationMode = AllocationMode.CopyHostPtr
                )

            { ClCOOMatrix.Context = context
              RowCount = m.RowCount
              ColumnCount = m.ColumnCount
              Rows = rows
              Columns = cols
              Values = vals }

        | x -> failwith "Unsupported matrix format: %A"

and CSCMatrix<'a> =
    { RowCount: int
      ColumnCount: int
      RowIndices: int []
      ColumnPointers: int []
      Values: 'a [] }

    static member FromArray2D(array: 'a [,], isZero: 'a -> bool) =
        let rowsCount = array |> Array2D.length1
        let columnsCount = array |> Array2D.length2

        let convertedMatrix =
            [ for i in 0 .. columnsCount - 1 -> array.[*, i] |> List.ofArray ]
            |> List.map
                (fun col ->
                    col
                    |> List.mapi (fun i x -> (x, i))
                    |> List.filter (fun pair -> not <| isZero (fst pair)))
            |> List.fold
                (fun (colPtrs, valueInx) col -> ((colPtrs.Head + col.Length) :: colPtrs), valueInx @ col)
                ([ 0 ], [])

        { Values =
              convertedMatrix
              |> (snd >> List.unzip >> fst)
              |> List.toArray
          RowIndices =
              convertedMatrix
              |> (snd >> List.unzip >> snd)
              |> List.toArray
          ColumnPointers = convertedMatrix |> fst |> List.rev |> List.toArray
          RowCount = rowsCount
          ColumnCount = columnsCount }

type MatrixTuples<'a> =
    { RowIndices: int []
      ColumnIndices: int []
      Values: 'a [] }
