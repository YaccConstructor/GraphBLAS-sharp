namespace GraphBLAS.FSharp

open Brahma.FSharp
open GraphBLAS.FSharp.Backend

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

    member this.NNZCount =
        match this with
        | MatrixCOO m -> m.Values.Length
        | MatrixCSR m -> m.Values.Length

    member this.ToBackend(context: ClContext) =
        match this with
        | MatrixCOO m ->
            let rows = context.CreateClArray m.Rows
            let columns = context.CreateClArray m.Columns
            let values = context.CreateClArray m.Values

            let result =
                { Backend.COOMatrix.Context = context
                  RowCount = m.RowCount
                  ColumnCount = m.ColumnCount
                  Rows = rows
                  Columns = columns
                  Values = values }

            Backend.MatrixCOO result
        | MatrixCSR m ->
            let rows = context.CreateClArray m.RowPointers
            let columns = context.CreateClArray m.ColumnIndices
            let values = context.CreateClArray m.Values

            let result =
                { Backend.CSRMatrix.Context = context
                  RowCount = m.RowCount
                  ColumnCount = m.ColumnCount
                  RowPointers = rows
                  Columns = columns
                  Values = values }

            Backend.MatrixCSR result

    static member FromBackend (q: MailboxProcessor<_>) matrix =
        match matrix with
        | Backend.MatrixCOO m ->
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

            MatrixCOO result
        | Backend.MatrixCSR m ->
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

            MatrixCSR result

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

type MatrixTuples<'a> =
    { RowIndices: int []
      ColumnIndices: int []
      Values: 'a [] }
