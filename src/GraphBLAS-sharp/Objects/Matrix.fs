namespace GraphBLAS.FSharp.Objects

open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Objects.ClMatrix

module Matrix =
    type CSR<'a when 'a: struct> =
        { RowCount: int
          ColumnCount: int
          RowPointers: int []
          ColumnIndices: int []
          Values: 'a [] }

        static member FromArray2D(array: 'a [,], isZero: 'a -> bool) =
            let rowsCount = array |> Array2D.length1
            let columnsCount = array |> Array2D.length2

            let convertedMatrix =
                [ for i in 0 .. rowsCount - 1 -> array.[i, *] |> List.ofArray ]
                |> List.map
                    (fun row ->
                        row
                        |> List.mapi (fun i x -> (x, i))
                        |> List.filter (fun pair -> not <| isZero (fst pair)))
                |> List.fold
                    (fun (rowPointers, valueInx) row ->
                        ((rowPointers.Head + row.Length) :: rowPointers), valueInx @ row)
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

        member this.NNZ = this.Values.Length

        member this.ToDevice(context: ClContext) =
            { Context = context
              RowCount = this.RowCount
              ColumnCount = this.ColumnCount
              RowPointers = context.CreateClArray this.RowPointers
              Columns = context.CreateClArray this.ColumnIndices
              Values = context.CreateClArray this.Values }

    type COO<'a when 'a: struct> =
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

        member this.NNZ = this.Values.Length

        static member FromTuples(rowCount: int, columnCount: int, rows: int [], columns: int [], values: 'a []) =
            { RowCount = rowCount
              ColumnCount = columnCount
              Rows = rows
              Columns = columns
              Values = values }

        static member FromArray2D(array: 'a [,], isZero: 'a -> bool) =
            let rows, cols, values =
                array
                |> Seq.cast<'a>
                |> Seq.mapi (fun idx v -> (idx / Array2D.length2 array, idx % Array2D.length2 array, v))
                |> Seq.filter (fun (_, _, v) -> not <| isZero v)
                |> Array.ofSeq
                |> Array.unzip3

            COO.FromTuples(Array2D.length1 array, Array2D.length2 array, rows, cols, values)

        member this.ToDevice(context: ClContext) =
            { Context = context
              RowCount = this.RowCount
              ColumnCount = this.ColumnCount
              Rows = context.CreateClArray this.Rows
              Columns = context.CreateClArray this.Columns
              Values = context.CreateClArray this.Values }

        member this.ToCSR =
            let rowPointers =
                let pointers = Array.zeroCreate this.RowCount

                Array.countBy id this.Rows
                |> Array.iter (fun (index, count) -> pointers.[index] <- count)

                Array.scan (+) 0 pointers

            { RowCount = this.RowCount
              ColumnCount = this.ColumnCount
              RowPointers = rowPointers
              ColumnIndices = this.Columns
              Values = this.Values }

    type CSC<'a when 'a: struct> =
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
                    (fun (colPointers, valueInx) col ->
                        ((colPointers.Head + col.Length) :: colPointers), valueInx @ col)
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

        member this.NNZ = this.Values.Length

        member this.ToDevice(context: ClContext) =
            { Context = context
              RowCount = this.RowCount
              ColumnCount = this.ColumnCount
              Rows = context.CreateClArray this.RowIndices
              ColumnPointers = context.CreateClArray this.ColumnPointers
              Values = context.CreateClArray this.Values }

    type LIL<'a when 'a: struct> =
        { RowCount: int
          ColumnCount: int
          Rows: Vector.Sparse<'a> option []
          NNZ: int }

        static member FromArray2D(array: 'a [,], isZero: 'a -> bool) =
            let mutable nnz = 0

            let rows =
                [ for i in 0 .. Array2D.length1 array - 1 do
                      let vector =
                          Vector.Sparse.FromArray(array.[i, *], isZero)

                      nnz <- nnz + vector.NNZ

                      if vector.NNZ > 0 then
                          Some vector
                      else
                          None ]
                |> Array.ofList

            { RowCount = Array2D.length1 array
              ColumnCount = Array2D.length2 array
              Rows = rows
              NNZ = nnz }

        member this.ToDevice(context: ClContext) =

            let rows =
                this.Rows
                |> Array.map (Option.bind (fun vector -> Some <| vector.ToDevice(context)))

            { Context = context
              RowCount = this.RowCount
              ColumnCount = this.ColumnCount
              Rows = rows
              NNZ = this.NNZ }

    type Tuples<'a> =
        { RowIndices: int []
          ColumnIndices: int []
          Values: 'a [] }

[<RequireQualifiedAccess>]
type Matrix<'a when 'a: struct> =
    | CSR of Matrix.CSR<'a>
    | COO of Matrix.COO<'a>
    | CSC of Matrix.CSC<'a>
    | LIL of Matrix.LIL<'a>

    member this.RowCount =
        match this with
        | CSR matrix -> matrix.RowCount
        | COO matrix -> matrix.RowCount
        | CSC matrix -> matrix.RowCount
        | LIL matrix -> matrix.RowCount

    member this.ColumnCount =
        match this with
        | CSR matrix -> matrix.ColumnCount
        | COO matrix -> matrix.ColumnCount
        | CSC matrix -> matrix.ColumnCount
        | LIL matrix -> matrix.ColumnCount

    member this.NNZ =
        match this with
        | COO m -> m.NNZ
        | CSR m -> m.NNZ
        | CSC m -> m.NNZ
        | LIL m -> m.NNZ

    member this.ToDevice(context: ClContext) =
        match this with
        | COO matrix -> ClMatrix.COO <| matrix.ToDevice context
        | CSR matrix -> ClMatrix.CSR <| matrix.ToDevice context
        | CSC matrix -> ClMatrix.CSC <| matrix.ToDevice context
        | LIL matrix -> ClMatrix.LIL <| matrix.ToDevice context
