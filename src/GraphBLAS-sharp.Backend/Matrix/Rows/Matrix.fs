namespace GraphBLAS.FSharp.Backend.Matrix.Rows

open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Matrix
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Objects.ClContext
open GraphBLAS.FSharp.Backend.Objects.ClMatrix
open GraphBLAS.FSharp.Backend.Quotes
open FSharp.Quotations.Evaluator

module Matrix =
    let toCSR (clContext: ClContext) workGroupSize =

        let concatVectors =
            Vector.Sparse.SparseVector.concat clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (matrix: Rows<'a>) ->

            // create row pointers
            let rowPointers =
                matrix.Rows
                |> Array.Parallel.map
                    (function
                    | None -> 0
                    | Some array -> array.Size)
                |> Array.scan (+) 0 // mb device prefix sum ???

            let rowPointers =
                clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, rowPointers)

            // compact columns and values
            matrix.Rows
            |> Array.Parallel.choose id
            |> concatVectors processor allocationMode
            |> fun vector ->
                { Context = clContext
                  RowCount = matrix.RowCount
                  ColumnCount = matrix.ColumnCount
                  RowPointers = rowPointers
                  Columns = vector.Indices
                  Values = vector.Values }

    let toCOO (clContext: ClContext) workGroupSize =

        let create = ClArray.create clContext workGroupSize

        let concatMatrix =
            COO.Matrix.concat clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (matrix: Rows<'a>) ->

            let createMatrix (vector: ClVector.Sparse<_>) rows =
                { Context = clContext
                  RowCount = matrix.RowCount
                  ColumnCount = matrix.ColumnCount
                  Rows = rows
                  Columns = vector.Indices
                  Values = vector.Values }

            let indices, rowsVectors =
                matrix.Rows
                |> Array.Parallel.mapi
                    (fun index optionRow ->
                        (match optionRow with
                         | None -> None
                         | Some row -> Some(index, row)))
                |> Array.Parallel.choose id
                |> Array.unzip

            // creat rows pointers
            let rowsIndices =
                (rowsVectors, indices)
                ||> Array.map2 (fun array -> create processor allocationMode array.Values.Length)

            Array.map2 createMatrix rowsVectors rowsIndices
            |> concatMatrix processor allocationMode matrix.ColumnCount matrix.RowCount
