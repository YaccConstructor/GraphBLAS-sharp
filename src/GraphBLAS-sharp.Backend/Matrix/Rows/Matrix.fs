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

        let concatIndices = ClArray.concat clContext workGroupSize

        let concatValues = ClArray.concat clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (matrix: LIL<'a>) ->

            let rowsPointers =
                matrix.Rows
                |> Array.map
                    (function
                    | None -> 0
                    | Some vector -> vector.Values.Length)
                // prefix sum
                |> Array.scan (+) 0
                |> fun pointers -> clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, pointers)

            let valuesByRows, columnsIndicesByRows =
                matrix.Rows
                |> Array.choose id
                |> Array.map (fun vector -> vector.Values, vector.Indices)
                |> Array.unzip

            let values =
                concatValues processor allocationMode valuesByRows

            let columnsIndices =
                concatIndices processor allocationMode columnsIndicesByRows

            { Context = clContext
              RowCount = matrix.RowCount
              ColumnCount = matrix.ColumnCount
              RowPointers = rowsPointers
              Columns = columnsIndices
              Values = values }
