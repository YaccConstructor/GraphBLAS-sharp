namespace GraphBLAS.FSharp.Backend.Matrix.Kronecker

open FSharp.Quotations
open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Matrix
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Objects.ClCell
open GraphBLAS.FSharp.Backend.Objects.ClVector
open GraphBLAS.FSharp.Backend.Objects.ClMatrix
open GraphBLAS.FSharp.Backend.Vector.Sparse.Vector

module ByRows =
    let concatOptionalVectors (clContext: ClContext) workGroupSize =

        let concatValues = ClArray.concat clContext workGroupSize

        let concatIndices = ClArray.concat clContext workGroupSize

        let mapIndices =
            ClArray.mapWithValue clContext workGroupSize <@ fun x y -> x + y @>

        fun (processor: MailboxProcessor<_>) allocationMode (vectors: Sparse<'a> option seq) ->
            let vectorIndices =
                vectors
                |> Seq.mapi
                    (fun offset ->
                        function
                        | Some v ->
                            let offsetClCell =
                                offset * v.Size |> clContext.CreateClCell

                            let newIndices =
                                mapIndices processor allocationMode offsetClCell v.Indices

                            offsetClCell.Free processor
                            newIndices |> Some
                        | _ -> None)
                |> Seq.choose id

            if Seq.isEmpty vectorIndices then
                None
            else
                let vectorValues =
                    vectors
                    |> Seq.choose id
                    |> Seq.map (fun vector -> vector.Values)

                let resultIndices =
                    concatIndices processor allocationMode vectorIndices

                let resultValues =
                    concatValues processor allocationMode vectorValues

                (resultIndices, resultValues) |> Some

    let tryFindIndex index (indices: int array) =
        let rec binSearch left right =
            if left <= right then
                let mid = (left + right) / 2
                let element = indices.[mid]

                if element = index then
                    Some mid
                else if element < index then
                    binSearch (mid + 1) right
                else
                    binSearch left (mid - 1)
            else
                None

        binSearch 0 (indices.Length - 1)

    let makeRow (clContext: ClContext) workGroupSize (op: Expr<'a option -> 'b option -> 'c option>) =

        let map = mapWithValue clContext workGroupSize op

        let concat =
            concatOptionalVectors clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode leftLength rightLength (leftIndicesOnHost: int array) (leftValuesOnHost: 'a array) (rightRow: Sparse<'b> option) ->

            let zeroVector =
                lazy (map processor allocationMode None rightLength rightRow)

            { 0 .. leftLength - 1 }
            |> Seq.map
                (fun i ->
                    leftIndicesOnHost
                    |> tryFindIndex i
                    |> function
                        | Some index ->
                            let value = leftValuesOnHost.[index] |> Some
                            map processor allocationMode value rightLength rightRow
                        | None -> zeroVector.Value)
            |> concat processor allocationMode
            |> Option.bind
                (fun (indices, values) ->
                    { Context = clContext
                      Indices = indices
                      Values = values
                      Size = rightLength * leftLength }
                    |> Some)

    let run<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct>
        (clContext: ClContext)
        workGroupSize
        (op: Expr<'a option -> 'b option -> 'c option>)
        =

        let splitLeft =
            CSR.Matrix.byRowsLazy clContext workGroupSize

        let splitRight =
            CSR.Matrix.byRowsLazy clContext workGroupSize

        let makeRow = makeRow clContext workGroupSize op

        fun (processor: MailboxProcessor<_>) allocationMode (leftMatrix: ClMatrix.CSR<'a>) (rightMatrix: ClMatrix.CSR<'b>) ->
            let splitLeftMatrix =
                splitLeft processor allocationMode leftMatrix

            let splitRightMatrix =
                splitRight processor allocationMode rightMatrix

            let resultRows =
                splitLeftMatrix
                |> Seq.fold
                    (fun rows leftRow ->
                        let leftIndices, leftValues =
                            match leftRow.Value with
                            | Some row -> (row.Indices.ToHostAndFree processor, row.Values.ToHostAndFree processor)
                            | None -> Array.empty, Array.empty

                        splitRightMatrix
                        |> Seq.fold
                            (fun rows rightRow ->
                                [ makeRow
                                      processor
                                      allocationMode
                                      leftMatrix.ColumnCount
                                      rightMatrix.ColumnCount
                                      leftIndices
                                      leftValues
                                      rightRow.Value ]
                                |> List.append rows)
                            rows)
                    List.empty

            let nnz =
                resultRows
                |> List.fold
                    (fun count ->
                        function
                        | Some row -> count + row.Size
                        | None -> count)
                    0

            { Context = clContext
              RowCount = leftMatrix.RowCount * rightMatrix.RowCount
              ColumnCount = leftMatrix.ColumnCount * rightMatrix.ColumnCount
              Rows = resultRows
              NNZ = nnz }
