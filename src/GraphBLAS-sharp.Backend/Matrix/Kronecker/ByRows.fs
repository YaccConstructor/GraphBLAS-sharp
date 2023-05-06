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
open GraphBLAS.FSharp.Backend.Vector
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

    let makeRow (clContext: ClContext) workGroupSize (op: Expr<'a option -> 'b option -> 'c option>) =

        let map = mapWithValue clContext workGroupSize op

        let concat =
            concatOptionalVectors clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode rightLength (zero: Sparse<'c> option) (leftRow: 'a option array) (rightRow: Sparse<'b> option) ->

            leftRow
            |> Array.map
                (function
                | Some value -> map processor allocationMode (Some value) rightLength rightRow
                | _ -> zero)
            |> concat processor allocationMode
            |> Option.map
                (fun (indices, values) ->
                    { Context = clContext
                      Indices = indices
                      Values = values
                      Size = rightLength * leftRow.Length })

    let run<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct and 'c: equality>
        (clContext: ClContext)
        workGroupSize
        (op: Expr<'a option -> 'b option -> 'c option>)
        =

        let map =
            CSR.Matrix.mapWithValueOption clContext op workGroupSize

        let toDense =
            Sparse.Vector.toDense clContext workGroupSize

        let toCSR =
            COO.Matrix.toCSRInPlace clContext workGroupSize

        let splitLeft =
            CSR.Matrix.byRowsLazy clContext workGroupSize

        let splitRight =
            CSR.Matrix.byRowsLazy clContext workGroupSize

        let splitResult =
            CSR.Matrix.byRows clContext workGroupSize

        let makeRow = makeRow clContext workGroupSize op

        fun (processor: MailboxProcessor<_>) allocationMode (leftMatrix: ClMatrix.CSR<'a>) (rightMatrix: ClMatrix.CSR<'b>) ->
            let splitLeftMatrix =
                splitLeft processor allocationMode leftMatrix

            let splitRightMatrix =
                splitRight processor allocationMode rightMatrix

            let zeroMatrix =
                rightMatrix
                |> map processor allocationMode None
                |> function
                    | Some m ->
                        m
                        |> toCSR processor allocationMode
                        |> splitResult processor allocationMode
                    | None -> Seq.replicate rightMatrix.RowCount None

            let resultRows =
                splitLeftMatrix
                |> Seq.fold
                    (fun rows leftRow ->
                        let denseLeftRow =
                            match leftRow.Value with
                            | Some row ->
                                let denseRow = toDense processor allocationMode row
                                row.Dispose processor
                                denseRow.ToHostAndFree processor
                            | None -> Array.create leftMatrix.ColumnCount None

                        splitRightMatrix
                        |> Seq.fold2
                            (fun rows zero rightRow ->
                                [ makeRow
                                      processor
                                      allocationMode
                                      rightMatrix.ColumnCount
                                      zero
                                      denseLeftRow
                                      rightRow.Value ]
                                |> List.append rows)
                            rows
                            zeroMatrix)
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
