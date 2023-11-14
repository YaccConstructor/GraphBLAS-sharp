namespace GraphBLAS.FSharp.Backend.Algorithms

open Brahma.FSharp
open FSharp.Quotations
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Objects.ClMatrix
open GraphBLAS.FSharp.Objects.ArraysExtensions
open GraphBLAS.FSharp.Objects.ClContextExtensions
open GraphBLAS.FSharp.Backend.Matrix.LIL
open GraphBLAS.FSharp.Backend.Matrix.COO

module internal MSBFS =
    module Levels =
        let private updateFront (clContext: ClContext) workGroupSize =

            let excludeValues = ClArray.excludeElements clContext workGroupSize

            let excludeIndices = ClArray.excludeElements clContext workGroupSize

            fun (queue: MailboxProcessor<_>) allocationMode (front: ClMatrix.COO<_>) (intersection: ClArray<int>) ->

                let newRows = excludeIndices queue allocationMode intersection front.Rows

                let newColumns = excludeIndices queue allocationMode intersection front.Columns

                let newValues = excludeValues queue allocationMode intersection front.Values

                match newRows, newColumns, newValues with
                | Some rows, Some columns, Some values ->
                    { Context = clContext
                      Rows = rows
                      Columns = columns
                      Values = values
                      RowCount = front.RowCount
                      ColumnCount = front.ColumnCount }
                    |> Some
                | _ -> None

        let private updateFrontAndLevels (clContext: ClContext) workGroupSize =

            let updateFront = updateFront clContext workGroupSize

            let mergeDisjoint = Matrix.mergeDisjoint clContext workGroupSize

            let findIntersection = Intersect.findKeysIntersection clContext workGroupSize

            fun (queue: MailboxProcessor<_>) allocationMode (levels: ClMatrix.COO<_>) (front: ClMatrix.COO<_>) ->

                // Find intersection of levels and front indices.
                let intersection = findIntersection queue DeviceOnly front levels

                // Remove mutual elements
                let newFront = updateFront queue allocationMode front intersection

                intersection.Free queue

                match newFront with
                | Some f ->
                    // Update levels
                    let newLevels = mergeDisjoint queue levels f
                    newLevels, newFront
                | _ -> levels, None

        let run<'a when 'a: struct>
            (add: Expr<int -> int -> int option>)
            (mul: Expr<'a -> int -> int option>)
            (clContext: ClContext)
            workGroupSize
            =

            let spGeMM =
                Operations.SpGeMM.expand add mul clContext workGroupSize

            let toCSRInPlace = Matrix.toCSRInPlace clContext workGroupSize

            let updateFrontAndLevels = updateFrontAndLevels clContext workGroupSize

            fun (queue: MailboxProcessor<Msg>) (matrix: ClMatrix<'a>) (source: int list) ->
                let vertexCount = matrix.RowCount
                let sourceVertexCount = source.Length

                let startMatrix =
                    source
                    |> List.mapi (fun i vertex -> i, vertex, 1)

                let mutable levels =
                    startMatrix
                    |> Matrix.ofList clContext DeviceOnly sourceVertexCount vertexCount

                let mutable front =
                    startMatrix
                    |> Matrix.ofList clContext DeviceOnly sourceVertexCount vertexCount
                    |> toCSRInPlace queue DeviceOnly

                let mutable level = 0
                let mutable stop = false

                while not stop do
                    level <- level + 1

                    //Getting new frontier
                    match spGeMM queue DeviceOnly matrix (ClMatrix.CSR front) with
                    | None ->
                        front.Dispose queue
                        stop <- true
                    | Some newFrontier ->
                        front.Dispose queue
                        //Filtering visited vertices
                        match updateFrontAndLevels queue DeviceOnly levels newFrontier with
                        | l, Some f ->
                            front <- toCSRInPlace queue DeviceOnly f
                            levels.Dispose queue
                            levels <- l
                            newFrontier.Dispose queue
                        | _, None ->
                            stop <- true
                            newFrontier.Dispose queue

                levels

        let runSingleSourceMultipleTimes<'a when 'a: struct>
            (add: Expr<int option -> int option -> int option>)
            (mul: Expr<'a option -> int option -> int option>)
            (clContext: ClContext)
            workGroupSize
            =

            let SSBFS = BFS.singleSourceSparse add mul clContext workGroupSize

            fun (queue: MailboxProcessor<Msg>) (matrix: ClMatrix<'a>) (source: int list) ->
                source
                |> List.map (SSBFS queue matrix)

    module Parents =
        let run = 0
