namespace GraphBLAS.FSharp.Backend.Algorithms

open Brahma.FSharp
open FSharp.Quotations
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Common
open GraphBLAS.FSharp.Objects.ClMatrix
open GraphBLAS.FSharp.Objects.ArraysExtensions
open GraphBLAS.FSharp.Objects.ClContextExtensions
open GraphBLAS.FSharp.Objects.ClCellExtensions
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Backend.Matrix.LIL
open GraphBLAS.FSharp.Backend.Matrix.COO

module internal MSBFS =
    let private frontExclude (clContext: ClContext) workGroupSize =

        let invert =
            ClArray.mapInPlace ArithmeticOperations.intNotQ clContext workGroupSize

        let prefixSum =
            PrefixSum.standardExcludeInPlace clContext workGroupSize

        let scatterIndices =
            Scatter.lastOccurrence clContext workGroupSize

        let scatterValues =
            Scatter.lastOccurrence clContext workGroupSize

        fun (queue: MailboxProcessor<_>) allocationMode (front: ClMatrix.COO<_>) (intersection: ClArray<int>) ->

            invert queue intersection

            let length =
                (prefixSum queue intersection)
                    .ToHostAndFree queue

            if length = 0 then
                None
            else
                let rows =
                    clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, length)

                let columns =
                    clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, length)

                let values =
                    clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, length)

                scatterIndices queue intersection front.Rows rows
                scatterIndices queue intersection front.Columns columns
                scatterValues queue intersection front.Values values

                { Context = clContext
                  Rows = rows
                  Columns = columns
                  Values = values
                  RowCount = front.RowCount
                  ColumnCount = front.ColumnCount }
                |> Some

    module Levels =
        let private updateFrontAndLevels (clContext: ClContext) workGroupSize =

            let updateFront = frontExclude clContext workGroupSize

            let mergeDisjoint =
                Matrix.mergeDisjoint clContext workGroupSize

            let setLevel = ClArray.fill clContext workGroupSize

            let findIntersection =
                Intersect.findKeysIntersection clContext workGroupSize

            fun (queue: MailboxProcessor<_>) allocationMode (level: int) (front: ClMatrix.COO<_>) (levels: ClMatrix.COO<_>) ->

                // Find intersection of levels and front indices.
                let intersection =
                    findIntersection queue DeviceOnly front levels

                // Remove mutual elements
                let newFront =
                    updateFront queue allocationMode front intersection

                intersection.Free queue

                match newFront with
                | Some f ->
                    let levelClCell = clContext.CreateClCell level

                    // Set current level value to all remaining front positions
                    setLevel queue levelClCell 0 f.Values.Length f.Values

                    levelClCell.Free queue

                    // Update levels
                    let newLevels = mergeDisjoint queue levels f

                    newLevels, newFront
                | _ -> levels, None

        let run<'a when 'a: struct>
            (add: Expr<int -> int -> int option>)
            (mul: Expr<int -> 'a -> int option>)
            (clContext: ClContext)
            workGroupSize
            =

            let spGeMM =
                Operations.SpGeMM.COO.expand add mul clContext workGroupSize

            let copy = Matrix.copy clContext workGroupSize

            let updateFrontAndLevels =
                updateFrontAndLevels clContext workGroupSize

            fun (queue: MailboxProcessor<Msg>) (matrix: ClMatrix<'a>) (source: int list) ->
                let vertexCount = matrix.RowCount
                let sourceVertexCount = source.Length

                let source = source |> List.sort

                let startMatrix =
                    source |> List.mapi (fun i vertex -> i, vertex, 1)

                let mutable levels =
                    startMatrix
                    |> Matrix.ofList clContext DeviceOnly sourceVertexCount vertexCount

                let mutable front = copy queue DeviceOnly levels

                let mutable level = 1
                let mutable stop = false

                while not stop do
                    level <- level + 1

                    //Getting new frontier
                    match spGeMM queue DeviceOnly (ClMatrix.COO front) matrix with
                    | None ->
                        front.Dispose queue
                        stop <- true

                    | Some newFrontier ->
                        front.Dispose queue

                        //Filtering visited vertices
                        match updateFrontAndLevels queue DeviceOnly level newFrontier levels with
                        | l, Some f ->
                            front <- f

                            levels.Dispose queue

                            levels <- l

                            newFrontier.Dispose queue

                        | _, None ->
                            stop <- true
                            newFrontier.Dispose queue

                ClMatrix.COO levels

        let runSingleSourceMultipleTimes<'a when 'a: struct>
            (add: Expr<int option -> int option -> int option>)
            (mul: Expr<'a option -> int option -> int option>)
            (clContext: ClContext)
            workGroupSize
            =

            let SSBFS =
                BFS.singleSourceSparse add mul clContext workGroupSize

            fun (queue: MailboxProcessor<Msg>) (matrix: ClMatrix<'a>) (source: int list) ->
                source |> List.map (SSBFS queue matrix)

    module Parents =
        let private updateFrontAndParents (clContext: ClContext) workGroupSize =
            let frontExclude = frontExclude clContext workGroupSize

            let mergeDisjoint =
                Matrix.mergeDisjoint clContext workGroupSize

            let findIntersection =
                Intersect.findKeysIntersection clContext workGroupSize

            let copyIndices = ClArray.copyTo clContext workGroupSize

            fun (queue: MailboxProcessor<Msg>) allocationMode (front: ClMatrix.COO<_>) (parents: ClMatrix.COO<_>) ->

                // Find intersection of levels and front indices.
                let intersection =
                    findIntersection queue DeviceOnly front parents

                // Remove mutual elements
                let newFront =
                    frontExclude queue allocationMode front intersection

                intersection.Free queue

                match newFront with
                | Some f ->
                    // Update parents
                    let newParents = mergeDisjoint queue parents f

                    copyIndices queue f.Columns f.Values

                    newParents, Some f

                | _ -> parents, None

        let run<'a when 'a: struct> (clContext: ClContext) workGroupSize =

            let spGeMM =
                Operations.SpGeMM.COO.expand
                    (ArithmeticOperations.min)
                    (ArithmeticOperations.fst)
                    clContext
                    workGroupSize

            let updateFrontAndParents =
                updateFrontAndParents clContext workGroupSize

            fun (queue: MailboxProcessor<Msg>) (inputMatrix: ClMatrix<'a>) (source: int list) ->
                let vertexCount = inputMatrix.RowCount
                let sourceVertexCount = source.Length

                let source = source |> List.sort

                let matrix =
                    match inputMatrix with
                    | ClMatrix.CSR m ->
                        { Context = clContext
                          RowPointers = m.RowPointers
                          Columns = m.Columns
                          Values = m.Columns
                          RowCount = m.RowCount
                          ColumnCount = m.ColumnCount }
                        |> ClMatrix.CSR
                    | _ -> failwith "Incorrect format"

                let mutable parents =
                    source
                    |> List.mapi (fun i vertex -> i, vertex, -1)
                    |> Matrix.ofList clContext DeviceOnly sourceVertexCount vertexCount

                let mutable front =
                    source
                    |> List.mapi (fun i vertex -> i, vertex, vertex)
                    |> Matrix.ofList clContext DeviceOnly sourceVertexCount vertexCount

                let mutable stop = false

                while not stop do
                    //Getting new frontier
                    match spGeMM queue DeviceOnly (ClMatrix.COO front) matrix with
                    | None ->
                        front.Dispose queue
                        stop <- true

                    | Some newFrontier ->
                        front.Dispose queue

                        //Filtering visited vertices
                        match updateFrontAndParents queue DeviceOnly newFrontier parents with
                        | p, Some f ->
                            front <- f

                            parents.Dispose queue
                            parents <- p

                            newFrontier.Dispose queue

                        | _, None ->
                            stop <- true
                            newFrontier.Dispose queue

                ClMatrix.COO parents
