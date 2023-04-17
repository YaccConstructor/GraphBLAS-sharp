namespace GraphBLAS.FSharp.Backend.Matrix.CSR

open Microsoft.FSharp.Quotations
open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Matrix.COO
open GraphBLAS.FSharp.Backend.Matrix.CSR
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Objects.ClMatrix
open GraphBLAS.FSharp.Backend.Objects.ClContext
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions

module internal Kronecker =
    /// Merges 2 disjoint matrices.
    let private mergeDisjoint<'a when 'a: struct> (clContext: ClContext) workGroupSize =

        let merge =
            <@ fun (ndRange: Range1D) firstSide secondSide sumOfSides (firstRowsBuffer: ClArray<int>) (firstColumnsBuffer: ClArray<int>) (firstValuesBuffer: ClArray<'a>) (secondRowsBuffer: ClArray<int>) (secondColumnsBuffer: ClArray<int>) (secondValuesBuffer: ClArray<'a>) (allRowsBuffer: ClArray<int>) (allColumnsBuffer: ClArray<int>) (mergedValuesBuffer: ClArray<'a>) ->

                let i = ndRange.GlobalID0

                let mutable beginIdxLocal = local ()
                let mutable endIdxLocal = local ()
                let localID = ndRange.LocalID0

                if localID < 2 then
                    let x = localID * (workGroupSize - 1) + i - 1

                    let diagonalNumber = min (sumOfSides - 1) x

                    let mutable leftEdge = diagonalNumber + 1 - secondSide
                    leftEdge <- max 0 leftEdge

                    let mutable rightEdge = firstSide - 1

                    rightEdge <- min diagonalNumber rightEdge

                    while leftEdge <= rightEdge do
                        let middleIdx = (leftEdge + rightEdge) / 2

                        let firstIndex: uint64 =
                            ((uint64 firstRowsBuffer.[middleIdx]) <<< 32)
                            ||| (uint64 firstColumnsBuffer.[middleIdx])

                        let secondIndex: uint64 =
                            ((uint64 secondRowsBuffer.[diagonalNumber - middleIdx])
                             <<< 32)
                            ||| (uint64 secondColumnsBuffer.[diagonalNumber - middleIdx])

                        if firstIndex < secondIndex then
                            leftEdge <- middleIdx + 1
                        else
                            rightEdge <- middleIdx - 1

                    // Here localID equals either 0 or 1
                    if localID = 0 then
                        beginIdxLocal <- leftEdge
                    else
                        endIdxLocal <- leftEdge

                barrierLocal ()

                let beginIdx = beginIdxLocal
                let endIdx = endIdxLocal
                let firstLocalLength = endIdx - beginIdx
                let mutable x = workGroupSize - firstLocalLength

                if endIdx = firstSide then
                    x <- secondSide - i + localID + beginIdx

                let secondLocalLength = x

                //First indices are from 0 to firstLocalLength - 1 inclusive
                //Second indices are from firstLocalLength to firstLocalLength + secondLocalLength - 1 inclusive
                let localIndices = localArray<uint64> workGroupSize

                if localID < firstLocalLength then
                    localIndices.[localID] <-
                        ((uint64 firstRowsBuffer.[beginIdx + localID])
                         <<< 32)
                        ||| (uint64 firstColumnsBuffer.[beginIdx + localID])

                if localID < secondLocalLength then
                    localIndices.[firstLocalLength + localID] <-
                        ((uint64 secondRowsBuffer.[i - beginIdx]) <<< 32)
                        ||| (uint64 secondColumnsBuffer.[i - beginIdx])

                barrierLocal ()

                if i < sumOfSides then
                    let mutable leftEdge = localID + 1 - secondLocalLength
                    leftEdge <- max 0 leftEdge

                    let mutable rightEdge = firstLocalLength - 1

                    rightEdge <- min localID rightEdge

                    while leftEdge <= rightEdge do
                        let middleIdx = (leftEdge + rightEdge) / 2
                        let firstIndex = localIndices.[middleIdx]

                        let secondIndex =
                            localIndices.[firstLocalLength + localID - middleIdx]

                        if firstIndex < secondIndex then
                            leftEdge <- middleIdx + 1
                        else
                            rightEdge <- middleIdx - 1

                    let boundaryX = rightEdge
                    let boundaryY = localID - leftEdge

                    // boundaryX and boundaryY can't be off the right edge of array (only off the left edge)
                    let isValidX = boundaryX >= 0
                    let isValidY = boundaryY >= 0

                    let mutable fstIdx = 0UL

                    if isValidX then
                        fstIdx <- localIndices.[boundaryX]

                    let mutable sndIdx = 0UL

                    if isValidY then
                        sndIdx <- localIndices.[firstLocalLength + boundaryY]

                    if not isValidX || isValidY && fstIdx < sndIdx then
                        allRowsBuffer.[i] <- int (sndIdx >>> 32)
                        allColumnsBuffer.[i] <- int sndIdx
                        mergedValuesBuffer.[i] <- secondValuesBuffer.[i - localID - beginIdx + boundaryY]
                    else
                        allRowsBuffer.[i] <- int (fstIdx >>> 32)
                        allColumnsBuffer.[i] <- int fstIdx
                        mergedValuesBuffer.[i] <- firstValuesBuffer.[beginIdx + boundaryX] @>

        let kernel = clContext.Compile(merge)

        fun (processor: MailboxProcessor<_>) (matrixLeftRows: ClArray<int>) (matrixLeftColumns: ClArray<int>) (matrixLeftValues: ClArray<'a>) (matrixRightRows: ClArray<int>) (matrixRightColumns: ClArray<int>) (matrixRightValues: ClArray<'a>) ->

            let firstSide = matrixLeftValues.Length
            let secondSide = matrixRightValues.Length
            let sumOfSides = firstSide + secondSide

            let allRows =
                clContext.CreateClArrayWithSpecificAllocationMode<int>(DeviceOnly, sumOfSides)

            let allColumns =
                clContext.CreateClArrayWithSpecificAllocationMode<int>(DeviceOnly, sumOfSides)

            let mergedValues =
                clContext.CreateClArrayWithSpecificAllocationMode<'a>(DeviceOnly, sumOfSides)

            let ndRange =
                Range1D.CreateValid(sumOfSides, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.KernelFunc
                            ndRange
                            firstSide
                            secondSide
                            sumOfSides
                            matrixLeftRows
                            matrixLeftColumns
                            matrixLeftValues
                            matrixRightRows
                            matrixRightColumns
                            matrixRightValues
                            allRows
                            allColumns
                            mergedValues)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            allRows, allColumns, mergedValues

    /// <summary>
    /// inserts one matrix to another.
    /// </summary>
    /// <remarks>
    /// Inserted matrix with shifted indices should not intersect the main matrix.
    /// </remarks>
    let private insertMatrixWithOffset (clContext: ClContext) workGroupSize =

        let mapWithValueClArray =
            ClArray.mapWithValue clContext workGroupSize <@ fun x y -> x + y @>

        let mergeDisjointCOO = mergeDisjoint clContext workGroupSize

        let copyClArray = ClArray.copy clContext workGroupSize

        fun (queue: MailboxProcessor<_>) allocationMode (rowOffset: int) (columnOffset: int) (matrixToInsert: ClMatrix.COO<'c> option) (resultMatrix: ClMatrix.COO<'c> option) ->

            match matrixToInsert with
            | None -> resultMatrix

            | Some m ->
                let rowOffset = rowOffset |> clContext.CreateClCell
                let columnOffset = columnOffset |> clContext.CreateClCell

                let newRowIndices =
                    mapWithValueClArray queue allocationMode rowOffset m.Rows

                let newColumnIndices =
                    mapWithValueClArray queue allocationMode columnOffset m.Columns

                queue.Post(Msg.CreateFreeMsg<_>(rowOffset))
                queue.Post(Msg.CreateFreeMsg<_>(columnOffset))

                match resultMatrix with
                | None ->
                    Some
                        { m with
                              Rows = newRowIndices
                              Columns = newColumnIndices
                              Values = copyClArray queue allocationMode m.Values }

                | Some mainMatrix ->

                    let newRows, newCols, newValues =
                        mergeDisjointCOO
                            queue
                            mainMatrix.Rows
                            mainMatrix.Columns
                            mainMatrix.Values
                            newRowIndices
                            newColumnIndices
                            m.Values

                    Some
                        { mainMatrix with
                              Rows = newRows
                              Columns = newCols
                              Values = newValues }

    let runToCOO<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct and 'c: equality>
        (clContext: ClContext)
        (op: Expr<'a option -> 'b option -> 'c option>)
        workGroupSize
        =

        let mapWithValueToCOO =
            Map.WithValue.runToCOO clContext op workGroupSize

        let insert =
            insertMatrixWithOffset clContext workGroupSize

        fun (queue: MailboxProcessor<_>) allocationMode (matrixLeft: ClMatrix.CSR<'a>) (matrixRight: ClMatrix.CSR<'b>) ->
            let noneClCell = None |> clContext.CreateClCell

            let mapWithZero =
                mapWithValueToCOO queue allocationMode noneClCell matrixRight

            queue.Post(Msg.CreateFreeMsg<_>(noneClCell))

            let mutable resultMatrix = None

            let leftMatrixRows = matrixLeft.RowPointers.ToHost queue
            let leftMatrixCols = matrixLeft.Columns.ToHost queue
            let leftMatrixVals = matrixLeft.Values.ToHost queue

            for row in 0 .. (matrixLeft.RowCount - 1) do
                let NNZInRow =
                    leftMatrixRows.[row + 1] - leftMatrixRows.[row]

                let firstElementIndex = leftMatrixRows.[row]

                let rowOffset = row * matrixRight.RowCount

                for offset in 0 .. (NNZInRow - 1) do
                    let currentColumn =
                        leftMatrixCols.[firstElementIndex + offset]

                    let numberOfZeroElements =
                        match offset with
                        | 0 -> currentColumn
                        | _ ->
                            currentColumn
                            - leftMatrixCols.[firstElementIndex + offset - 1]
                            - 1

                    // Inserting matrices resulting from multiplication by 0
                    if currentColumn <> 0 && numberOfZeroElements >= 0 then
                        for i in (currentColumn - numberOfZeroElements) .. currentColumn - 1 do
                            let columnOffset = i * matrixRight.ColumnCount

                            let newMatrix =
                                insert queue allocationMode rowOffset columnOffset mapWithZero resultMatrix

                            resultMatrix <-
                                match resultMatrix, mapWithZero with
                                | Some oldMatrix, Some _ ->
                                    queue.Post(Msg.CreateFreeMsg<_>(oldMatrix.Rows))
                                    queue.Post(Msg.CreateFreeMsg<_>(oldMatrix.Columns))
                                    queue.Post(Msg.CreateFreeMsg<_>(oldMatrix.Values))
                                    newMatrix
                                | _ -> newMatrix

                    // inserting new matrix resulting from multiplication by non None element of left matrix
                    let columnOffset = currentColumn * matrixRight.ColumnCount

                    let operand =
                        Some leftMatrixVals.[firstElementIndex + offset]
                        |> clContext.CreateClCell

                    let mappedMatrix =
                        mapWithValueToCOO queue allocationMode operand matrixRight

                    queue.Post(Msg.CreateFreeMsg<_>(operand))

                    let newMatrix =
                        insert queue allocationMode rowOffset columnOffset mappedMatrix resultMatrix

                    resultMatrix <-
                        match resultMatrix, mappedMatrix with
                        | Some oldMatrix, Some m ->
                            queue.Post(Msg.CreateFreeMsg<_>(oldMatrix.Rows))
                            queue.Post(Msg.CreateFreeMsg<_>(oldMatrix.Columns))
                            queue.Post(Msg.CreateFreeMsg<_>(oldMatrix.Values))
                            queue.Post(Msg.CreateFreeMsg<_>(m.Rows))
                            queue.Post(Msg.CreateFreeMsg<_>(m.Columns))
                            queue.Post(Msg.CreateFreeMsg<_>(m.Values))
                            newMatrix
                        | _ -> newMatrix

                // inserting matrices resulting from multiplication by 0 to the rest of the place in row
                let startColumn =
                    match NNZInRow with
                    | 0 -> 0
                    | _ ->
                        leftMatrixCols.[firstElementIndex + NNZInRow - 1]
                        + 1

                for i in startColumn .. matrixLeft.ColumnCount - 1 do
                    let columnOffset = i * matrixRight.ColumnCount

                    let newMatrix =
                        insert queue allocationMode rowOffset columnOffset mapWithZero resultMatrix

                    resultMatrix <-
                        match resultMatrix, mapWithZero with
                        | Some oldMatrix, Some _ ->
                            queue.Post(Msg.CreateFreeMsg<_>(oldMatrix.Rows))
                            queue.Post(Msg.CreateFreeMsg<_>(oldMatrix.Columns))
                            queue.Post(Msg.CreateFreeMsg<_>(oldMatrix.Values))
                            newMatrix
                        | _ -> newMatrix

            match resultMatrix with
            | None -> None
            | Some m ->
                Some
                    { m with
                          RowCount = matrixLeft.RowCount * matrixRight.RowCount
                          ColumnCount = matrixLeft.ColumnCount * matrixRight.ColumnCount }

    let run<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct and 'c: equality>
        (clContext: ClContext)
        (op: Expr<'a option -> 'b option -> 'c option>)
        workGroupSize
        =

        let kroneckerToCOO = runToCOO clContext op workGroupSize

        let toCSRInplace =
            Matrix.toCSRInplace clContext workGroupSize

        fun (queue: MailboxProcessor<_>) allocationMode (matrixLeft: ClMatrix.CSR<'a>) (matrixRight: ClMatrix.CSR<'b>) ->
            let result =
                kroneckerToCOO queue allocationMode matrixLeft matrixRight

            Option.bind (Some << (toCSRInplace queue allocationMode)) result
