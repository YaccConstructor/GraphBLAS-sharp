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
    type private optionalMatrix<'elem when 'elem: struct> =
        { RowCount: int
          ColumnCount: int
          Matrix: (ClArray<int> * ClArray<int> * ClArray<'elem>) option }

    let mergeDisjoint<'a when 'a: struct> (clContext: ClContext) workGroupSize =

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

    let private insertMatrixWithOffset (clContext: ClContext) workGroupSize =

        let mapWithValueClArray =
            ClArray.mapWithValue clContext workGroupSize <@ fun x y -> x + y @>

        let mergeDisjointCOO =
            mergeDisjoint clContext workGroupSize

        let copyClArray = ClArray.copy clContext workGroupSize

        fun (queue: MailboxProcessor<_>) allocationMode (rowOffset: int) (columnOffset: int) (matrixToInsert: optionalMatrix<'c>) (resultMatrix: optionalMatrix<'c>) ->

            match matrixToInsert.Matrix with
            | None -> resultMatrix

            | Some (rows, cols, vals) ->
                let rowOffset = rowOffset |> clContext.CreateClCell
                let columnOffset = columnOffset |> clContext.CreateClCell

                let newRowIndices =
                    mapWithValueClArray queue allocationMode rowOffset rows

                let newColumnIndices =
                    mapWithValueClArray queue allocationMode columnOffset cols

                queue.Post(Msg.CreateFreeMsg<_>(rowOffset))
                queue.Post(Msg.CreateFreeMsg<_>(columnOffset))

                match resultMatrix.Matrix with
                | None ->
                    { RowCount = resultMatrix.RowCount
                      ColumnCount = resultMatrix.ColumnCount
                      Matrix = Some(newRowIndices, newColumnIndices, copyClArray queue allocationMode vals) }

                | Some (mainRows, mainCols, mainVals) ->

                    let newMatrix =
                        mergeDisjointCOO queue mainRows mainCols mainVals newRowIndices newColumnIndices vals

                    { RowCount = resultMatrix.RowCount
                      ColumnCount = resultMatrix.ColumnCount
                      Matrix = Some newMatrix }

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
                match mapWithValueToCOO queue allocationMode noneClCell matrixRight with
                | None ->
                    { RowCount = matrixRight.RowCount
                      ColumnCount = matrixRight.RowCount
                      Matrix = None }
                | Some m ->
                    { RowCount = matrixRight.RowCount
                      ColumnCount = matrixRight.RowCount
                      Matrix = Some(m.Rows, m.Columns, m.Values) }

            queue.Post(Msg.CreateFreeMsg<_>(noneClCell))

            let mutable resultMatrix =
                { RowCount = matrixLeft.RowCount * matrixRight.RowCount
                  ColumnCount = matrixLeft.ColumnCount * matrixRight.ColumnCount
                  Matrix = None }

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
                                match resultMatrix.Matrix, mapWithZero.Matrix with
                                | Some (oldRows, oldCols, oldVals), Some _ ->
                                    queue.Post(Msg.CreateFreeMsg<_>(oldRows))
                                    queue.Post(Msg.CreateFreeMsg<_>(oldCols))
                                    queue.Post(Msg.CreateFreeMsg<_>(oldVals))
                                    newMatrix
                                | _ -> newMatrix

                    // inserting new matrix resulting from multiplication by non None element of left matrix
                    let columnOffset = currentColumn * matrixRight.ColumnCount

                    let operand =
                        Some leftMatrixVals.[firstElementIndex + offset]
                        |> clContext.CreateClCell

                    let mappedMatrix =
                        match mapWithValueToCOO queue allocationMode operand matrixRight with
                        | None ->
                            { RowCount = matrixRight.RowCount
                              ColumnCount = matrixRight.RowCount
                              Matrix = None }
                        | Some m ->
                            { RowCount = m.RowCount
                              ColumnCount = m.RowCount
                              Matrix = Some(m.Rows, m.Columns, m.Values) }

                    queue.Post(Msg.CreateFreeMsg<_>(operand))

                    let newMatrix =
                        insert queue allocationMode rowOffset columnOffset mappedMatrix resultMatrix

                    resultMatrix <-
                        match resultMatrix.Matrix, mappedMatrix.Matrix with
                        | Some (oldRows, oldCols, oldVals), Some (mappedRows, mappedCols, mappedVals) ->
                            queue.Post(Msg.CreateFreeMsg<_>(oldRows))
                            queue.Post(Msg.CreateFreeMsg<_>(oldCols))
                            queue.Post(Msg.CreateFreeMsg<_>(oldVals))
                            queue.Post(Msg.CreateFreeMsg<_>(mappedRows))
                            queue.Post(Msg.CreateFreeMsg<_>(mappedCols))
                            queue.Post(Msg.CreateFreeMsg<_>(mappedVals))
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
                        match resultMatrix.Matrix, mapWithZero.Matrix with
                        | Some (oldRows, oldCols, oldVals), Some _ ->
                            queue.Post(Msg.CreateFreeMsg<_>(oldRows))
                            queue.Post(Msg.CreateFreeMsg<_>(oldCols))
                            queue.Post(Msg.CreateFreeMsg<_>(oldVals))
                            newMatrix
                        | _ -> newMatrix

            match resultMatrix.Matrix with
            | None -> None
            | Some (rows, cols, vals) ->
                Some
                    { Context = clContext
                      RowCount = resultMatrix.RowCount
                      ColumnCount = resultMatrix.ColumnCount
                      Rows = rows
                      Columns = cols
                      Values = vals }

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

            match result with
            | None -> None
            | Some resultMatrix -> Some(resultMatrix |> toCSRInplace queue allocationMode)
