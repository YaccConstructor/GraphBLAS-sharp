namespace GraphBLAS.FSharp.Backend.Matrix.CSR

open Microsoft.FSharp.Quotations
open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Matrix.COO
open GraphBLAS.FSharp.Backend.Matrix.CSR
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Objects.ClCell
open GraphBLAS.FSharp.Backend.Objects.ClMatrix
open GraphBLAS.FSharp.Backend.Objects.ClContext
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
    let private insertWithOffset (clContext: ClContext) workGroupSize =

        let mapWithValueClArray =
            ClArray.mapWithValue clContext workGroupSize <@ fun x y -> x + y @>

        let mergeDisjointCOO = mergeDisjoint clContext workGroupSize

        let copyClArray = ClArray.copy clContext workGroupSize

        fun (queue: MailboxProcessor<_>) allocationMode (rowOffset: int) (columnOffset: int) (matrixToInsert: ClMatrix.COO<'c>) (resultMatrix: ClMatrix.COO<'c> option) ->
            let rowOffset = rowOffset |> clContext.CreateClCell
            let columnOffset = columnOffset |> clContext.CreateClCell

            let newRowIndices =
                mapWithValueClArray queue allocationMode rowOffset matrixToInsert.Rows

            let newColumnIndices =
                mapWithValueClArray queue allocationMode columnOffset matrixToInsert.Columns

            rowOffset.Free queue
            columnOffset.Free queue

            match resultMatrix with
            | None ->
                { matrixToInsert with
                      Rows = newRowIndices
                      Columns = newColumnIndices
                      Values = copyClArray queue allocationMode matrixToInsert.Values }
                |> Some
            | Some mainMatrix ->
                let newRows, newCols, newValues =
                    mergeDisjointCOO
                        queue
                        mainMatrix.Rows
                        mainMatrix.Columns
                        mainMatrix.Values
                        newRowIndices
                        newColumnIndices
                        matrixToInsert.Values

                { mainMatrix with
                      Rows = newRows
                      Columns = newCols
                      Values = newValues }
                |> Some

    let disposeOption queue (matrix: COO<_> option) =
        match matrix with
        | Some m -> m.Dispose queue
        | _ -> ()

    let insertZero
        queue
        insert
        (zeroCounts: int list array)
        (mappedZero: COO<'c> option)
        (matrixRight: CSR<'b>)
        sourceMatrix
        =

        let rowCount = zeroCounts.Length

        let insertMany sourceMatrix row firstColumn count =
            let rec insertManyRec iter matrix =
                if iter >= count then
                    matrix
                else
                    let rowOffset = row * matrixRight.RowCount

                    let columnOffset =
                        (firstColumn + iter) * matrixRight.ColumnCount

                    match mappedZero with
                    | Some m ->
                        let result = insert rowOffset columnOffset m matrix
                        disposeOption queue matrix
                        result
                    | _ -> matrix
                    |> insertManyRec (iter + 1)

            insertManyRec 0 sourceMatrix

        let rec insertInRowRec zeroCounts row column matrix =
            match zeroCounts with
            | [] -> matrix
            | h :: tl ->
                insertMany matrix row column h
                |> insertInRowRec tl row (h + column + 1)

        let rec insertZeroRec row matrix =
            if row >= rowCount then
                matrix
            else
                insertInRowRec zeroCounts.[row] row 0 matrix
                |> insertZeroRec (row + 1)

        insertZeroRec 0 sourceMatrix

    let insertNonZero
        queue
        map
        insert
        (rowsEdges: (int * int) [])
        (matrixRight: CSR<'b>)
        (leftValuesHost: 'a [])
        (leftColsHost: int [])
        (sourceMatrix: COO<'c> option)
        =

        let rowCount = rowsEdges.Length

        let rec insertInRowRec row rightEdge index matrix =
            if index > rightEdge then
                matrix
            else
                let value = leftValuesHost.[index] |> Some
                let column = leftColsHost.[index]

                let rowOffset = row * matrixRight.RowCount
                let columnOffset = column * matrixRight.ColumnCount

                let mappedMatrix = map value matrixRight

                match mappedMatrix with
                | Some m ->
                    let result = insert rowOffset columnOffset m matrix
                    disposeOption queue matrix
                    disposeOption queue mappedMatrix
                    result
                | _ -> matrix
                |> insertInRowRec row rightEdge (index + 1)

        let rec insertNonZeroRec row matrix =
            if row >= rowCount then
                matrix
            else
                let leftEdge, rightEdge = rowsEdges.[row]

                insertInRowRec row rightEdge leftEdge matrix
                |> insertNonZeroRec (row + 1)

        insertNonZeroRec 0 sourceMatrix

    let run<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct and 'c: equality>
        (clContext: ClContext)
        (op: Expr<'a option -> 'b option -> 'c option>)
        workGroupSize
        =

        let mapWithValueToCOO =
            Map.WithValue.runToCOO clContext op workGroupSize

        let insert = insertWithOffset clContext workGroupSize

        fun (queue: MailboxProcessor<_>) allocationMode (matrixLeft: ClMatrix.CSR<'a>) (matrixRight: ClMatrix.CSR<'b>) ->
            let map = mapWithValueToCOO queue allocationMode
            let insert = insert queue allocationMode

            let mappedZero = map None matrixRight

            let leftMatrixRows = matrixLeft.RowPointers.ToHost queue
            let leftMatrixCols = matrixLeft.Columns.ToHost queue
            let leftMatrixVals = matrixLeft.Values.ToHost queue

            let nnzInRows =
                leftMatrixRows
                |> Array.pairwise
                |> Array.map (fun (fst, snd) -> snd - fst)

            let rowsEdges =
                leftMatrixRows
                |> Array.pairwise
                |> Array.map (fun (fst, snd) -> (fst, snd - 1))

            let (zeroCounts: int list array) = Array.zeroCreate matrixLeft.RowCount

            (rowsEdges, [| 0 .. matrixLeft.RowCount - 1 |])
            ||> Array.iter2
                    (fun edges i ->
                        zeroCounts.[i] <-
                            leftMatrixCols.[fst edges..snd edges]
                            |> Array.toList
                            |> List.insertAt 0 -1
                            |> List.insertAt (nnzInRows.[i] + 1) matrixLeft.ColumnCount
                            |> List.pairwise
                            |> List.map (fun (fstCol, sndCol) -> sndCol - fstCol - 1))

            insertZero queue insert zeroCounts mappedZero matrixRight None
            |> insertNonZero queue map insert rowsEdges matrixRight leftMatrixVals leftMatrixCols
            |> Option.bind
                (fun m ->
                    { m with
                          RowCount = matrixLeft.RowCount * matrixRight.RowCount
                          ColumnCount = matrixLeft.ColumnCount * matrixRight.ColumnCount }
                    |> Some)
