namespace GraphBLAS.FSharp.Backend

open Brahma.OpenCL
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Backend.Common

module internal EWiseAdd =
    let cooNotEmpty (matrixLeft: COOFormat<'a>) (matrixRight: COOFormat<'a>) (mask: Mask2D option) (semiring: Semiring<'a>) : OpenCLEvaluation<COOFormat<'a>> =
        let workGroupSize = Utils.workGroupSize
        let (BinaryOp append) = semiring.PlusMonoid.Append
        let zero = semiring.PlusMonoid.Zero

        //It is useful to consider that the first array is longer than the second one
        let firstRows, firstColumns, firstValues, secondRows, secondColumns, secondValues, plus =
            if matrixLeft.Rows.Length > matrixRight.Rows.Length then
                matrixLeft.Rows, matrixLeft.Columns, matrixLeft.Values, matrixRight.Rows, matrixRight.Columns, matrixRight.Values, append
            else
                matrixRight.Rows, matrixRight.Columns, matrixRight.Values, matrixLeft.Rows, matrixLeft.Columns, matrixLeft.Values, <@ fun x y -> (%append) y x @>

        let filterThroughMask =
            opencl {
                //TODO
                ()
            }

        let longSide = firstValues.Length
        let shortSide = secondValues.Length
        let sumOfSides = shortSide + longSide

        let allRows = Array.zeroCreate sumOfSides
        let allColumns = Array.zeroCreate sumOfSides
        let allValues = Array.create sumOfSides zero

        let merge =
            <@
                fun (ndRange: _1D)
                    (firstRowsBuffer: int[])
                    (firstColumnsBuffer: int[])
                    (firstValuesBuffer: 'a[])
                    (secondRowsBuffer: int[])
                    (secondColumnsBuffer: int[])
                    (secondValuesBuffer: 'a[])
                    (allRowsBuffer: int[])
                    (allColumnsBuffer: int[])
                    (allValuesBuffer: 'a[]) ->

                    let i = ndRange.GlobalID0

                    if i < sumOfSides then
                        let mutable beginIdxLocal = local ()
                        let mutable endIdxLocal = local ()
                        let localID = ndRange.LocalID0
                        if localID < 2 then
                            let mutable x = localID * (workGroupSize - 1) + i - 1
                            if x >= sumOfSides then x <- sumOfSides - 1
                            let diagonalNumber = x

                            let mutable leftEdge = diagonalNumber + 1 - shortSide
                            if leftEdge < 0 then leftEdge <- 0

                            let mutable rightEdge = longSide - 1
                            if rightEdge > diagonalNumber then rightEdge <- diagonalNumber

                            while leftEdge <= rightEdge do
                                let middleIdx = (leftEdge + rightEdge) / 2
                                let firstIndex: uint64 = ((uint64 firstRowsBuffer.[middleIdx]) <<< 32) ||| (uint64 firstColumnsBuffer.[middleIdx])
                                let secondIndex: uint64 = ((uint64 secondRowsBuffer.[diagonalNumber - middleIdx]) <<< 32) ||| (uint64 secondColumnsBuffer.[diagonalNumber - middleIdx])
                                if firstIndex < secondIndex then leftEdge <- middleIdx + 1 else rightEdge <- middleIdx - 1

                            // Here localID equals either 0 or 1
                            if localID = 0 then beginIdxLocal <- leftEdge else endIdxLocal <- leftEdge
                        barrier ()

                        let beginIdx = beginIdxLocal
                        let endIdx = endIdxLocal
                        let firstLocalLength = endIdx - beginIdx
                        let mutable x = workGroupSize - firstLocalLength
                        if endIdx = longSide then x <- shortSide - i + localID + beginIdx
                        let secondLocalLength = x

                        //First indices are from 0 to firstLocalLength - 1 inclusive
                        //Second indices are from firstLocalLength to firstLocalLength + secondLocalLength - 1 inclusive
                        let localIndices = localArray<uint64> workGroupSize

                        if localID < firstLocalLength then
                            localIndices.[localID] <- ((uint64 firstRowsBuffer.[beginIdx + localID]) <<< 32) ||| (uint64 firstColumnsBuffer.[beginIdx + localID])
                        if localID < secondLocalLength then
                            localIndices.[firstLocalLength + localID] <- ((uint64 secondRowsBuffer.[i - beginIdx]) <<< 32) ||| (uint64 secondColumnsBuffer.[i - beginIdx])
                        barrier ()

                        let mutable leftEdge = localID + 1 - secondLocalLength
                        if leftEdge < 0 then leftEdge <- 0

                        let mutable rightEdge = firstLocalLength - 1
                        if rightEdge > localID then rightEdge <- localID

                        while leftEdge <= rightEdge do
                            let middleIdx = (leftEdge + rightEdge) / 2
                            let firstIndex = localIndices.[middleIdx]
                            let secondIndex = localIndices.[firstLocalLength + localID - middleIdx]
                            if firstIndex < secondIndex then leftEdge <- middleIdx + 1 else rightEdge <- middleIdx - 1

                        let boundaryX = rightEdge
                        let boundaryY = localID - leftEdge

                        // boundaryX and boundaryY can't be off the right edge of array (only off the left edge)
                        let isValidX = boundaryX >= 0
                        let isValidY = boundaryY >= 0

                        let mutable fstIdx = uint64 0
                        if isValidX then fstIdx <- localIndices.[boundaryX]

                        let mutable sndIdx = uint64 0
                        if isValidY then sndIdx <- localIndices.[firstLocalLength + boundaryY]

                        if not isValidX || isValidY && fstIdx < sndIdx then
                            allRowsBuffer.[i] <- int (sndIdx >>> 32)
                            allColumnsBuffer.[i] <- int sndIdx
                            allValuesBuffer.[i] <- secondValuesBuffer.[i - localID - beginIdx + boundaryY]
                        else
                            allRowsBuffer.[i] <- int (fstIdx >>> 32)
                            allColumnsBuffer.[i] <- int fstIdx
                            allValuesBuffer.[i] <- firstValuesBuffer.[beginIdx + boundaryX]
            @>

        let merge =
            opencl {
                let binder kernelP =
                    let ndRange = _1D(Utils.workSize sumOfSides, workGroupSize)
                    kernelP
                        ndRange
                        firstRows
                        firstColumns
                        firstValues
                        secondRows
                        secondColumns
                        secondValues
                        allRows
                        allColumns
                        allValues
                do! RunCommand merge binder
            }

        let auxiliaryArray = Array.create sumOfSides 1

        let fillAuxiliaryArray =
            <@
                fun (ndRange: _1D)
                    (allRowsBuffer: int[])
                    (allColumnsBuffer: int[])
                    (allValuesBuffer: 'a[])
                    (auxiliaryArrayBuffer: int[]) ->

                    let i = ndRange.GlobalID0

                    if i < sumOfSides - 1 && allRowsBuffer.[i] = allRowsBuffer.[i + 1] && allColumnsBuffer.[i] = allColumnsBuffer.[i + 1] then
                        auxiliaryArrayBuffer.[i] <- 0

                        //Do not drop explicit zeroes
                        allValuesBuffer.[i + 1] <- (%plus) allValuesBuffer.[i] allValuesBuffer.[i + 1]

                        //Drop explicit zeroes
                        // let localResultBuffer = (%plus) allValuesBuffer.[i] allValuesBuffer.[i + 1]
                        // if localResultBuffer = zero then auxiliaryArrayBuffer.[i + 1] <- 0 else allValuesBuffer.[i + 1] <- localResultBuffer
            @>

        let fillAuxiliaryArray =
            opencl {
                let binder kernelP =
                    let ndRange = _1D(Utils.workSize (sumOfSides - 1), workGroupSize)
                    kernelP
                        ndRange
                        allRows
                        allColumns
                        allValues
                        auxiliaryArray
                do! RunCommand fillAuxiliaryArray binder
            }

        let prefixSumArrayLength = auxiliaryArray.Length

        let createUnion =
            <@
                fun (ndRange: _1D)
                    (allRowsBuffer: int[])
                    (allColumnsBuffer: int[])
                    (allValuesBuffer: 'a[])
                    (prefixSumArrayBuffer: int[])
                    (resultRowsBuffer: int[])
                    (resultColumnsBuffer: int[])
                    (resultValuesBuffer: 'a[]) ->

                    let i = ndRange.GlobalID0

                    if i = prefixSumArrayLength - 1 || i < prefixSumArrayLength && prefixSumArrayBuffer.[i] <> prefixSumArrayBuffer.[i + 1] then
                        let index = prefixSumArrayBuffer.[i]

                        resultRowsBuffer.[index] <- allRowsBuffer.[i]
                        resultColumnsBuffer.[index] <- allColumnsBuffer.[i]
                        resultValuesBuffer.[index] <- allValuesBuffer.[i]
            @>

        let resultLength = Array.zeroCreate 1

        opencl {
            do! merge
            do! filterThroughMask
            do! fillAuxiliaryArray
            let! prefixSumArray, resultLength = Scan.run auxiliaryArray resultLength
            let! _ = ToHost resultLength
            let resultLength = resultLength.[0]
            let resultRows = Array.zeroCreate resultLength
            let resultColumns = Array.zeroCreate resultLength
            let resultValues = Array.create resultLength zero
            let binder kernelP =
                let ndRange = _1D(Utils.workSize prefixSumArray.Length, workGroupSize)
                kernelP
                    ndRange
                    allRows
                    allColumns
                    allValues
                    prefixSumArray
                    resultRows
                    resultColumns
                    resultValues
            do! RunCommand createUnion binder

            return {
                RowCount = matrixLeft.RowCount
                ColumnCount = matrixLeft.ColumnCount
                Rows = resultRows
                Columns = resultColumns
                Values = resultValues
            }
        }

    let coo (matrixLeft: COOFormat<'a>) (matrixRight: COOFormat<'a>) (mask: Mask2D option) (semiring: Semiring<'a>) : OpenCLEvaluation<COOFormat<'a>> =
        if matrixLeft.Values.Length = 0 then
            opencl {
                let! resultRows = Copy.run matrixRight.Rows
                let! resultColumns = Copy.run matrixRight.Columns
                let! resultValues = Copy.run matrixRight.Values

                return {
                    RowCount = matrixRight.RowCount
                    ColumnCount = matrixRight.ColumnCount
                    Rows = resultRows
                    Columns = resultColumns
                    Values = resultValues
                }
            }
        elif matrixRight.Values.Length = 0 then
            opencl {
                let! resultRows = Copy.run matrixLeft.Rows
                let! resultColumns = Copy.run matrixLeft.Columns
                let! resultValues = Copy.run matrixLeft.Values

                return {
                    RowCount = matrixLeft.RowCount
                    ColumnCount = matrixLeft.ColumnCount
                    Rows = resultRows
                    Columns = resultColumns
                    Values = resultValues
                }
            }
        else cooNotEmpty matrixLeft matrixRight mask semiring

    //let coo (matrixLeft: COOFormat<'a>) (matrixRight: COOFormat<'a>) (mask: Mask2D option) (semiring: Semiring<'a>) : OpenCLEvaluation<COOFormat<'a>> =
    //     let workGroupSize = Utils.workGroupSize
    //     let (BinaryOp append) = semiring.PlusMonoid.Append
    //     let zero = semiring.PlusMonoid.Zero

    //     //It is useful to consider that the first array is longer than the second one
    //     let firstRows, firstColumns, firstValues, secondRows, secondColumns, secondValues, plus =
    //         if matrixLeft.Rows.Length > matrixRight.Rows.Length then
    //             matrixLeft.Rows, matrixLeft.Columns, matrixLeft.Values, matrixRight.Rows, matrixRight.Columns, matrixRight.Values, append
    //         else
    //             matrixRight.Rows, matrixRight.Columns, matrixRight.Values, matrixLeft.Rows, matrixLeft.Columns, matrixLeft.Values, <@ fun x y -> (%append) y x @>

    //     let filterThroughMask =
    //         opencl {
    //             //TODO
    //             ()
    //         }

    //     let allRows = Array.zeroCreate <| firstRows.Length + secondRows.Length
    //     let allColumns = Array.zeroCreate <| firstColumns.Length + secondColumns.Length
    //     let allValues = Array.create (firstValues.Length + secondValues.Length) zero

    //     let longSide = firstRows.Length
    //     let shortSide = secondRows.Length

    //     let allRowsLength = allRows.Length

    //     let createSortedConcatenation =
    //         <@
    //             fun (ndRange: _1D)
    //                 (firstRowsBuffer: int[])
    //                 (firstColumnsBuffer: int[])
    //                 (firstValuesBuffer: 'a[])
    //                 (secondRowsBuffer: int[])
    //                 (secondColumnsBuffer: int[])
    //                 (secondValuesBuffer: 'a[])
    //                 (allRowsBuffer: int[])
    //                 (allColumnsBuffer: int[])
    //                 (allValuesBuffer: 'a[]) ->

    //                 let i = ndRange.GlobalID0

    //                 if i < allRowsLength then
    //                     let f n = if 0 > n + 1 - shortSide then 0 else n + 1 - shortSide
    //                     let mutable leftEdge = f i

    //                     let g n = if n > longSide - 1 then longSide - 1 else n
    //                     let mutable rightEdge = g i

    //                     while leftEdge <= rightEdge do
    //                         let middleIdx = (leftEdge + rightEdge) / 2
    //                         let firstRow = firstRowsBuffer.[middleIdx]
    //                         let firstColumn = firstColumnsBuffer.[middleIdx]
    //                         let secondRow = secondRowsBuffer.[i - middleIdx]
    //                         let secondColumn = secondColumnsBuffer.[i - middleIdx]
    //                         if firstRow < secondRow || firstRow = secondRow && firstColumn < secondColumn then leftEdge <- middleIdx + 1 else rightEdge <- middleIdx - 1

    //                     let boundaryX = rightEdge
    //                     let boundaryY = i - leftEdge

    //                     if boundaryX < 0 then
    //                         allRowsBuffer.[i] <- secondRowsBuffer.[boundaryY]
    //                         allColumnsBuffer.[i] <- secondColumnsBuffer.[boundaryY]
    //                         allValuesBuffer.[i] <- secondValuesBuffer.[boundaryY]
    //                     elif boundaryY < 0 then
    //                         allRowsBuffer.[i] <- firstRowsBuffer.[boundaryX]
    //                         allColumnsBuffer.[i] <- firstColumnsBuffer.[boundaryX]
    //                         allValuesBuffer.[i] <- firstValuesBuffer.[boundaryX]
    //                     else
    //                         let firstRow = firstRowsBuffer.[boundaryX]
    //                         let firstColumn = firstColumnsBuffer.[boundaryX]
    //                         let secondRow = secondRowsBuffer.[boundaryY]
    //                         let secondColumn = secondColumnsBuffer.[boundaryY]
    //                         if firstRow < secondRow || firstRow = secondRow && firstColumn < secondColumn then
    //                             allRowsBuffer.[i] <- secondRow
    //                             allColumnsBuffer.[i] <- secondColumn
    //                             allValuesBuffer.[i] <- secondValuesBuffer.[boundaryY]
    //                         else
    //                             allRowsBuffer.[i] <- firstRow
    //                             allColumnsBuffer.[i] <- firstColumn
    //                             allValuesBuffer.[i] <- firstValuesBuffer.[boundaryX]
    //         @>

    //     let createSortedConcatenation =
    //         opencl {
    //             let binder kernelP =
    //                 let ndRange = _1D(Utils.workSize allRows.Length, workGroupSize)
    //                 kernelP
    //                     ndRange
    //                     firstRows
    //                     firstColumns
    //                     firstValues
    //                     secondRows
    //                     secondColumns
    //                     secondValues
    //                     allRows
    //                     allColumns
    //                     allValues
    //             do! RunCommand createSortedConcatenation binder
    //         }

    //     let auxiliaryArray = Array.create allRows.Length 1

    //     let fillAuxiliaryArray =
    //         <@
    //             fun (ndRange: _1D)
    //                 (allRowsBuffer: int[])
    //                 (allColumnsBuffer: int[])
    //                 (allValuesBuffer: 'a[])
    //                 (auxiliaryArrayBuffer: int[]) ->

    //                 let i = ndRange.GlobalID0 + 1

    //                 if i < allRowsLength && allRowsBuffer.[i - 1] = allRowsBuffer.[i] && allColumnsBuffer.[i - 1] = allColumnsBuffer.[i] then
    //                     auxiliaryArrayBuffer.[i] <- 0
    //                     let localResultBuffer = (%plus) allValuesBuffer.[i - 1] allValuesBuffer.[i]
    //                     //Drop explicit zeroes
    //                     if localResultBuffer = zero then auxiliaryArrayBuffer.[i] <- 0 else allValuesBuffer.[i] <- localResultBuffer
    //         @>

    //     let fillAuxiliaryArray =
    //         opencl {
    //             let binder kernelP =
    //                 let ndRange = _1D(Utils.workSize (allRows.Length - 1), workGroupSize)
    //                 kernelP
    //                     ndRange
    //                     allRows
    //                     allColumns
    //                     allValues
    //                     auxiliaryArray
    //             do! RunCommand fillAuxiliaryArray binder
    //         }

    //     let auxiliaryArrayLength = auxiliaryArray.Length

    //     let createUnion =
    //         <@
    //             fun (ndRange: _1D)
    //                 (allRowsBuffer: int[])
    //                 (allColumnsBuffer: int[])
    //                 (allValuesBuffer: 'a[])
    //                 (auxiliaryArrayBuffer: int[])
    //                 (prefixSumArrayBuffer: int[])
    //                 (resultRowsBuffer: int[])
    //                 (resultColumnsBuffer: int[])
    //                 (resultValuesBuffer: 'a[]) ->

    //                 let i = ndRange.GlobalID0

    //                 if i < auxiliaryArrayLength && auxiliaryArrayBuffer.[i] = 1 then
    //                     let index = prefixSumArrayBuffer.[i] - 1

    //                     resultRowsBuffer.[index] <- allRowsBuffer.[i]
    //                     resultColumnsBuffer.[index] <- allColumnsBuffer.[i]
    //                     resultValuesBuffer.[index] <- allValuesBuffer.[i]
    //         @>

    //     let resultRows = Array.zeroCreate allRows.Length
    //     let resultColumns = Array.zeroCreate allColumns.Length
    //     let resultValues = Array.create allValues.Length zero
    //     let totalSum = Array.zeroCreate 1

    //     let createUnion =
    //         opencl {
    //             let! prefixSumArray, _ = Scan.run auxiliaryArray totalSum
    //             let binder kernelP =
    //                 let ndRange = _1D(Utils.workSize auxiliaryArray.Length, workGroupSize)
    //                 kernelP
    //                     ndRange
    //                     allRows
    //                     allColumns
    //                     allValues
    //                     auxiliaryArray
    //                     prefixSumArray
    //                     resultRows
    //                     resultColumns
    //                     resultValues
    //             do! RunCommand createUnion binder
    //         }

    //     opencl {
    //         do! createSortedConcatenation
    //         do! filterThroughMask
    //         do! fillAuxiliaryArray
    //         do! createUnion

    //         return {
    //             RowCount = matrixLeft.RowCount
    //             ColumnCount = matrixLeft.ColumnCount
    //             Rows = resultRows
    //             Columns = resultColumns
    //             Values = resultValues
    //         }
    //     }

