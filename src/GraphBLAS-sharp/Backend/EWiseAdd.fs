namespace GraphBLAS.FSharp.Backend

open Brahma.OpenCL
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Backend.Common

module internal EWiseAdd =
    let coo (matrixLeft: COOFormat<'a>) (matrixRight: COOFormat<'a>) (mask: Mask2D option) (semiring: Semiring<'a>) : OpenCLEvaluation<COOFormat<'a>> =
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

        let allRows = Array.zeroCreate <| firstRows.Length + secondRows.Length
        let allColumns = Array.zeroCreate <| firstColumns.Length + secondColumns.Length
        let allValues = Array.create (firstValues.Length + secondValues.Length) zero

        let longSide = firstRows.Length
        let shortSide = secondRows.Length

        let allRowsLength = allRows.Length

        let createSortedConcatenation =
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

                    if i < allRowsLength then
                        let f n = if 0 > n + 1 - shortSide then 0 else n + 1 - shortSide
                        let mutable leftEdge = f i

                        let g n = if n > longSide - 1 then longSide - 1 else n
                        let mutable rightEdge = g i

                        while leftEdge <= rightEdge do
                            let middleIdx = (leftEdge + rightEdge) / 2
                            let firstRow = firstRowsBuffer.[middleIdx]
                            let firstColumn = firstColumnsBuffer.[middleIdx]
                            let secondRow = secondRowsBuffer.[i - middleIdx]
                            let secondColumn = secondColumnsBuffer.[i - middleIdx]
                            if firstRow < secondRow || firstRow = secondRow && firstColumn < secondColumn then leftEdge <- middleIdx + 1 else rightEdge <- middleIdx - 1

                        let boundaryX = rightEdge
                        let boundaryY = i - leftEdge

                        if boundaryX < 0 then
                            allRowsBuffer.[i] <- secondRowsBuffer.[boundaryY]
                            allColumnsBuffer.[i] <- secondColumnsBuffer.[boundaryY]
                            allValuesBuffer.[i] <- secondValuesBuffer.[boundaryY]
                        elif boundaryY < 0 then
                            allRowsBuffer.[i] <- firstRowsBuffer.[boundaryX]
                            allColumnsBuffer.[i] <- firstColumnsBuffer.[boundaryX]
                            allValuesBuffer.[i] <- firstValuesBuffer.[boundaryX]
                        else
                            let firstRow = firstRowsBuffer.[boundaryX]
                            let firstColumn = firstColumnsBuffer.[boundaryX]
                            let secondRow = secondRowsBuffer.[boundaryY]
                            let secondColumn = secondColumnsBuffer.[boundaryY]
                            if firstRow < secondRow || firstRow = secondRow && firstColumn < secondColumn then
                                allRowsBuffer.[i] <- secondRow
                                allColumnsBuffer.[i] <- secondColumn
                                allValuesBuffer.[i] <- secondValuesBuffer.[boundaryY]
                            else
                                allRowsBuffer.[i] <- firstRow
                                allColumnsBuffer.[i] <- firstColumn
                                allValuesBuffer.[i] <- firstValuesBuffer.[boundaryX]
            @>

        let createSortedConcatenation =
            opencl {
                let binder kernelP =
                    let ndRange = _1D(Utils.workSize allRows.Length, workGroupSize)
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
                do! RunCommand createSortedConcatenation binder
            }

        let auxiliaryArray = Array.create allRows.Length 1

        let fillAuxiliaryArray =
            <@
                fun (ndRange: _1D)
                    (allRowsBuffer: int[])
                    (allColumnsBuffer: int[])
                    (allValuesBuffer: 'a[])
                    (auxiliaryArrayBuffer: int[]) ->

                    let i = ndRange.GlobalID0 + 1

                    if i < allRowsLength && allRowsBuffer.[i - 1] = allRowsBuffer.[i] && allColumnsBuffer.[i - 1] = allColumnsBuffer.[i] then
                        auxiliaryArrayBuffer.[i] <- 0
                        let localResultBuffer = (%plus) allValuesBuffer.[i - 1] allValuesBuffer.[i]
                        //Drop explicit zeroes
                        if localResultBuffer = zero then auxiliaryArrayBuffer.[i] <- 0 else allValuesBuffer.[i] <- localResultBuffer
            @>

        let fillAuxiliaryArray =
            opencl {
                let binder kernelP =
                    let ndRange = _1D(Utils.workSize (allRows.Length - 1), workGroupSize)
                    kernelP
                        ndRange
                        allRows
                        allColumns
                        allValues
                        auxiliaryArray
                do! RunCommand fillAuxiliaryArray binder
            }

        let auxiliaryArrayLength = auxiliaryArray.Length

        let createUnion =
            <@
                fun (ndRange: _1D)
                    (allRowsBuffer: int[])
                    (allColumnsBuffer: int[])
                    (allValuesBuffer: 'a[])
                    (auxiliaryArrayBuffer: int[])
                    (prefixSumArrayBuffer: int[])
                    (resultRowsBuffer: int[])
                    (resultColumnsBuffer: int[])
                    (resultValuesBuffer: 'a[]) ->

                    let i = ndRange.GlobalID0

                    if i < auxiliaryArrayLength && auxiliaryArrayBuffer.[i] = 1 then
                        let index = prefixSumArrayBuffer.[i] - 1

                        resultRowsBuffer.[index] <- allRowsBuffer.[i]
                        resultColumnsBuffer.[index] <- allColumnsBuffer.[i]
                        resultValuesBuffer.[index] <- allValuesBuffer.[i]
            @>

        let resultRows = Array.zeroCreate allRows.Length
        let resultColumns = Array.zeroCreate allColumns.Length
        let resultValues = Array.create allValues.Length zero

        let createUnion =
            opencl {
                let! prefixSumArray = Scan.v2 auxiliaryArray
                let binder kernelP =
                    let ndRange = _1D(Utils.workSize auxiliaryArray.Length, workGroupSize)
                    kernelP
                        ndRange
                        allRows
                        allColumns
                        allValues
                        auxiliaryArray
                        prefixSumArray
                        resultRows
                        resultColumns
                        resultValues
                do! RunCommand createUnion binder
            }

        opencl {
            do! createSortedConcatenation
            do! filterThroughMask
            do! fillAuxiliaryArray
            do! createUnion

            return {
                RowCount = matrixLeft.RowCount
                ColumnCount = matrixLeft.ColumnCount
                Rows = resultRows
                Columns = resultColumns
                Values = resultValues
            }
        }
