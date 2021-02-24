namespace GraphBLAS.FSharp

open Brahma.OpenCL
open Brahma.FSharp.OpenCL.Core
open Brahma.FSharp.OpenCL.Extensions
open GlobalContext
open Helpers
open FSharp.Quotations.Evaluator
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation
open Toolbox

type COOFormat<'a> = {
    Rows: int[]
    Columns: int[]
    Values: 'a[]
    RowCount: int
    ColumnCount: int
}

type CSRFormat<'a> = {
    Values: 'a[]
    Columns: int[]
    RowPointers: int[]
    ColumnCount: int
}
with
    static member CreateEmpty<'a>() = {
        Values = Array.zeroCreate<'a> 0
        Columns = Array.zeroCreate<int> 0
        RowPointers = Array.zeroCreate<int> 0
        ColumnCount = 0
    }

type CSRMatrix<'a when 'a : struct and 'a : equality>(csrTuples: CSRFormat<'a>) =
    inherit Matrix<'a>(csrTuples.RowPointers.Length - 1, csrTuples.ColumnCount)

    let rowCount = base.RowCount
    let columnCount = base.ColumnCount

    let spMV (vector: DenseVector<'a>) (mask: Mask1D) (semiring: Semiring<'a>) : OpenCLEvaluation<Vector<'a>> =
        let csrMatrixRowCount = rowCount
        let csrMatrixColumnCount = columnCount
        let vectorLength = vector.Size
        if csrMatrixColumnCount <> vectorLength then
            invalidArg
                "vector"
                (sprintf "Argument has invalid dimension. Need %i, but given %i" csrMatrixColumnCount vectorLength)

        let (BinaryOp plus) = semiring.PlusMonoid.Append
        let (BinaryOp mult) = semiring.Times

        let resultVector = Array.zeroCreate<'a> csrMatrixRowCount
        let command =
            <@
                fun (ndRange: _1D)
                    (resultBuffer: 'a[])
                    (csrValuesBuffer: 'a[])
                    (csrColumnsBuffer: int[])
                    (csrRowPointersBuffer: int[])
                    (vectorBuffer: 'a[]) ->

                    let i = ndRange.GlobalID0
                    let mutable localResultBuffer = resultBuffer.[i]
                    for k in csrRowPointersBuffer.[i] .. csrRowPointersBuffer.[i + 1] - 1 do
                        localResultBuffer <- (%plus) localResultBuffer
                            ((%mult) csrValuesBuffer.[k] vectorBuffer.[csrColumnsBuffer.[k]])
                    resultBuffer.[i] <- localResultBuffer
            @>

        let ndRange = _1D(csrMatrixRowCount)
        let binder = fun kernelPrepare ->
            kernelPrepare
                ndRange
                resultVector
                csrTuples.Values
                csrTuples.Columns
                csrTuples.RowPointers
                vector.Values

        opencl {
            do! RunCommand command binder
            return upcast DenseVector(resultVector, semiring.PlusMonoid)
        }

    // Not Implemented
    new(rows: int[], columns: int[], values: 'a[]) = CSRMatrix(CSRFormat.CreateEmpty())

    member this.Values = csrTuples.Values
    member this.Columns = csrTuples.Columns
    member this.RowPointers = csrTuples.RowPointers

    override this.Clear () = failwith "Not Implemented"
    override this.Copy () = failwith "Not Implemented"
    override this.Resize a b = failwith "Not Implemented"
    override this.GetNNZ () = failwith "Not Implemented"
    override this.GetTuples () = failwith "Not Implemented"
    override this.GetMask(?isComplemented: bool) =
        let isComplemented = defaultArg isComplemented false
        failwith "Not Implemented"
    override this.ToHost () = failwith "Not implemented"

    override this.Extract (mask: Mask2D option) : OpenCLEvaluation<Matrix<'a>> = failwith "Not Implemented"
    override this.Extract (colMask: Mask1D option * int) : OpenCLEvaluation<Vector<'a>> = failwith "Not Implemented"
    override this.Extract (rowMask: int * Mask1D option) : OpenCLEvaluation<Vector<'a>> = failwith "Not Implemented"
    override this.Extract (idx: int * int) : OpenCLEvaluation<Scalar<'a>> = failwith "Not Implemented"
    override this.Assign (mask: Mask2D option, value: Matrix<'a>) : OpenCLEvaluation<unit> = failwith "Not Implemented"
    override this.Assign (colMask: Mask1D option * int, value: Vector<'a>) : OpenCLEvaluation<unit> = failwith "Not Implemented"
    override this.Assign (rowMask: int * Mask1D option, value: Vector<'a>) : OpenCLEvaluation<unit> = failwith "Not Implemented"
    override this.Assign (idx: int * int, value: Scalar<'a>) : OpenCLEvaluation<unit> = failwith "Not Implemented"
    override this.Assign (mask: Mask2D option, value: Scalar<'a>) : OpenCLEvaluation<unit> = failwith "Not Implemented"
    override this.Assign (colMask: Mask1D option * int, value: Scalar<'a>) : OpenCLEvaluation<unit> = failwith "Not Implemented"
    override this.Assign (rowMask: int * Mask1D option, value: Scalar<'a>) : OpenCLEvaluation<unit> = failwith "Not Implemented"

    override this.Mxm a b c = failwith "Not Implemented"
    override this.Mxv a b c = failwith "Not Implemented"
    override this.EWiseAdd a b c = failwith "Not Implemented"
    override this.EWiseMult a b c = failwith "Not Implemented"
    override this.Apply a b  = failwith "Not Implemented"
    override this.Prune a b = failwith "Not Implemented"
    override this.ReduceIn a b = failwith "Not Implemented"
    override this.ReduceOut a b = failwith "Not Implemented"
    override this.Reduce a = failwith "Not Implemented"
    override this.Transpose () = failwith "Not Implemented"
    override this.Kronecker a b c = failwith "Not Implemented"

and COOMatrix<'a when 'a : struct and 'a : equality>(rowCount: int, columnCount: int, indices: uint64[], values: 'a[]) =
    inherit Matrix<'a>(rowCount, columnCount)

    let mutable indices = indices
    let mutable values = values
    // member this.Rows with get() = rows
    // member this.Columns with get() = columns
    member this.Indices with get() = indices
    member this.Values with get() = values

    new (rowCount: int, columnCount: int, rows: int[], columns: int[], values: 'a[]) =
        let indices =
            [| for i in 0 .. rows.Length - 1 do
                yield (uint64 rows.[i]) <<< 32 ||| (uint64 columns.[i]) |]
        COOMatrix(rowCount, columnCount, indices, values)

    override this.Clear () = failwith "Not Implemented"
    override this.Copy () = failwith "Not Implemented"
    override this.Resize a b = failwith "Not Implemented"
    override this.GetNNZ () = failwith "Not Implemented"

    override this.GetTuples () =

        let indicesLength = indices.Length

        let unpack =
            <@
                fun (ndRange: _1D)
                    (indicesBuffer: uint64[])
                    (rowsBuffer: int[])
                    (columnsBuffer: int[]) ->

                    let i = ndRange.GlobalID0
                    if i < indicesLength then
                        let doubleIndex = indicesBuffer.[i]
                        rowsBuffer.[i] <- int (doubleIndex >>> 32)
                        columnsBuffer.[i] <- int doubleIndex
            @>

        let rows = Array.zeroCreate indicesLength
        let columns = Array.zeroCreate indicesLength

        let binder kernelP =
            let ndRange = _1D(workSize indicesLength, workGroupSize)
            kernelP
                ndRange
                indices
                rows
                columns

        opencl {
            do! RunCommand unpack binder
            return {| Rows = rows; Columns = columns; Values = this.Values |}
        }

    override this.GetMask(?isComplemented: bool) =
        let isComplemented = defaultArg isComplemented false
        failwith "Not Implemented"

    override this.ToHost () =
        opencl {
            //let! tuples = this.GetTuples ()
            // let! _ = ToHost tuples.Rows
            // let! _ = ToHost tuples.Columns
            // let! _ = ToHost tuples.Values
            let! _ = ToHost this.Indices
            let! _ = ToHost this.Values

            return upcast this
        }

    override this.Extract (mask: Mask2D option) : OpenCLEvaluation<Matrix<'a>> = failwith "Not Implemented"
    override this.Extract (colMask: Mask1D option * int) : OpenCLEvaluation<Vector<'a>> = failwith "Not Implemented"
    override this.Extract (rowMask: int * Mask1D option) : OpenCLEvaluation<Vector<'a>> = failwith "Not Implemented"
    override this.Extract (idx: int * int) : OpenCLEvaluation<Scalar<'a>> = failwith "Not Implemented"
    override this.Assign (mask: Mask2D option, value: Matrix<'a>) : OpenCLEvaluation<unit> = failwith "Not Implemented"
    override this.Assign (colMask: Mask1D option * int, value: Vector<'a>) : OpenCLEvaluation<unit> = failwith "Not Implemented"
    override this.Assign (rowMask: int * Mask1D option, value: Vector<'a>) : OpenCLEvaluation<unit> = failwith "Not Implemented"
    override this.Assign (idx: int * int, value: Scalar<'a>) : OpenCLEvaluation<unit> = failwith "Not Implemented"
    override this.Assign (mask: Mask2D option, value: Scalar<'a>) : OpenCLEvaluation<unit> = failwith "Not Implemented"
    override this.Assign (colMask: Mask1D option * int, value: Scalar<'a>) : OpenCLEvaluation<unit> = failwith "Not Implemented"
    override this.Assign (rowMask: int * Mask1D option, value: Scalar<'a>) : OpenCLEvaluation<unit> = failwith "Not Implemented"

    override this.Mxm a b c = failwith "Not Implemented"
    override this.Mxv a b c = failwith "Not Implemented"

    member internal this.EWiseAddCOOA
        (matrix: COOMatrix<'a>)
        (mask: Mask2D option)
        (semiring: Semiring<'a>) : OpenCLEvaluation<Matrix<'a>> =

        let workGroupSize = Toolbox.workGroupSize
        let (BinaryOp append) = semiring.PlusMonoid.Append
        let zero = semiring.PlusMonoid.Zero

        //It is useful to consider that the first array is longer than the second one
        let firstIndices, firstValues, secondIndices, secondValues, plus =
            if this.Indices.Length > matrix.Indices.Length then
                this.Indices, this.Values, matrix.Indices, matrix.Values, append
            else
                matrix.Indices, matrix.Values, this.Indices, this.Values, <@ fun x y -> (%append) y x @>

        let filterThroughMask =
            opencl {
                //TODO
                ()
            }

        let allIndices = Array.zeroCreate <| firstIndices.Length + secondIndices.Length
        let allValues = Array.create (firstValues.Length + secondValues.Length) zero

        let longSide = firstIndices.Length
        let shortSide = secondIndices.Length

        let allIndicesLength = allIndices.Length

        let createSortedConcatenation =
            <@
                fun (ndRange: _1D)
                    (firstIndicesBuffer: uint64[])
                    (firstValuesBuffer: 'a[])
                    (secondIndicesBuffer: uint64[])
                    (secondValuesBuffer: 'a[])
                    (allIndicesBuffer: uint64[])
                    (allValuesBuffer: 'a[]) ->

                    let i = ndRange.GlobalID0

                    if i < allIndicesLength then
                        let f n = if 0 > n + 1 - shortSide then 0 else n + 1 - shortSide
                        let mutable leftEdge = f i

                        let g n = if n > longSide - 1 then longSide - 1 else n
                        let mutable rightEdge = g i

                        while leftEdge <= rightEdge do
                            let middleIdx = (leftEdge + rightEdge) / 2
                            let firstIndex = firstIndicesBuffer.[middleIdx]
                            let secondIndex = secondIndicesBuffer.[i - middleIdx]
                            if firstIndex < secondIndex then leftEdge <- middleIdx + 1 else rightEdge <- middleIdx - 1

                        let boundaryX = rightEdge
                        let boundaryY = i - leftEdge

                        if boundaryX < 0 then
                            allIndicesBuffer.[i] <- secondIndicesBuffer.[boundaryY]
                            allValuesBuffer.[i] <- secondValuesBuffer.[boundaryY]
                        elif boundaryY < 0 then
                            allIndicesBuffer.[i] <- firstIndicesBuffer.[boundaryX]
                            allValuesBuffer.[i] <- firstValuesBuffer.[boundaryX]
                        else
                            let firstIndex = firstIndicesBuffer.[boundaryX]
                            let secondIndex = secondIndicesBuffer.[boundaryY]
                            if firstIndex < secondIndex then
                                allIndicesBuffer.[i] <- secondIndex
                                allValuesBuffer.[i] <- secondValuesBuffer.[boundaryY]
                            else
                                allIndicesBuffer.[i] <- firstIndex
                                allValuesBuffer.[i] <- firstValuesBuffer.[boundaryX]

                    // let i = ndRange.GlobalID0

                    // if i < allIndicesLength then
                    //     let knots = localArray<int> 2
                    //     let localID = ndRange.LocalID0
                    //     if localID < 2 then
                    //         let mutable x = localID * (workGroupSize - 1) + i - 1
                    //         if x >= shortSide + longSide then x <- shortSide + longSide - 1
                    //         let diagonalNumber = x

                    //         let mutable leftEdge = diagonalNumber + 1 - shortSide
                    //         if leftEdge < 0 then leftEdge <- 0

                    //         let mutable rightEdge = longSide - 1
                    //         if rightEdge > diagonalNumber then rightEdge <- diagonalNumber

                    //         while leftEdge <= rightEdge do
                    //             let middleIdx = (leftEdge + rightEdge) / 2
                    //             let firstIndex = firstIndicesBuffer.[middleIdx]
                    //             let secondIndex = secondIndicesBuffer.[diagonalNumber - middleIdx]
                    //             if firstIndex < secondIndex then leftEdge <- middleIdx + 1 else rightEdge <- middleIdx - 1

                    //         knots.[localID] <- leftEdge
                    //     barrier ()

                    //     let beginIdx = knots.[0] // BANK CONFLICTS?
                    //     let endIdx = knots.[1]
                    //     let firstLocalLength = endIdx - beginIdx
                    //     let mutable x = workGroupSize - firstLocalLength
                    //     if endIdx = longSide then x <- shortSide - i + localID + beginIdx
                    //     let secondLocalLength = x

                    //     //First indices are from 0 to firstLocalLength - 1 inclusive
                    //     //Second indices are from firstLocalLength to firstLocalLength + secondLocalLength - 1 inclusive
                    //     let localIndices = localArray<uint64> workGroupSize

                    //     if localID < firstLocalLength then
                    //         localIndices.[localID] <- firstIndicesBuffer.[beginIdx + localID]
                    //     if localID < secondLocalLength then
                    //         localIndices.[firstLocalLength + localID] <- secondIndicesBuffer.[i - beginIdx]
                    //     barrier ()

                    //     let mutable leftEdge = localID + 1 - secondLocalLength
                    //     if leftEdge < 0 then leftEdge <- 0

                    //     let mutable rightEdge = firstLocalLength - 1
                    //     if rightEdge > localID then rightEdge <- localID

                    //     while leftEdge <= rightEdge do
                    //         let middleIdx = (leftEdge + rightEdge) / 2
                    //         let firstIndex= localIndices.[middleIdx]
                    //         let secondIndex = localIndices.[firstLocalLength + localID - middleIdx]
                    //         if firstIndex < secondIndex then leftEdge <- middleIdx + 1 else rightEdge <- middleIdx - 1

                    //     let boundaryX = rightEdge
                    //     let boundaryY = localID - leftEdge

                    //     let isValidX = boundaryX >= 0
                    //     let isValidY = boundaryY >= 0

                    //     let mutable fstIdx = uint64 0
                    //     if isValidX then fstIdx <- localIndices.[boundaryX]

                    //     let mutable sndIdx = uint64 0
                    //     if isValidY then sndIdx <- localIndices.[firstLocalLength + boundaryY]

                    //     if not isValidX || isValidY && fstIdx < sndIdx then
                    //         allIndicesBuffer.[i] <- sndIdx
                    //         allValuesBuffer.[i] <- secondValuesBuffer.[i - localID - beginIdx + boundaryY]
                    //     else
                    //         allIndicesBuffer.[i] <- fstIdx
                    //         allValuesBuffer.[i] <- firstValuesBuffer.[beginIdx + boundaryX]
            @>

        let createSortedConcatenation =
            opencl {
                let binder kernelP =
                    let ndRange = _1D(workSize allIndices.Length, workGroupSize)
                    kernelP
                        ndRange
                        firstIndices
                        firstValues
                        secondIndices
                        secondValues
                        allIndices
                        allValues
                do! RunCommand createSortedConcatenation binder
            }

        let auxiliaryArray = Array.create allIndices.Length 1

        let fillAuxiliaryArray =
            <@
                fun (ndRange: _1D)
                    (allIndicesBuffer: uint64[])
                    (allValuesBuffer: 'a[])
                    (auxiliaryArrayBuffer: int[]) ->

                    let i = ndRange.GlobalID0

                    if i < allIndicesLength - 1 && allIndicesBuffer.[i] = allIndicesBuffer.[i + 1] then
                        auxiliaryArrayBuffer.[i + 1] <- 0

                        //Do not drop explicit zeroes
                        allValuesBuffer.[i] <- (%plus) allValuesBuffer.[i] allValuesBuffer.[i + 1]

                        //Drop explicit zeroes
                        //let localResultBuffer = (%plus) allValuesBuffer.[i] allValuesBuffer.[i + 1]
                        //if localResultBuffer = zero then auxiliaryArrayBuffer.[i] <- 0 else allValuesBuffer.[i] <- localResultBuffer
            @>

        let fillAuxiliaryArray =
            opencl {
                let binder kernelP =
                    let ndRange = _1D(workSize (allIndicesLength - 1), workGroupSize)
                    kernelP
                        ndRange
                        allIndices
                        allValues
                        auxiliaryArray
                do! RunCommand fillAuxiliaryArray binder
            }

        let auxiliaryArrayLength = auxiliaryArray.Length

        let createUnion =
            <@
                fun (ndRange: _1D)
                    (allIndicesBuffer: uint64[])
                    (allValuesBuffer: 'a[])
                    (auxiliaryArrayBuffer: int[])
                    (prefixSumArrayBuffer: int[])
                    (resultIndicesBuffer: uint64[])
                    (resultValuesBuffer: 'a[]) ->

                    let i = ndRange.GlobalID0

                    if i < auxiliaryArrayLength && auxiliaryArrayBuffer.[i] = 1 then
                        let index = prefixSumArrayBuffer.[i] + 1

                        resultIndicesBuffer.[index] <- allIndicesBuffer.[i]
                        resultValuesBuffer.[index] <- allValuesBuffer.[i]
            @>

        let resultIndices = Array.zeroCreate allIndices.Length
        let resultValues = Array.create allValues.Length zero

        let createUnion =
            opencl {
                let! prefixSumArray = prefixSum2 auxiliaryArray
                let binder kernelP =
                    let ndRange = _1D(workSize auxiliaryArray.Length, workGroupSize)
                    kernelP
                        ndRange
                        allIndices
                        allValues
                        auxiliaryArray
                        prefixSumArray
                        resultIndices
                        resultValues
                do! RunCommand createUnion binder
            }

        opencl {
            do! createSortedConcatenation
            do! filterThroughMask
            do! fillAuxiliaryArray
            do! createUnion

            return upcast COOMatrix<'a>(this.RowCount, this.ColumnCount, resultIndices, resultValues)
        }

    member internal this.EWiseAddCOO
        (matrix: COOMatrix<'a>)
        (mask: Mask2D option)
        (semiring: Semiring<'a>) : OpenCLEvaluation<Matrix<'a>> =

        let workGroupSize = Toolbox.workGroupSize
        let (BinaryOp append) = semiring.PlusMonoid.Append
        let zero = semiring.PlusMonoid.Zero

        //It is useful to consider that the first array is longer than the second one
        let firstIndices, firstValues, secondIndices, secondValues, plus =
            if this.Indices.Length > matrix.Indices.Length then
                this.Indices, this.Values, matrix.Indices, matrix.Values, append
            else
                matrix.Indices, matrix.Values, this.Indices, this.Values, <@ fun x y -> (%append) y x @>

        let filterThroughMask =
            opencl {
                //TODO
                ()
            }

        let allIndices = Array.zeroCreate <| firstIndices.Length + secondIndices.Length
        let allValues = Array.create (firstValues.Length + secondValues.Length) zero

        let longSide = firstIndices.Length
        let shortSide = secondIndices.Length

        let allIndicesLength = allIndices.Length

        let knots = Array.zeroCreate <| (shortSide + longSide + workGroupSize - 1) / workGroupSize + 1
        knots.[knots.Length - 1] <- longSide
        let knotsLength = knots.Length

        let prepareToCreateSortedConcatenation =
            <@
                fun (ndRange: _1D)
                    (firstIndicesBuffer: uint64[])
                    (secondIndicesBuffer: uint64[])
                    (knotsBuffer: int[]) ->

                    let i = ndRange.GlobalID0 + 1

                    if i < knotsLength then
                        let mutable x = i * workGroupSize - 1
                        if x >= shortSide + longSide then x <- shortSide + longSide - 1
                        let diagonalNumber = x

                        let mutable leftEdge = 0
                        if 0 <= diagonalNumber + 1 - shortSide then leftEdge <- diagonalNumber + 1 - shortSide

                        let mutable rightEdge = diagonalNumber
                        if diagonalNumber > longSide - 1 then rightEdge <- longSide - 1

                        while leftEdge <= rightEdge do
                            let middleIdx = (leftEdge + rightEdge) / 2
                            let firstIndex = firstIndicesBuffer.[middleIdx]
                            let secondIndex = secondIndicesBuffer.[diagonalNumber - middleIdx]
                            if firstIndex < secondIndex then leftEdge <- middleIdx + 1 else rightEdge <- middleIdx - 1

                        knotsBuffer.[i] <- leftEdge
            @>

        let prepareToCreateSortedConcatenation =
            opencl {
                let binder kernelP =
                    let ndRange = _1D(workSize (knotsLength - 1), workGroupSize)
                    kernelP
                        ndRange
                        firstIndices
                        secondIndices
                        knots
                do! RunCommand prepareToCreateSortedConcatenation binder
            }

        let createSortedConcatenation =
            <@
                fun (ndRange: _1D)
                    (knotsBuffer: int[])
                    (firstIndicesBuffer: uint64[])
                    (firstValuesBuffer: 'a[])
                    (secondIndicesBuffer: uint64[])
                    (secondValuesBuffer: 'a[])
                    (allIndicesBuffer: uint64[])
                    (allValuesBuffer: 'a[]) ->

                    let i = ndRange.GlobalID0

                    if i < allIndicesLength then
                        let localID = ndRange.LocalID0
                        let workGroupNumber = i / workGroupSize
                        let beginIdx = knotsBuffer.[workGroupNumber]
                        let endIdx = knotsBuffer.[workGroupNumber + 1]
                        let firstLocalLength = endIdx - beginIdx
                        let mutable x = workGroupSize - firstLocalLength
                        if endIdx = longSide then x <- shortSide - i + localID + beginIdx
                        let secondLocalLength = x

                        let firstIndicesLocalBuffer = localArray<uint64> workGroupSize
                        let secondIndicesLocalBuffer = localArray<uint64> workGroupSize

                        if localID < firstLocalLength then
                            firstIndicesLocalBuffer.[localID] <- firstIndicesBuffer.[beginIdx + localID]
                        if localID < secondLocalLength then
                            secondIndicesLocalBuffer.[localID] <- secondIndicesBuffer.[i - beginIdx]

                        barrier ()

                        let mutable leftEdge = 0
                        if 0 <= localID + 1 - secondLocalLength then leftEdge <- localID + 1 - secondLocalLength

                        let mutable rightEdge = localID
                        if localID > firstLocalLength - 1 then rightEdge <- firstLocalLength - 1

                        while leftEdge <= rightEdge do
                            let middleIdx = (leftEdge + rightEdge) / 2
                            let firstIndex = firstIndicesLocalBuffer.[middleIdx]
                            let secondIndex = secondIndicesLocalBuffer.[localID - middleIdx]
                            if firstIndex < secondIndex then leftEdge <- middleIdx + 1 else rightEdge <- middleIdx - 1

                        let boundaryX = rightEdge
                        let boundaryY = localID - leftEdge

                        if boundaryX < 0 then
                            allIndicesBuffer.[i] <- secondIndicesLocalBuffer.[boundaryY]
                            allValuesBuffer.[i] <- secondValuesBuffer.[i - localID - beginIdx + boundaryY]
                        elif boundaryY < 0 then
                            allIndicesBuffer.[i] <- firstIndicesLocalBuffer.[boundaryX]
                            allValuesBuffer.[i] <- firstValuesBuffer.[beginIdx + boundaryX]
                        else
                            let firstIndex = firstIndicesLocalBuffer.[boundaryX]
                            let secondIndex = secondIndicesLocalBuffer.[boundaryY]
                            if firstIndex < secondIndex then
                                allIndicesBuffer.[i] <- secondIndex
                                allValuesBuffer.[i] <- secondValuesBuffer.[i - localID - beginIdx + boundaryY]
                            else
                                allIndicesBuffer.[i] <- firstIndex
                                allValuesBuffer.[i] <- firstValuesBuffer.[beginIdx + boundaryX]
            @>

        let createSortedConcatenation =
            opencl {
                let binder kernelP =
                    let ndRange = _1D(workSize allIndices.Length, workGroupSize)
                    kernelP
                        ndRange
                        knots
                        firstIndices
                        firstValues
                        secondIndices
                        secondValues
                        allIndices
                        allValues
                do! RunCommand createSortedConcatenation binder
            }

        let auxiliaryArray = Array.create allIndices.Length 1

        let fillAuxiliaryArray =
            <@
                fun (ndRange: _1D)
                    (allIndicesBuffer: uint64[])
                    (allValuesBuffer: 'a[])
                    (auxiliaryArrayBuffer: int[]) ->

                    let i = ndRange.GlobalID0 + 1

                    if i < allIndicesLength && allIndicesBuffer.[i - 1] = allIndicesBuffer.[i] then
                        auxiliaryArrayBuffer.[i] <- 0
                        let localResultBuffer = (%plus) allValuesBuffer.[i - 1] allValuesBuffer.[i]
                        if localResultBuffer = zero then auxiliaryArrayBuffer.[i] <- 0 else allValuesBuffer.[i] <- localResultBuffer
            @>

        let fillAuxiliaryArray =
            opencl {
                let binder kernelP =
                    let ndRange = _1D(workSize (allIndices.Length - 1), workGroupSize)
                    kernelP
                        ndRange
                        allIndices
                        allValues
                        auxiliaryArray
                do! RunCommand fillAuxiliaryArray binder
            }

        let auxiliaryArrayLength = auxiliaryArray.Length

        let createUnion =
            <@
                fun (ndRange: _1D)
                    (allIndicesBuffer: uint64[])
                    (allValuesBuffer: 'a[])
                    (auxiliaryArrayBuffer: int[])
                    (prefixSumArrayBuffer: int[])
                    (resultIndicesBuffer: uint64[])
                    (resultValuesBuffer: 'a[]) ->

                    let i = ndRange.GlobalID0

                    if i < auxiliaryArrayLength && auxiliaryArrayBuffer.[i] = 1 then
                        let index = prefixSumArrayBuffer.[i] - 1

                        resultIndicesBuffer.[index] <- allIndicesBuffer.[i]
                        resultValuesBuffer.[index] <- allValuesBuffer.[i]
            @>

        let resultIndices = Array.zeroCreate allIndices.Length
        let resultValues = Array.create allValues.Length zero

        let createUnion =
            opencl {
                let! prefixSumArray = Toolbox.prefixSum2 auxiliaryArray
                let binder kernelP =
                    let ndRange = _1D(workSize auxiliaryArray.Length, workGroupSize)
                    kernelP
                        ndRange
                        allIndices
                        allValues
                        auxiliaryArray
                        prefixSumArray
                        resultIndices
                        resultValues
                do! RunCommand createUnion binder
            }

        opencl {
            do! prepareToCreateSortedConcatenation
            do! createSortedConcatenation
            do! filterThroughMask
            do! fillAuxiliaryArray
            do! createUnion

            return upcast COOMatrix<'a>(this.RowCount, this.ColumnCount, resultIndices, resultValues)
        }

    override this.EWiseAdd
        (matrix: Matrix<'a>)
        (mask: Mask2D option)
        (semiring: Semiring<'a>) =

        if (this.RowCount, this.ColumnCount) <> (matrix.RowCount, matrix.ColumnCount) then
            invalidArg
                "matrix"
                (sprintf "Argument has invalid dimension. Need %A, but given %A" (this.RowCount, this.ColumnCount) (matrix.RowCount, matrix.ColumnCount))

        // let mask =
        //     match matrixMask with
        //     | Some m ->
        //         if (m.RowCount, m.ColumnCount) <> (this.RowCount, this.ColumnCount) then
        //             invalidArg
        //                 "mask"
        //                 (sprintf "Argument has invalid dimension. Need %A, but given %A" (this.RowCount, this.ColumnCount) (m.RowCount, m.ColumnCount))
        //         m
        //     | _ -> Mask2D(Array.empty, this.RowCount, this.ColumnCount, true) // Empty complemented mask is equal to none

        match matrix with
        | :? COOMatrix<'a> -> this.EWiseAddCOO (downcast matrix) mask semiring
        | _ -> failwith "Not Implemented"

    override this.EWiseMult a b c = failwith "Not Implemented"
    override this.Apply a b  = failwith "Not Implemented"
    override this.Prune a b = failwith "Not Implemented"
    override this.ReduceIn a b = failwith "Not Implemented"
    override this.ReduceOut a b = failwith "Not Implemented"
    override this.Reduce a = failwith "Not Implemented"
    override this.Transpose () = failwith "Not Implemented"
    override this.Kronecker a b c = failwith "Not Implemented"

and SparseVector<'a when 'a : struct and 'a : equality>(size: int, indices: int[], values: 'a[]) =
    inherit Vector<'a>(size)

    let mutable indices, values = indices, values
    member this.Values with get() = values
    member this.Indices with get() = indices
    member this.Elements with get() = (indices, values) ||> Array.zip

    override this.Clear () = failwith "Not Implemented"
    override this.Copy () = failwith "Not Implemented"
    override this.Resize a = failwith "Not Implemented"
    override this.GetNNZ () = failwith "Not Implemented"

    override this.GetTuples () =
        opencl {
            return {| Indices = this.Indices; Values = this.Values |}
        }

    override this.GetMask(?isComplemented: bool) =
        let isComplemented = defaultArg isComplemented false
        failwith "Not Implemented"

    override this.ToHost () =
        opencl {
            let! _ = ToHost this.Indices
            let! _ = ToHost this.Values

            return upcast this
        }

    override this.Extract (mask: Mask1D option) : OpenCLEvaluation<Vector<'a>> = failwith "Not Implemented"
    override this.Extract (idx: int) : OpenCLEvaluation<Scalar<'a>> = failwith "Not Implemented"
    override this.Assign (mask: Mask1D option, vector: Vector<'a>) : OpenCLEvaluation<unit> = failwith "Not Implemented"
    override this.Assign (idx: int, Scalar (value: 'a)) : OpenCLEvaluation<unit> = failwith "Not Implemented"
    override this.Assign (mask: Mask1D option, Scalar (value: 'a)) : OpenCLEvaluation<unit> = failwith "Not Implemented"

    override this.Vxm (matrix: Matrix<'a>) (mask: Mask1D option) (semiring: Semiring<'a>) : OpenCLEvaluation<Vector<'a>> = failwith "Not Implemented"

    member internal this.EWiseAddSparse
        (vector: SparseVector<'a>)
        (mask: Mask1D option)
        (semiring: Semiring<'a>) : OpenCLEvaluation<Vector<'a>> =

        let (BinaryOp append) = semiring.PlusMonoid.Append
        let zero = semiring.PlusMonoid.Zero

        //It is useful to consider that the first array is longer than the second one
        let firstIndices, firstValues, secondIndices, secondValues, plus =
            if this.Indices.Length > vector.Indices.Length then
                this.Indices, this.Values, vector.Indices, vector.Values, append
            else
                vector.Indices, vector.Values, this.Indices, this.Values, <@ fun x y -> (%append) y x @>

        let filterThroughMask =
            opencl {
                //TODO
                ()
            }

        let allIndices = Array.zeroCreate <| firstIndices.Length + secondIndices.Length
        let allValues = Array.create (firstValues.Length + secondValues.Length) zero

        let longSide = firstIndices.Length
        let shortSide = secondIndices.Length

        let allIndicesLength = allIndices.Length

        let createSortedConcatenation =
            <@
                fun (ndRange: _1D)
                    (firstIndicesBuffer: int[])
                    (firstValuesBuffer: 'a[])
                    (secondIndicesBuffer: int[])
                    (secondValuesBuffer: 'a[])
                    (allIndicesBuffer: int[])
                    (allValuesBuffer: 'a[]) ->

                    let i = ndRange.GlobalID0
                    if i < allIndicesLength then
                        let f n = if 0 > n + 1 - shortSide then 0 else n + 1 - shortSide
                        let mutable leftEdge = f i

                        let g n = if n > longSide - 1 then longSide - 1 else n
                        let mutable rightEdge = g i

                        while leftEdge <= rightEdge do
                            let middleIdx = (leftEdge + rightEdge) / 2
                            if firstIndicesBuffer.[middleIdx] < secondIndicesBuffer.[i - middleIdx] then leftEdge <- middleIdx + 1 else rightEdge <- middleIdx - 1

                        let boundaryX, boundaryY = rightEdge, i - leftEdge

                        if boundaryX < 0 then
                            allIndicesBuffer.[i] <- secondIndicesBuffer.[boundaryY]
                            allValuesBuffer.[i] <- secondValuesBuffer.[boundaryY]
                        elif boundaryY < 0 then
                            allIndicesBuffer.[i] <- firstIndicesBuffer.[boundaryX]
                            allValuesBuffer.[i] <- firstValuesBuffer.[boundaryX]
                        else
                            let firstIndex = firstIndicesBuffer.[boundaryX]
                            let secondIndex = secondIndicesBuffer.[boundaryY]
                            if firstIndex < secondIndex then
                                allIndicesBuffer.[i] <- secondIndex
                                allValuesBuffer.[i] <- secondValuesBuffer.[boundaryY]
                            else
                                allIndicesBuffer.[i] <- firstIndex
                                allValuesBuffer.[i] <- firstValuesBuffer.[boundaryX]
            @>

        let createSortedConcatenation =
            opencl {
                let binder kernelP =
                    let ndRange = _1D(workSize allIndices.Length, workGroupSize)
                    kernelP
                        ndRange
                        firstIndices
                        firstValues
                        secondIndices
                        secondValues
                        allIndices
                        allValues
                do! RunCommand createSortedConcatenation binder
            }

        let auxiliaryArray = Array.create allIndices.Length 1

        let fillAuxiliaryArray =
            <@
                fun (ndRange: _1D)
                    (allIndicesBuffer: int[])
                    (allValuesBuffer: 'a[])
                    (auxiliaryArrayBuffer: int[]) ->

                    let i = ndRange.GlobalID0

                    if i + 1 < allIndicesLength && allIndicesBuffer.[i] = allIndicesBuffer.[i + 1] then
                        auxiliaryArrayBuffer.[i + 1] <- 0
                        let localResultBuffer = (%plus) allValuesBuffer.[i] allValuesBuffer.[i + 1]
                        if localResultBuffer = zero then auxiliaryArrayBuffer.[i] <- 0 else allValuesBuffer.[i] <- localResultBuffer
            @>

        let fillAuxiliaryArray =
            opencl {
                let binder kernelP =
                    let ndRange = _1D(workSize (allIndices.Length - 1), workGroupSize)
                    kernelP
                        ndRange
                        allIndices
                        allValues
                        auxiliaryArray
                do! RunCommand fillAuxiliaryArray binder
            }

        let auxiliaryArrayLength = auxiliaryArray.Length

        let createUnion =
            <@
                fun (ndRange: _1D)
                    (allIndicesBuffer: int[])
                    (allValuesBuffer: 'a[])
                    (auxiliaryArrayBuffer: int[])
                    (prefixSumArrayBuffer: int[])
                    (resultIndicesBuffer: int[])
                    (resultValuesBuffer: 'a[]) ->

                    let i = ndRange.GlobalID0

                    if i < auxiliaryArrayLength && auxiliaryArrayBuffer.[i] = 1 then
                        let index = prefixSumArrayBuffer.[i] - 1

                        resultIndicesBuffer.[index] <- allIndicesBuffer.[i]
                        resultValuesBuffer.[index] <- allValuesBuffer.[i]
            @>

        let resultIndices = Array.zeroCreate allIndices.Length
        let resultValues = Array.create allValues.Length zero

        let createUnion =
            opencl {
                let! prefixSumArray = Toolbox.prefixSum2 auxiliaryArray
                let binder kernelP =
                    let ndRange = _1D(workSize auxiliaryArray.Length, workGroupSize)
                    kernelP
                        ndRange
                        allIndices
                        allValues
                        auxiliaryArray
                        prefixSumArray
                        resultIndices
                        resultValues
                do! RunCommand createUnion binder
            }

        opencl {
            do! createSortedConcatenation
            do! filterThroughMask
            do! fillAuxiliaryArray
            do! createUnion

            return upcast SparseVector<'a>(this.Size, resultIndices, resultValues)
        }

    override this.EWiseAdd
        (vector: Vector<'a>)
        (mask: Mask1D option)
        (semiring: Semiring<'a>) =

        if vector.Size <> this.Size then
            invalidArg
                "vector"
                (sprintf "Argument has invalid dimension. Need %i, but given %i" this.Size vector.Size)

        // let mask =
        //     match mask with
        //     | Some m ->
        //         if m.Size <> this.Size then
        //             invalidArg
        //                 "mask"
        //                 (sprintf "Argument has invalid dimension. Need %i, but given %i" this.Size m.Size)
        //         m
        //     | _ -> Mask1D(Array.empty, this.Size, true) // Empty complemented mask is equal to none

        match vector with
        | :? SparseVector<'a> -> this.EWiseAddSparse (downcast vector) mask semiring
        | _ -> failwith "Not Implemented"

    override this.EWiseMult a b c = failwith "Not Implemented"
    override this.Apply a b = failwith "Not Implemented"
    override this.Prune a b = failwith "Not Implemented"
    override this.Reduce (monoid: Monoid<'a>) = failwith "Not Implemented"

and DenseVector<'a when 'a : struct and 'a : equality>(vector: 'a[], monoid: Monoid<'a>) =
    inherit Vector<'a>(vector.Length)

    member this.Monoid = monoid
    member this.Values: 'a[] = vector

    override this.Clear () = failwith "Not Implemented"
    override this.Copy () = failwith "Not Implemented"
    override this.Resize a = failwith "Not Implemented"
    override this.GetNNZ () = failwith "Not Implemented"
    override this.GetTuples () = failwith "Not Implemented"
    override this.GetMask(?isComplemented: bool) =
        let isComplemented = defaultArg isComplemented false
        failwith "Not Implemented"
    override this.ToHost () = failwith "Not Implemented"

    override this.Extract (mask: Mask1D option) : OpenCLEvaluation<Vector<'a>> = failwith "Not Implemented"
    override this.Extract (idx: int) : OpenCLEvaluation<Scalar<'a>> = failwith "Not Implemented"
    override this.Assign (mask: Mask1D option, vector: Vector<'a>) : OpenCLEvaluation<unit> = failwith "Not Implemented"
    override this.Assign (idx: int, Scalar (value: 'a)) : OpenCLEvaluation<unit> = failwith "Not Implemented"
    override this.Assign (mask: Mask1D option, Scalar (value: 'a)) : OpenCLEvaluation<unit> = failwith "Not Implemented"

    override this.Vxm a b c = failwith "Not Implemented"
    override this.EWiseAdd a b c = failwith "Not Implemented"
    override this.EWiseMult a b c = failwith "Not Implemented"
    override this.Apply a b = failwith "Not Implemented"
    override this.Prune a b = failwith "Not Implemented"
    override this.Reduce (monoid: Monoid<'a>) = failwith "Not Implemented"
