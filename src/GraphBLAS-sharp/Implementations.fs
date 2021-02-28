namespace GraphBLAS.FSharp

open Brahma.OpenCL
open Brahma.FSharp.OpenCL.Core
open Brahma.FSharp.OpenCL.Extensions
open Helpers
open FSharp.Quotations.Evaluator
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation
open Toolbox

type COOFormat<'a> = {
    RowCount: int
    ColumnCount: int
    Rows: int[]
    Columns: int[]
    Values: 'a[]
}

type CSRFormat<'a> = {
    ColumnCount: int
    RowPointers: int[]
    ColumnIndices: int[]
    Values: 'a[]
}
with
    static member CreateEmpty<'a>() = {
        RowPointers = Array.zeroCreate<int> 0
        ColumnIndices = Array.zeroCreate<int> 0
        Values = Array.zeroCreate<'a> 0
        ColumnCount = 0
    }

type CSRMatrix<'a when 'a : struct and 'a : equality>(csrTuples: CSRFormat<'a>) =
    inherit Matrix<'a>(csrTuples.RowPointers.Length - 1, csrTuples.ColumnCount)

    let rowCount = base.RowCount
    let columnCount = base.ColumnCount

    // let spMV (vector: DenseVector<'a>) (mask: Mask1D) (semiring: Semiring<'a>) : OpenCLEvaluation<Vector<'a>> =
    //     let csrMatrixRowCount = rowCount
    //     let csrMatrixColumnCount = columnCount
    //     let vectorLength = vector.Size
    //     if csrMatrixColumnCount <> vectorLength then
    //         invalidArg
    //             "vector"
    //             (sprintf "Argument has invalid dimension. Need %i, but given %i" csrMatrixColumnCount vectorLength)

    //     let (BinaryOp plus) = semiring.PlusMonoid.Append
    //     let (BinaryOp mult) = semiring.Times

    //     let resultVector = Array.zeroCreate<'a> csrMatrixRowCount
    //     let command =
    //         <@
    //             fun (ndRange: _1D)
    //                 (resultBuffer: 'a[])
    //                 (csrValuesBuffer: 'a[])
    //                 (csrColumnsBuffer: int[])
    //                 (csrRowPointersBuffer: int[])
    //                 (vectorBuffer: 'a[]) ->

    //                 let i = ndRange.GlobalID0
    //                 let mutable localResultBuffer = resultBuffer.[i]
    //                 for k in csrRowPointersBuffer.[i] .. csrRowPointersBuffer.[i + 1] - 1 do
    //                     localResultBuffer <- (%plus) localResultBuffer
    //                         ((%mult) csrValuesBuffer.[k] vectorBuffer.[csrColumnsBuffer.[k]])
    //                 resultBuffer.[i] <- localResultBuffer
    //         @>

    //     let ndRange = _1D(csrMatrixRowCount)
    //     let binder = fun kernelPrepare ->
    //         kernelPrepare
    //             ndRange
    //             resultVector
    //             csrTuples.Values
    //             csrTuples.ColumnIndices
    //             csrTuples.RowPointers
    //             vector.Values

    //     opencl {
    //         do! RunCommand command binder
    //         return upcast DenseVector(resultVector, semiring.PlusMonoid)
    //     }

    // Not Implemented
    new(rows: int[], columns: int[], values: 'a[]) = CSRMatrix(CSRFormat.CreateEmpty())
    new(pathToMatrix: string) = CSRMatrix(CSRFormat.CreateEmpty())

    member this.Values = csrTuples.Values
    member this.Columns = csrTuples.ColumnIndices
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

and COOMatrix<'a when 'a : struct and 'a : equality>(rowCount: int, columnCount: int, rows: int[], columns: int[], values: 'a[]) =
    inherit Matrix<'a>(rowCount, columnCount)

    let mutable rows, columns, values = rows, columns, values

    new(array: 'a[,], isZero: 'a -> bool) =
        let (rows, cols, vals) =
            array
            |> Seq.cast<'a>
            |> Seq.mapi (fun idx v -> (idx / Array2D.length2 array, idx % Array2D.length2 array, v))
            |> Seq.filter (fun (i, j, v) -> not <| isZero v)
            |> Array.ofSeq
            |> Array.unzip3

        COOMatrix(Array2D.length1 array, Array2D.length2 array, rows, cols, vals)

    override this.ToString() =
        [
            sprintf "COO Matrix %ix%i \n" rowCount columnCount
            sprintf "RowIndices: %A \n" rows
            sprintf "ColumnIndices: %A \n" columns
            sprintf "Values: %A \n" values
        ]
        |> String.concat ""

    member this.Rows with get() = rows
    member this.Columns with get() = columns
    member this.Values with get() = values
    member this.Elements with get() = (rows, columns, values) |||> Array.zip3

    override this.Clear () = failwith "Not Implemented"
    override this.Copy () = failwith "Not Implemented"
    override this.Resize a b = failwith "Not Implemented"
    override this.GetNNZ () = failwith "Not Implemented"

    override this.GetTuples() = opencl {
        return {
            RowIndices = this.Rows
            ColumnIndices = this.Columns
            Values = this.Values
        }
    }

    override this.GetMask(?isComplemented: bool) =
        let isComplemented = defaultArg isComplemented false
        failwith "Not Implemented"

    override this.ToHost() = opencl {
        let! _ = ToHost this.Rows
        let! _ = ToHost this.Columns
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

    member internal this.EWiseAddCOO
        (matrix: COOMatrix<'a>)
        (mask: Mask2D option)
        (semiring: Semiring<'a>) : OpenCLEvaluation<Matrix<'a>> =

        let workGroupSize = Toolbox.workGroupSize
        let (BinaryOp append) = semiring.PlusMonoid.Append
        let zero = semiring.PlusMonoid.Zero

        //It is useful to consider that the first array is longer than the second one
        let firstRows, firstColumns, firstValues, secondRows, secondColumns, secondValues, plus =
            if this.Rows.Length > matrix.Rows.Length then
                this.Rows, this.Columns, this.Values, matrix.Rows, matrix.Columns, matrix.Values, append
            else
                matrix.Rows, matrix.Columns, matrix.Values, this.Rows, this.Columns, this.Values, <@ fun x y -> (%append) y x @>

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
                    let ndRange = _1D(workSize allRows.Length, workGroupSize)
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
                    let ndRange = _1D(workSize (allRows.Length - 1), workGroupSize)
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
                let! prefixSumArray = Toolbox.prefixSum2 auxiliaryArray
                let binder kernelP =
                    let ndRange = _1D(workSize auxiliaryArray.Length, workGroupSize)
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

            return upcast COOMatrix<'a>(this.RowCount, this.ColumnCount, resultRows, resultColumns, resultValues)
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
