namespace GraphBLAS.FSharp

open Brahma.OpenCL
open Brahma.FSharp.OpenCL.Core
open Brahma.FSharp.OpenCL.Extensions
open GlobalContext
open Helpers
open FSharp.Quotations.Evaluator
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation

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

    let spMV (vector: Vector<'a>) (mask: Mask1D) (semiring: Semiring<'a>) : Vector<'a> =
        let csrMatrixRowCount = rowCount
        let csrMatrixColumnCount = columnCount
        let vectorLength = vector.Length
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
                vector.AsArray

        let eval = opencl {
            do! RunCommand command binder
            return! ToHost resultVector
        }

        upcast DenseVector(oclContext.RunSync eval, semiring.PlusMonoid)

    member this.Values = csrTuples.Values
    member this.Columns = csrTuples.Columns
    member this.RowPointers = csrTuples.RowPointers

    override this.Mask = failwith "Not implemented"
    override this.Complemented = failwith "Not implemented"

    override this.Item
        with get (mask: Mask2D option) : Matrix<'a> = failwith "Not Implemented"
        and set (mask: Mask2D option) (value: Matrix<'a>) = failwith "Not Implemented"
    override this.Item
        with get (vectorMask: Mask1D option, colIdx: int) : Vector<'a> = failwith "Not Implemented"
        and set (vectorMask: Mask1D option, colIdx: int) (value: Vector<'a>) = failwith "Not Implemented"
    override this.Item
        with get (rowIdx: int, vectorMask: Mask1D option) : Vector<'a> = failwith "Not Implemented"
        and set (rowIdx: int, vectorMask: Mask1D option) (value: Vector<'a>) = failwith "Not Implemented"
    override this.Item
        with get (rowIdx: int, colIdx: int) : Scalar<'a> = failwith "Not Implemented"
        and set (rowIdx: int, colIdx: int) (value: Scalar<'a>) = failwith "Not Implemented"
    override this.Fill
        with set (mask: Mask2D option) (value: Scalar<'a>) = failwith "Not Implemented"
    override this.Fill
        with set (vectorMask: Mask1D option, colIdx: int) (value: Scalar<'a>) = failwith "Not Implemented"
    override this.Fill
        with set (rowIdx: int, vectorMask: Mask1D option) (value: Scalar<'a>) = failwith "Not Implemented"

    override this.Mxm a b c = failwith "Not Implemented"
    override this.Mxv a b c = failwith "Not Implemented"
    override this.EWiseAdd a b c = failwith "Not Implemented"
    override this.EWiseMult a b c = failwith "Not Implemented"
    override this.Apply a b  = failwith "Not Implemented"
    override this.ReduceIn a b = failwith "Not Implemented"
    override this.ReduceOut a b = failwith "Not Implemented"
    override this.Reduce a = failwith "Not Implemented"
    override this.T = failwith "Not Implemented"

and COOMatrix<'a when 'a : struct and 'a : equality>(elements: (int * int * 'a)[], rowCount: int, columnCount: int) =
    inherit Matrix<'a>(rowCount, columnCount)

    let mutable rows, columns, values = elements |> Array.unzip3
    member this.Rows with get() = rows
    member this.Columns with get() = columns
    member this.Values with get() = values
    member this.Elements with get() = elements

    override this.Mask = Some <| Mask2D(Array.zip this.Rows this.Columns, this.RowCount, this.ColumnCount, false)
    override this.Complemented = Some <| Mask2D(Array.zip this.Rows this.Columns, this.RowCount, this.ColumnCount, true)

    member private this.SetCOOMatrix
        (mask: Mask2D)
        (matrix: COOMatrix<'a>) =
        let newElements = matrix.Elements |> Array.filter (fun (i, j, _) -> mask.[i, j])
        let elementsToSet, elementsToAppend =
            newElements
            |> Array.partition (
                fun (i, j, _) ->
                this.Elements
                |> Array.exists (fun (u, v, _) -> (u, v) = (i, j)))

        let workflow =
            opencl {
                let command =
                    <@
                        fun (ndRange: _1D)
                            (resultValuesBuffer: 'a[])
                            (resultRowsBuffer: int[])
                            (resultColumnsBuffer: int[])
                            (valuesToSetBuffer: 'a[])
                            (rowsToSetBuffer: int[])
                            (columnsToSetBuffer: int[]) ->

                            let i = ndRange.GlobalID0
                            let value, row, column = valuesToSetBuffer.[i], rowsToSetBuffer.[i], columnsToSetBuffer.[i]
                            for j in 0 .. resultValuesBuffer.Length - 1 do
                                if resultRowsBuffer.[j] = row && resultColumnsBuffer.[j] = column then
                                    resultValuesBuffer.[j] <- value
                    @>

                let resultValues = this.Values
                let resultRows = this.Rows
                let resultColumns = this.Columns
                let rowsToSet, columnsToSet, valuesToSet = elementsToSet |> Array.unzip3
                let binder kernelP =
                    let ndRange = _1D(elementsToSet.Length)
                    kernelP
                        ndRange
                        resultValues
                        resultRows
                        resultColumns
                        valuesToSet
                        rowsToSet
                        columnsToSet

                do! RunCommand command binder
                return! ToHost resultValues
            }

        let renewedValues = oclContext.RunSync workflow

        let resultRows, resultColumns, resultValues =
            (rows, columns, renewedValues)
            |||> Array.zip3
            |> Array.append elementsToAppend
            |> Array.unzip3

        rows <- resultRows
        columns <- resultColumns
        values <- resultValues

    override this.Item
        with get (matrixMask: Mask2D option) : Matrix<'a> =
            let resultElements =
                match matrixMask with
                | Some mask ->
                    if (mask.RowCount, mask.ColumnCount) <> (this.RowCount, this.ColumnCount) then
                        invalidArg
                            "mask"
                            (sprintf "Argument has invalid dimension. Need %A, but given %A" (this.RowCount, this.ColumnCount) (mask.RowCount, mask.ColumnCount))

                    this.Elements |> Array.filter (fun (i, j, _) -> mask.[i, j])
                | _ -> this.Elements

            upcast COOMatrix<'a>(resultElements, this.RowCount, this.ColumnCount)

        and set (matrixMask: Mask2D option) (matrix: Matrix<'a>) =
            let mask =
                match matrixMask with
                | Some m ->
                    if (m.RowCount, m.ColumnCount) <> (this.RowCount, this.ColumnCount) then
                        invalidArg
                            "mask"
                            (sprintf "Argument has invalid dimension. Need %A, but given %A" (this.RowCount, this.ColumnCount) (m.RowCount, m.ColumnCount))
                    m
                | _ -> Mask2D(Array.empty, this.RowCount, this.ColumnCount, true) // Empty complemented mask is equal to none

            match matrix with
            | :? COOMatrix<'a> -> this.SetCOOMatrix mask (downcast matrix)
            | _ -> failwith "Not Implemented"

    member private this.SetVerticalSparseVector
        (mask: Mask1D)
        (colIdx: int)
        (vector: SparseVector<'a>) =

        let newElements = vector.Elements |> Array.filter (fun (i, _) -> mask.[i])
        let elementsToSet, elementsToAppend =
            newElements
            |> Array.partition (
                fun (i, _) ->
                this.Elements
                |> Array.exists (fun (u, v, _) -> (u, v) = (i, colIdx)))

        let workflow =
            opencl {
                let command =
                    <@
                        fun (ndRange: _1D)
                            (resultValuesBuffer: 'a[])
                            (resultRowsBuffer: int[])
                            (resultColumnsBuffer: int[])
                            (valuesToSetBuffer: 'a[])
                            (rowsToSetBuffer: int[]) ->

                            let i = ndRange.GlobalID0
                            let value, row = valuesToSetBuffer.[i], rowsToSetBuffer.[i]
                            for j in 0 .. resultValuesBuffer.Length - 1 do
                                if resultRowsBuffer.[j] = row && resultColumnsBuffer.[j] = colIdx then
                                    resultValuesBuffer.[j] <- value
                    @>

                let resultValues = this.Values
                let resultRows = this.Rows
                let resultColumns = this.Columns
                let rowsToSet, valuesToSet = elementsToSet |> Array.unzip
                let binder kernelP =
                    let ndRange = _1D(elementsToSet.Length)
                    kernelP
                        ndRange
                        resultValues
                        resultRows
                        resultColumns
                        valuesToSet
                        rowsToSet

                do! RunCommand command binder
                return! ToHost resultValues
            }

        let renewedValues = oclContext.RunSync workflow
        let renewedElementsToAppend = elementsToAppend |> Array.map (fun (i, a) -> (i, colIdx, a))

        let resultRows, resultColumns, resultValues =
            (rows, columns, renewedValues)
            |||> Array.zip3
            |> Array.append renewedElementsToAppend
            |> Array.unzip3

        rows <- resultRows
        columns <- resultColumns
        values <- resultValues

    override this.Item
        with get (vectorMask: Mask1D option, colIdx: int) : Vector<'a> =
            let resultLength = this.RowCount

            let resultElements =
                this.Elements
                |> Array.filter (second >> (=) colIdx)
                |> Array.map (fun (i, _, a) -> (i, a))

            match vectorMask with
            | Some mask ->
                if mask.Length <> resultLength then
                    invalidArg
                        "mask"
                        (sprintf "Argument has invalid dimension. Need %i, but given %i" resultLength mask.Length)
                upcast SparseVector(resultElements |> Array.filter (fun (i, _) -> mask.[i]), resultLength)
            | _ -> upcast SparseVector(resultElements, resultLength)

        and set (vectorMask: Mask1D option, colIdx: int) (vector: Vector<'a>) =
            let mask =
                match vectorMask with
                | Some m ->
                    if m.Length <> this.RowCount then
                        invalidArg
                            "mask"
                            (sprintf "Argument has invalid dimension. Need %i, but given %i" this.RowCount m.Length)
                    m
                | _ -> Mask1D(Array.empty, this.RowCount, true) // Empty complemented mask is equal to none

            match vector with
            | :? SparseVector<'a> -> this.SetVerticalSparseVector mask colIdx (downcast vector)
            | _ -> failwith "Not Implemented"

    member private this.SetHorizontalSparseVector
        (mask: Mask1D)
        (rowIdx: int)
        (vector: SparseVector<'a>) =

        let newElements = vector.Elements |> Array.filter (fun (i, _) -> mask.[i])
        let elementsToSet, elementsToAppend =
            newElements
            |> Array.partition (
                fun (j, _) ->
                this.Elements
                |> Array.exists (fun (u, v, _) -> (u, v) = (rowIdx, j)))

        let workflow =
            opencl {
                let command =
                    <@
                        fun (ndRange: _1D)
                            (resultValuesBuffer: 'a[])
                            (resultRowsBuffer: int[])
                            (resultColumnsBuffer: int[])
                            (valuesToSetBuffer: 'a[])
                            (columnsToSetBuffer: int[]) ->

                            let i = ndRange.GlobalID0
                            let value, column = valuesToSetBuffer.[i], columnsToSetBuffer.[i]
                            for j in 0 .. resultValuesBuffer.Length - 1 do
                                if resultRowsBuffer.[j] = rowIdx && resultColumnsBuffer.[j] = column then
                                    resultValuesBuffer.[j] <- value
                    @>

                let resultValues = this.Values
                let resultRows = this.Rows
                let resultColumns = this.Columns
                let columnsToSet, valuesToSet = elementsToSet |> Array.unzip
                let binder kernelP =
                    let ndRange = _1D(elementsToSet.Length)
                    kernelP
                        ndRange
                        resultValues
                        resultRows
                        resultColumns
                        valuesToSet
                        columnsToSet

                do! RunCommand command binder
                return! ToHost resultValues
            }

        let renewedValues = oclContext.RunSync workflow
        let renewedElementsToAppend = elementsToAppend |> Array.map (fun (j, a) -> (rowIdx, j, a))

        let resultRows, resultColumns, resultValues =
            (rows, columns, renewedValues)
            |||> Array.zip3
            |> Array.append renewedElementsToAppend
            |> Array.unzip3

        rows <- resultRows
        columns <- resultColumns
        values <- resultValues

    override this.Item
        with get (rowIdx: int, vectorMask: Mask1D option) : Vector<'a> =
            let resultLength = this.ColumnCount

            let resultElements =
                this.Elements
                |> Array.filter (first >> (=) rowIdx)
                |> Array.map (fun (_, j, a) -> (j, a))

            match vectorMask with
            | Some mask ->
                if mask.Length <> resultLength then
                    invalidArg
                        "mask"
                        (sprintf "Argument has invalid dimension. Need %i, but given %i" resultLength mask.Length)
                upcast SparseVector(resultElements |> Array.filter (fun (i, _) -> mask.[i]), resultLength)
            | _ -> upcast SparseVector(resultElements, resultLength)

        and set (rowIdx: int, vectorMask: Mask1D option) (vector: Vector<'a>) =
            let mask =
                match vectorMask with
                | Some m ->
                    if m.Length <> this.RowCount then
                        invalidArg
                            "mask"
                            (sprintf "Argument has invalid dimension. Need %i, but given %i" this.RowCount m.Length)
                    m
                | _ -> Mask1D(Array.empty, this.RowCount, true) // Empty complemented mask is equal to none

            match vector with
            | :? SparseVector<'a> -> this.SetHorizontalSparseVector mask rowIdx (downcast vector)
            | _ -> failwith "Not Implemented"

    override this.Item
        with get (rowIdx: int, colIdx: int) : Scalar<'a> =
            let value = this.Elements |> Array.tryFind (fun (i, j, _) -> (i, j) = (rowIdx, colIdx))
            match value with
            | Some (_, _, a) -> Scalar a
            | _ -> failwith "Attempt to address to zero value" // Needs correction

        and set (rowIdx: int, colIdx: int) (Scalar (value: 'a)) =
            let mutable isFound = false
            let mutable i = 0
            while not isFound && i < this.Elements.Length do
                let row, column, _ = this.Elements.[i]
                if row = rowIdx && column = colIdx then
                    isFound <- true
                    values.[i] <- value
                i <- i + 1

            if not isFound then
                let resultRows, resultCOlumns, resultValues =
                    (rows, columns, values)
                    |||> Array.zip3
                    |> Array.append [|rowIdx, colIdx, value|]
                    |> Array.unzip3

                rows <- resultRows
                columns <- resultCOlumns
                values <- resultValues

    override this.Fill
        with set (matrixMask: Mask2D option) (Scalar (value: 'a)) =
            match matrixMask with
            | None ->
                let resultRows, resultCOlumns, resultValues =
                    [| for i in 0 .. this.RowCount - 1 do
                        for j in 0 .. this.ColumnCount - 1 do
                            yield (i, j, value) |]
                    |> Array.unzip3

                rows <- resultRows
                columns <- resultCOlumns
                values <- resultValues
            | Some mask ->
                if (mask.RowCount, mask.ColumnCount) <> (this.RowCount, this.ColumnCount) then
                    invalidArg
                        "mask"
                        (sprintf "Argument has invalid dimension. Need %A, but given %A" (this.RowCount, this.ColumnCount) (mask.RowCount, mask.ColumnCount))
                let newIndices = Array.zip mask.Rows mask.Columns
                let indicesToSet, indicesToAppend =
                    newIndices
                    |> Array.partition (
                        fun (i, j) ->
                        this.Elements
                        |> Array.exists (fun (u, v, _) -> (u, v) = (i, j)))

                let workflow =
                    opencl {
                        let command =
                            <@
                                fun (ndRange: _1D)
                                    (resultValuesBuffer: 'a[])
                                    (resultRowsBuffer: int[])
                                    (resultColumnsBuffer: int[])
                                    (rowsToSetBuffer: int[])
                                    (columnsToSetBuffer: int[]) ->

                                    let i = ndRange.GlobalID0
                                    let row, column = rowsToSetBuffer.[i], columnsToSetBuffer.[i]
                                    for j in 0 .. resultValuesBuffer.Length - 1 do
                                        if resultRowsBuffer.[j] = row && resultColumnsBuffer.[j] = column then
                                            resultValuesBuffer.[j] <- value
                            @>

                        let resultValues = this.Values
                        let resultRows = this.Rows
                        let resultColumns = this.Columns
                        let rowsToSet, columnsToSet = indicesToSet |> Array.unzip
                        let binder kernelP =
                            let ndRange = _1D(indicesToSet.Length)
                            kernelP
                                ndRange
                                resultValues
                                resultRows
                                resultColumns
                                rowsToSet
                                columnsToSet

                        do! RunCommand command binder
                        return! ToHost resultValues
                    }

                let renewedValues = oclContext.RunSync workflow
                let elementsToAppend = indicesToAppend |> Array.map (fun (i, j) -> (i, j, value))

                let resultRows, resultColumns, resultValues =
                    (rows, columns, renewedValues)
                    |||> Array.zip3
                    |> Array.append elementsToAppend
                    |> Array.unzip3

                rows <- resultRows
                columns <- resultColumns
                values <- resultValues

    override this.Fill
        with set (vectorMask: Mask1D option, colIdx: int) (Scalar (value: 'a)) =
            let mask =
                match vectorMask with
                | Some m ->
                    if m.Length <> this.RowCount then
                        invalidArg
                            "mask"
                            (sprintf "Argument has invalid dimension. Need %i, but given %i" this.RowCount m.Length)
                    m
                | _ -> Mask1D(Array.empty, this.RowCount, true) // Empty complemented mask is equal to none

            let indicesToSet, indicesToAppend =
                mask.Indices
                |> Array.partition (
                    fun i ->
                    this.Elements
                    |> Array.exists (fun (u, v, _) -> (u, v) = (i, colIdx)))

            let workflow =
                opencl {
                    let command =
                        <@
                            fun (ndRange: _1D)
                                (resultValuesBuffer: 'a[])
                                (resultRowsBuffer: int[])
                                (resultColumnsBuffer: int[])
                                (rowsToSetBuffer: int[]) ->

                                let i = ndRange.GlobalID0
                                let row = rowsToSetBuffer.[i]
                                for j in 0 .. resultValuesBuffer.Length - 1 do
                                    if resultRowsBuffer.[j] = row && resultColumnsBuffer.[j] = colIdx then
                                        resultValuesBuffer.[j] <- value
                        @>

                    let resultValues = this.Values
                    let resultRows = this.Rows
                    let resultColumns = this.Columns
                    let binder kernelP =
                        let ndRange = _1D(indicesToSet.Length)
                        kernelP
                            ndRange
                            resultValues
                            resultRows
                            resultColumns
                            indicesToSet

                    do! RunCommand command binder
                    return! ToHost resultValues
                }

            let renewedValues = oclContext.RunSync workflow
            let elementsToAppend = indicesToAppend |> Array.map (fun i -> (i, colIdx, value))

            let resultRows, resultColumns, resultValues =
                (rows, columns, renewedValues)
                |||> Array.zip3
                |> Array.append elementsToAppend
                |> Array.unzip3

            rows <- resultRows
            columns <- resultColumns
            values <- resultValues

    override this.Fill
        with set (rowIdx: int, vectorMask: Mask1D option) (Scalar (value: 'a)) =
            let mask =
                match vectorMask with
                | Some m ->
                    if m.Length <> this.ColumnCount then
                        invalidArg
                            "mask"
                            (sprintf "Argument has invalid dimension. Need %i, but given %i" this.ColumnCount m.Length)
                    m
                | _ -> Mask1D(Array.empty, this.RowCount, true) // Empty complemented mask is equal to none

            let indicesToSet, indicesToAppend =
                mask.Indices
                |> Array.partition (
                    fun j ->
                    this.Elements
                    |> Array.exists (fun (u, v, _) -> (u, v) = (rowIdx, j)))

            let workflow =
                opencl {
                    let command =
                        <@
                            fun (ndRange: _1D)
                                (resultValuesBuffer: 'a[])
                                (resultRowsBuffer: int[])
                                (resultColumnsBuffer: int[])
                                (columnsToSetBuffer: int[]) ->

                                let i = ndRange.GlobalID0
                                let column = columnsToSetBuffer.[i]
                                for j in 0 .. resultValuesBuffer.Length - 1 do
                                    if resultRowsBuffer.[j] = rowIdx && resultColumnsBuffer.[j] = column then
                                        resultValuesBuffer.[j] <- value
                        @>

                    let resultValues = this.Values
                    let resultRows = this.Rows
                    let resultColumns = this.Columns
                    let binder kernelP =
                        let ndRange = _1D(indicesToSet.Length)
                        kernelP
                            ndRange
                            resultValues
                            resultRows
                            resultColumns
                            indicesToSet

                    do! RunCommand command binder
                    return! ToHost resultValues
                }

            let renewedValues = oclContext.RunSync workflow
            let elementsToAppend = indicesToAppend |> Array.map (fun j -> (rowIdx, j, value))

            let resultRows, resultColumns, resultValues =
                (rows, columns, renewedValues)
                |||> Array.zip3
                |> Array.append elementsToAppend
                |> Array.unzip3

            rows <- resultRows
            columns <- resultColumns
            values <- resultValues

    member private this.MxCOOm
        (matrix: COOMatrix<'a>)
        (mask: Mask2D)
        (semiring: Semiring<'a>) : Matrix<'a> =

        let (BinaryOp plus) = semiring.PlusMonoid.Append
        let (BinaryOp mult) = semiring.Times
        let zero = semiring.PlusMonoid.Zero

        let resultIndices =
            [| for i in 0 .. this.Elements.Length - 1 do
                let leftRow = this.Rows.[i]
                let leftColumn = this.Columns.[i]
                for j in 0 .. matrix.Elements.Length - 1 do
                    let rightRow = matrix.Rows.[j]
                    let rightColumn = matrix.Columns.[j]
                    if leftColumn = rightRow then yield (leftRow, rightColumn) |]
            |> Array.distinct

        let resultRows, resultColumns = resultIndices |> Array.unzip

        let workflow =
            opencl {
                let command =
                    <@
                        fun (ndRange: _1D)
                            (resultValuesBuffer: 'a[])
                            (leftValuesBuffer: 'a[])
                            (leftRowsBuffer: int[])
                            (leftColumnsBuffer: int[])
                            (rightValuesBuffer: 'a[])
                            (rightRowsBuffer: int[])
                            (rightColumnsBuffer: int[]) ->

                            let i = ndRange.GlobalID0
                            let row = resultRows.[i]
                            let column = resultColumns.[i]

                            if mask.[row, column] then
                                let mutable localResultBuffer = resultValuesBuffer.[i]
                                for j in 0 .. leftValuesBuffer.Length - 1 do
                                    for k in 0 .. rightValuesBuffer.Length - 1 do
                                        if leftRowsBuffer.[j] = row && rightColumnsBuffer.[k] = column
                                            && leftColumnsBuffer.[j] = rightRowsBuffer.[k] then
                                                localResultBuffer <- (%plus) localResultBuffer
                                                    ((%mult) leftValuesBuffer.[j] rightValuesBuffer.[k])

                                resultValuesBuffer.[i] <- localResultBuffer
                    @>

                let resultValues = Array.init resultIndices.Length (fun _ -> zero)
                let leftValues = this.Values
                let leftRows = this.Rows
                let leftColumns = this.Columns
                let rightValues = matrix.Values
                let rightRows = matrix.Rows
                let rightColumns = matrix.Columns
                let binder kernelP =
                    let ndRange = _1D(resultValues.Length)
                    kernelP
                        ndRange
                        resultValues
                        leftValues
                        leftRows
                        leftColumns
                        rightValues
                        rightRows
                        rightColumns

                do! RunCommand command binder
                return! ToHost resultValues
            }

        let resultValues = oclContext.RunSync workflow

        let resultElements =
            (resultRows, resultColumns, resultValues)
            |||> Array.zip3
            |> Array.filter (third >> (<>) zero)

        upcast COOMatrix<'a>(resultElements, this.RowCount, matrix.ColumnCount)

    override this.Mxm
        (matrix: Matrix<'a>)
        (matrixMask: Mask2D option)
        (semiring: Semiring<'a>) =

        if this.ColumnCount <> matrix.RowCount then
            invalidArg
                "matrix"
                (sprintf "Argument has invalid dimension. Need %i, but given %i" this.ColumnCount matrix.RowCount)

        let mask =
            match matrixMask with
            | Some m ->
                if (m.RowCount, m.ColumnCount) <> (this.RowCount, matrix.ColumnCount) then
                    invalidArg
                        "mask"
                        (sprintf "Argument has invalid dimension. Need %A, but given %A" (this.RowCount, matrix.ColumnCount) (m.RowCount, m.ColumnCount))
                m
            | _ -> Mask2D(Array.empty, this.RowCount, matrix.ColumnCount, true) // Empty complemented mask is equal to none

        match matrix with
        | :? COOMatrix<'a> -> this.MxCOOm (downcast matrix) mask semiring
        | _ -> failwith "Not Implemented"

    member private this.MxCOOv
        (vector: SparseVector<'a>)
        (mask: Mask1D)
        (semiring: Semiring<'a>) : Vector<'a> =

        let matrixColumnCount = this.ColumnCount

        let (BinaryOp plus) = semiring.PlusMonoid.Append
        let (BinaryOp mult) = semiring.Times
        let zero = semiring.PlusMonoid.Zero

        let resultIndices = Array.init matrixColumnCount id

        let workflow =
            opencl {
                let command =
                    <@
                        fun (ndRange: _1D)
                            (resultValuesBuffer: 'a[])
                            (matrixValuesBuffer: 'a[])
                            (matrixRowsBuffer: int[])
                            (matrixColumnsBuffer: int[])
                            (vectorValuesBuffer: 'a[])
                            (vectorIndicesBuffer: int[]) ->

                            let i = ndRange.GlobalID0
                            if mask.[i] then
                                let mutable localResultBuffer = resultValuesBuffer.[i]
                                for j in 0 .. vectorValuesBuffer.Length - 1 do
                                    for k in 0 .. matrixValuesBuffer.Length - 1 do
                                        if vectorIndicesBuffer.[j] = matrixRowsBuffer.[k] && matrixColumnsBuffer.[k] = i then
                                            localResultBuffer <- (%plus) localResultBuffer
                                                ((%mult) matrixValuesBuffer.[k] vectorValuesBuffer.[j])
                                resultValuesBuffer.[i] <- localResultBuffer
                    @>

                let vectorValues = vector.Values
                let vectorIndices = vector.Indices
                let matrixValues = this.Values
                let matrixRows = this.Rows
                let matrixColumns = this.Columns
                let resultValues = Array.init matrixColumnCount (fun _ -> zero)
                let binder kernelP =
                    let ndRange = _1D(matrixColumnCount)
                    kernelP
                        ndRange
                        resultValues
                        matrixValues
                        matrixRows
                        matrixColumns
                        vectorValues
                        vectorIndices

                do! RunCommand command binder
                return! ToHost resultValues
            }

        let resultValues = oclContext.RunSync workflow

        let resultElements =
            (resultIndices, resultValues)
            ||> Array.zip
            |> Array.filter (snd >> (<>) zero)

        upcast SparseVector(resultElements, matrixColumnCount)

    override this.Mxv
        (vector: Vector<'a>)
        (vectorMask: Mask1D option)
        (semiring: Semiring<'a>) =

        if this.ColumnCount <> vector.Length then
            invalidArg
                "matrix"
                (sprintf "Argument has invalid dimension. Need %i, but given %i" vector.Length this.ColumnCount)

        let mask =
            match vectorMask with
            | Some m ->
                if m.Length <> vector.Length then
                    invalidArg
                        "mask"
                        (sprintf "Argument has invalid dimension. Need %i, but given %i" vector.Length m.Length)
                m
            | _ -> Mask1D(Array.empty, vector.Length, true) // Empty complemented mask is equal to none

        match vector with
        | :? SparseVector<'a> -> this.MxCOOv (downcast vector) mask semiring
        | _ -> failwith "Not Implemented"

    member private this.EWiseAddCOO
        (matrix: COOMatrix<'a>)
        (mask: Mask2D)
        (semiring: Semiring<'a>) : OpenCLEvaluation<Matrix<'a>> =
        let (BinaryOp plus) = semiring.PlusMonoid.Append
        let zero = semiring.PlusMonoid.Zero

        let leftIndices = this.Elements |> Array.map (fun (i, j, _) -> (i, j))
        let rightIndices = matrix.Elements |> Array.map (fun (i, j, _) -> (i, j))

        let commonIndices =
            leftIndices
            |> Array.filter (fun i ->
                rightIndices
                |> Array.contains i)

        let leftCommonElements, leftUniqueElements =
            this.Elements
            |> Array.partition (fun (i, j, _) ->
                commonIndices
                |> Array.contains (i, j))
        let rightCommonElements, rightUniqueElements =
            matrix.Elements
            |> Array.partition (fun (i, j, _) ->
                commonIndices
                |> Array.contains (i, j))

        opencl {
            let command =
                <@
                    fun (ndRange: _2D)
                        (resultValuesBuffer: 'a[])
                        (leftValuesBuffer: 'a[])
                        (leftRowsBuffer: int[])
                        (leftColumnsBuffer: int[])
                        (rightValuesBuffer: 'a[])
                        (rightRowsBuffer: int[])
                        (rightColumnsBuffer: int[]) ->

                        let i, j = ndRange.GlobalID0, ndRange.GlobalID1
                        let row = leftRowsBuffer.[i]
                        let column = leftColumnsBuffer.[i]
                        if mask.[row, column] && rightRowsBuffer.[j] = row && rightColumnsBuffer.[j] = column then
                            resultValuesBuffer.[i] <- (%plus) leftValuesBuffer.[i] rightValuesBuffer.[j]
                @>

            let leftCommonRows, leftCommonColumns, leftCommonValues = leftCommonElements |> Array.unzip3
            let rightCommonRows, rightCommonColumns, rightCommonValues = rightCommonElements |> Array.unzip3

            let resultCommonValues = Array.init leftCommonElements.Length (fun _ -> zero)

            let binder kernelP =
                let ndRange = _2D(leftCommonElements.Length, rightCommonElements.Length)
                kernelP
                    ndRange
                    resultCommonValues
                    leftCommonValues
                    leftCommonRows
                    leftCommonColumns
                    rightCommonValues
                    rightCommonRows
                    rightCommonColumns

            do! RunCommand command binder
            //return! ToHost resultCommonValues
            let resultCommonRows, resultCommonColumns, _ = leftCommonElements |> Array.unzip3
            let uniqueIndices =
                (leftUniqueElements, rightUniqueElements)
                ||> Array.append
                |> Array.filter (fun (i, j, _) -> mask.[i, j])
            let resultElements =
                (resultCommonRows, resultCommonColumns, resultCommonValues)
                |||> Array.zip3
                |> Array.append uniqueIndices
                |> Array.filter (third >> (<>) zero)

            return upcast COOMatrix<'a>(resultElements, this.RowCount, this.ColumnCount)
        }

        //let resultCommonValues = oclContext.RunSync workflow

        // let resultCommonRows, resultCommonColumns, _ = leftCommonElements |> Array.unzip3
        // let uniqueIndices =
        //     (leftUniqueElements, rightUniqueElements)
        //     ||> Array.append
        //     |> Array.filter (fun (i, j, _) -> mask.[i, j])
        // let resultElements =
        //     (resultCommonRows, resultCommonColumns, resultCommonValues)
        //     |||> Array.zip3
        //     |> Array.append uniqueIndices
        //     |> Array.filter (third >> (<>) zero)

        //upcast COOMatrix<'a>(resultElements, this.RowCount, this.ColumnCount)

    override this.EWiseAdd
        (matrix: Matrix<'a>)
        (matrixMask: Mask2D option)
        (semiring: Semiring<'a>) =

        if (this.RowCount, this.ColumnCount) <> (matrix.RowCount, matrix.ColumnCount) then
            invalidArg
                "matrix"
                (sprintf "Argument has invalid dimension. Need %A, but given %A" (this.RowCount, this.ColumnCount) (matrix.RowCount, matrix.ColumnCount))

        let mask =
            match matrixMask with
            | Some m ->
                if (m.RowCount, m.ColumnCount) <> (this.RowCount, this.ColumnCount) then
                    invalidArg
                        "mask"
                        (sprintf "Argument has invalid dimension. Need %A, but given %A" (this.RowCount, this.ColumnCount) (m.RowCount, m.ColumnCount))
                m
            | _ -> Mask2D(Array.empty, this.RowCount, this.ColumnCount, true) // Empty complemented mask is equal to none

        match matrix with
        | :? COOMatrix<'a> -> this.EWiseAddCOO (downcast matrix) mask semiring
        | _ -> failwith "Not Implemented"

    member private this.EWiseMultCOO
        (matrix: COOMatrix<'a>)
        (mask: Mask2D)
        (semiring: Semiring<'a>) : Matrix<'a> =
        let (BinaryOp mult) = semiring.Times
        let zero = semiring.PlusMonoid.Zero

        let leftIndices = this.Elements |> Array.map (fun (i, j, _) -> (i, j))
        let rightIndices = matrix.Elements |> Array.map (fun (i, j, _) -> (i, j))

        let commonIndices =
            leftIndices
            |> Array.filter (fun i ->
                rightIndices
                |> Array.contains i)

        let leftCommonElements =
            this.Elements
            |> Array.filter (fun (i, j, _) ->
                commonIndices
                |> Array.contains (i, j))
        let rightCommonElements =
            matrix.Elements
            |> Array.filter (fun (i, j, _) ->
                commonIndices
                |> Array.contains (i, j))

        let workflow =
            opencl {
                let command =
                    <@
                        fun (ndRange: _2D)
                            (resultValuesBuffer: 'a[])
                            (leftValuesBuffer: 'a[])
                            (leftRowsBuffer: int[])
                            (leftColumnsBuffer: int[])
                            (rightValuesBuffer: 'a[])
                            (rightRowsBuffer: int[])
                            (rightColumnsBuffer: int[]) ->

                            let i, j = ndRange.GlobalID0, ndRange.GlobalID1
                            let row = leftRowsBuffer.[i]
                            let column = leftColumnsBuffer.[i]
                            if mask.[row, column] && rightRowsBuffer.[j] = row && rightColumnsBuffer.[j] = column then
                                resultValuesBuffer.[i] <- (%mult) leftValuesBuffer.[i] rightValuesBuffer.[j]
                    @>

                let leftCommonRows, leftCommonColumns, leftCommonValues = leftCommonElements |> Array.unzip3
                let rightCommonRows, rightCommonColumns, rightCommonValues = rightCommonElements |> Array.unzip3

                let resultCommonValues = Array.init leftCommonElements.Length (fun _ -> zero)

                let binder kernelP =
                    let ndRange = _2D(leftCommonElements.Length, rightCommonElements.Length)
                    kernelP
                        ndRange
                        resultCommonValues
                        leftCommonValues
                        leftCommonRows
                        leftCommonColumns
                        rightCommonValues
                        rightCommonRows
                        rightCommonColumns

                do! RunCommand command binder
                return! ToHost resultCommonValues
            }

        let resultCommonValues = oclContext.RunSync workflow

        let resultCommonRows, resultCommonColumns, _ = leftCommonElements |> Array.unzip3
        let resultTriples =
            (resultCommonRows, resultCommonColumns, resultCommonValues)
            |||> Array.zip3
            |> Array.filter (third >> (<>) zero)

        upcast COOMatrix<'a>(resultTriples, this.RowCount, this.ColumnCount)

    override this.EWiseMult
        (matrix: Matrix<'a>)
        (matrixMask: Mask2D option)
        (semiring: Semiring<'a>) =

        if (this.RowCount, this.ColumnCount) <> (matrix.RowCount, matrix.ColumnCount) then
            invalidArg
                "matrix"
                (sprintf "Argument has invalid dimension. Need %A, but given %A" (this.RowCount, this.ColumnCount) (matrix.RowCount, matrix.ColumnCount))

        let mask =
            match matrixMask with
            | Some m ->
                if (m.RowCount, m.ColumnCount) <> (this.RowCount, this.ColumnCount) then
                    invalidArg
                        "mask"
                        (sprintf "Argument has invalid dimension. Need %A, but given %A" (this.RowCount, this.ColumnCount) (m.RowCount, m.ColumnCount))
                m
            | _ -> Mask2D(Array.empty, this.RowCount, this.ColumnCount, true) // Empty complemented mask is equal to none

        match matrix with
        | :? COOMatrix<'a> -> this.EWiseMultCOO (downcast matrix) mask semiring
        | _ -> failwith "Not Implemented"

    override this.Apply
        (matrixMask: Mask2D option)
        (operator: UnaryOp<'a, 'b>) =
        let mask =
            match matrixMask with
            | Some m ->
                if (m.RowCount, m.ColumnCount) <> (this.RowCount, this.ColumnCount) then
                    invalidArg
                        "mask"
                        (sprintf "Argument has invalid dimension. Need %A, but given %A" (this.RowCount, this.ColumnCount) (m.RowCount, m.ColumnCount))
                m
            | _ -> Mask2D(Array.empty, this.RowCount, this.ColumnCount, true) // Empty complemented mask is equal to none

        let (UnaryOp op) = operator

        let matrixRows, matrixColumns, matrixValues =
            this.Elements
            |> Array.filter (fun (i, j, _) -> mask.[i, j])
            |> Array.unzip3

        let workflow =
            opencl {
                let command =
                    <@
                        fun (ndRange: _1D)
                            (resultValuesBuffer: 'b[])
                            (matrixValuesBuffer: 'a[]) ->

                            let i = ndRange.GlobalID0
                            resultValuesBuffer.[i] <- (%op) matrixValuesBuffer.[i]
                    @>

                let resultValues = Array.zeroCreate matrixValues.Length // No matter what the initial values are

                let binder kernelP =
                    let ndRange = _1D(resultValues.Length)
                    kernelP
                        ndRange
                        resultValues
                        matrixValues

                do! RunCommand command binder
                return! ToHost resultValues
            }

        let resultValues = oclContext.RunSync workflow
        let resultElements = Array.zip3 matrixRows matrixColumns resultValues

        upcast COOMatrix(resultElements, this.RowCount, this.ColumnCount)

    override this.ReduceIn
        (vectorMask: Mask1D option)
        (monoid: Monoid<'a>) =
        failwith "Not Implemented"

    override this.ReduceOut
        (vectorMask: Mask1D option)
        (monoid: Monoid<'a>) =
        failwith "Not Implemented"

    override this.Reduce
        (monoid: Monoid<'a>) =
        failwith "Not implemented"

    override this.T = upcast COOMatrix<'a>(Array.zip3 columns rows values, this.ColumnCount, this.RowCount)

and SparseVector<'a when 'a : struct and 'a : equality>(size: int, listOfNonzeroes: (int * 'a) list) =
    inherit Vector<'a>(size)

    let mutable indices, values =
        listOfNonzeroes
        |> Array.ofList
        |> Array.unzip
    member this.Values with get() = values
    member this.Indices with get() = indices
    member this.Elements with get() = (indices, values) ||> Array.zip

    new(elements: (int * 'a)[], length: int) = SparseVector(length, Array.toList elements)

    member this.AsList: (int * 'a) list = listOfNonzeroes

    override this.AsArray = failwith "Not Implemented"
    override this.Clear() = failwith "Not Implemented"

    override this.Mask = Some <| Mask1D(indices, this.Length, false)
    override this.Complemented = Some <| Mask1D(indices, this.Length, true)

    member private this.SetSparseVector
        (mask: Mask1D)
        (vector: SparseVector<'a>) =
        let newElements = vector.Elements |> Array.filter (fun (i, _) -> mask.[i])
        let elementsToSet, elementsToAppend =
            newElements
            |> Array.partition (
                fun (i, _) ->
                this.Elements
                |> Array.exists (fst >> (=) i))

        let workflow =
            opencl {
                let command =
                    <@
                        fun (ndRange: _1D)
                            (resultValuesBuffer: 'a[])
                            (resultIndicesBuffer: int[])
                            (valuesToSetBuffer: 'a[])
                            (indicesToSetBuffer: int[]) ->

                            let i = ndRange.GlobalID0
                            let value, index = valuesToSetBuffer.[i], indicesToSetBuffer.[i]
                            for j in 0 .. resultValuesBuffer.Length - 1 do
                                if resultIndicesBuffer.[j] = index then
                                    resultValuesBuffer.[j] <- value
                    @>

                let resultValues = this.Values
                let resultIndices = this.Indices
                let indicesToSet, valuesToSet = elementsToSet |> Array.unzip
                let binder kernelP =
                    let ndRange = _1D(elementsToSet.Length)
                    kernelP
                        ndRange
                        resultValues
                        resultIndices
                        valuesToSet
                        indicesToSet

                do! RunCommand command binder
                return! ToHost resultValues
            }

        let renewedValues = oclContext.RunSync workflow

        let resultIndices, resultValues =
            (indices, renewedValues)
            ||> Array.zip
            |> Array.append elementsToAppend
            |> Array.unzip

        indices <- resultIndices
        values <- resultValues

    override this.Item
        with get (vectorMask: Mask1D option) : Vector<'a> =
            let resultElements =
                match vectorMask with
                | Some mask ->
                    if mask.Length <> this.Length then
                        invalidArg
                            "mask"
                            (sprintf "Argument has invalid dimension. Need %i, but given %i" this.Length mask.Length)

                    this.Elements |> Array.filter (fun (i, _) -> mask.[i])
                | _ -> this.Elements

            upcast SparseVector(resultElements, this.Length)

        and set (vectorMask: Mask1D option) (vector: Vector<'a>) =
            let mask =
                match vectorMask with
                | Some m ->
                    if m.Length <> this.Length then
                        invalidArg
                            "mask"
                            (sprintf "Argument has invalid dimension. Need %i, but given %i" this.Length m.Length)
                    m
                | _ -> Mask1D(Array.empty, this.Length, true) // Empty complemented mask is equal to none
            match vector with
            | :? SparseVector<'a> -> this.SetSparseVector mask (downcast vector)
            | _ -> failwith "Not Implemented"

    override this.Item
        with get (idx: int) : Scalar<'a> =
            let value = this.Elements |> Array.tryFind (fst >> (=) idx)
            match value with
            | Some (_, a) -> Scalar a
            | _ -> failwith "Attempt to address to zero value" // Needs correction

        and set (idx: int) (Scalar (value: 'a)) =
            let mutable isFound = false
            let mutable i = 0
            while not isFound && i < this.Elements.Length do
                if this.Indices.[i] = idx then
                    isFound <- true
                    values.[i] <- value
                i <- i + 1

            if not isFound then
                let resultIndices, resultValues =
                    (indices, values)
                    ||> Array.zip
                    |> Array.append [|idx, value|]
                    |> Array.unzip

                indices <- resultIndices
                values <- resultValues

    override this.Fill
        with set (vectorMask: Mask1D option) (Scalar (value: 'a)) =
            match vectorMask with
            | None ->
                let resultIndices, resultValues =
                    [| for i in 0 .. this.Length - 1 do
                        yield (i, value) |]
                    |> Array.unzip

                indices <- resultIndices
                values <- resultValues
            | Some mask ->
                if mask.Length <> this.Length then
                    invalidArg
                        "mask"
                        (sprintf "Argument has invalid dimension. Need %A, but given %A" this.Length mask.Length)
                let newIndices = mask.Indices
                let indicesToSet, indicesToAppend =
                    newIndices
                    |> Array.partition (
                        fun i ->
                        this.Elements
                        |> Array.exists (fst >> (=) i))

                let workflow =
                    opencl {
                        let command =
                            <@
                                fun (ndRange: _1D)
                                    (resultValuesBuffer: 'a[])
                                    (resultIndicesBuffer: int[])
                                    (indicesToSetBuffer: int[]) ->

                                    let i = ndRange.GlobalID0
                                    for j in 0 .. resultValuesBuffer.Length - 1 do
                                        if resultIndicesBuffer.[j] = indicesToSetBuffer.[i] then
                                            resultValuesBuffer.[j] <- value
                            @>

                        let resultValues = this.Values
                        let resultIndices = this.Indices
                        let binder kernelP =
                            let ndRange = _1D(indicesToSet.Length)
                            kernelP
                                ndRange
                                resultValues
                                resultIndices
                                indicesToSet

                        do! RunCommand command binder
                        return! ToHost resultValues
                    }

                let renewedValues = oclContext.RunSync workflow
                let elementsToAppend = indicesToAppend |> Array.map (fun i -> (i, value))

                let resultIndices, resultValues =
                    (indices, renewedValues)
                    ||> Array.zip
                    |> Array.append elementsToAppend
                    |> Array.unzip

                indices <- resultIndices
                values <- resultValues

    member internal this.VxCOOm
        (matrix: COOMatrix<'a>)
        (mask: Mask1D)
        (semiring: Semiring<'a>) : Vector<'a> =

        let matrixColumnCount = matrix.ColumnCount

        let (BinaryOp plus) = semiring.PlusMonoid.Append
        let (BinaryOp mult) = semiring.Times
        let zero = semiring.PlusMonoid.Zero

        let resultIndices = Array.init matrixColumnCount id

        let workflow =
            opencl {
                let command =
                    <@
                        fun (ndRange: _1D)
                            (resultValuesBuffer: 'a[])
                            (matrixValuesBuffer: 'a[])
                            (matrixRowsBuffer: int[])
                            (matrixColumnsBuffer: int[])
                            (vectorValuesBuffer: 'a[])
                            (vectorIndicesBuffer: int[]) ->

                            let i = ndRange.GlobalID0
                            if mask.[i] then
                                let mutable localResultBuffer = resultValuesBuffer.[i]
                                for j in 0 .. vectorValuesBuffer.Length - 1 do
                                    for k in 0 .. matrixValuesBuffer.Length - 1 do
                                        if vectorIndicesBuffer.[j] = matrixRowsBuffer.[k] && matrixColumnsBuffer.[k] = i then
                                            localResultBuffer <- (%plus) localResultBuffer
                                                ((%mult) vectorValuesBuffer.[j] matrixValuesBuffer.[k])
                                resultValuesBuffer.[i] <- localResultBuffer
                    @>

                let vectorValues = this.Values
                let vectorIndices = this.Indices
                let matrixValues = matrix.Values
                let matrixRows = matrix.Rows
                let matrixColumns = matrix.Columns
                let resultValues = Array.init matrixColumnCount (fun _ -> zero)
                let binder kernelP =
                    let ndRange = _1D(matrixColumnCount)
                    kernelP
                        ndRange
                        resultValues
                        matrixValues
                        matrixRows
                        matrixColumns
                        vectorValues
                        vectorIndices

                do! RunCommand command binder
                return! ToHost resultValues
            }

        let resultValues = oclContext.RunSync workflow

        let resultElements =
            (resultIndices, resultValues)
            ||> Array.zip
            |> Array.filter (snd >> (<>) zero)

        upcast SparseVector(resultElements, matrixColumnCount)

    override this.Vxm
        (matrix: Matrix<'a>)
        (vectorMask: Mask1D option)
        (semiring: Semiring<'a>) =

        if matrix.RowCount <> this.Length then
            invalidArg
                "matrix"
                (sprintf "Argument has invalid dimension. Need %i, but given %i" this.Length matrix.RowCount)

        let mask =
            match vectorMask with
            | Some m ->
                if m.Length <> this.Length then
                    invalidArg
                        "mask"
                        (sprintf "Argument has invalid dimension. Need %i, but given %i" this.Length m.Length)
                m
            | _ -> Mask1D(Array.empty, this.Length, true) // Empty complemented mask is equal to none

        match matrix with
        | :? COOMatrix<'a> -> this.VxCOOm (downcast matrix) mask semiring
        | _ -> failwith "Not Implemented"

    member internal this.EWiseAddSparse
        (vector: SparseVector<'a>)
        (mask: Mask1D)
        (semiring: Semiring<'a>) : Vector<'a> =

        let (BinaryOp plus) = semiring.PlusMonoid.Append
        let zero = semiring.PlusMonoid.Zero

        let leftIndices = this.Indices
        let rightIndices = vector.Indices

        let firstIndices, secondIndices =
            if this.Indices.Length > vector.Indices.Length
            then this.Indices, vector.Indices
            else vector.Indices, this.Indices

        let longSide = firstIndices.Length
        let shortSide = secondIndices.Length

        let maxIndex = max firstIndices.[leftIndices.Length - 1] secondIndices.[rightIndices.Length - 1]
        let minIndex = min firstIndices.[0] secondIndices.[0]

        let workflow =
            opencl {

                let allIndices = Array.zeroCreate <| leftIndices.Length + rightIndices.Length

                let extendedFirstIndices = Array.concat [ [| minIndex - 1 |] ; firstIndices ; [| maxIndex + 1 |] ]
                let extendedSecondIndices = Array.concat [ [| minIndex - 1 |] ; secondIndices ; [| maxIndex + 1 |] ]

                let createUnion =
                    <@
                        fun (ndRange: _1D)
                            (extendedFirstIndicesBuffer: int[])
                            (extendedSecondIndicesBuffer: int[])
                            (allIndicesBuffer: int[]) ->

                            let i = ndRange.GlobalID0

                            let mutable leftEdge =
                                if i < shortSide
                                then 1
                                else i + 2 - shortSide

                            let mutable rightEdge =
                                if i < longSide - 1
                                then i + 1
                                else longSide

                            let mutable boundaryX, boundaryY = leftEdge, i + 2 - leftEdge

                            while leftEdge < rightEdge do
                                let middleIdx = (leftEdge + rightEdge) / 2
                                if extendedFirstIndicesBuffer.[middleIdx] < extendedSecondIndicesBuffer.[i + 2 - middleIdx]
                                then
                                    boundaryX <- middleIdx
                                    boundaryY <- i + 1 - middleIdx
                                    leftEdge <- middleIdx + 1
                                else
                                    boundaryX <- middleIdx - 1
                                    boundaryY <- i + 2 - middleIdx
                                    rightEdge <- middleIdx

                            if extendedFirstIndicesBuffer.[boundaryX] < extendedSecondIndicesBuffer.[boundaryY]
                            then allIndicesBuffer.[i] <- extendedFirstIndicesBuffer.[boundaryX]
                            else allIndicesBuffer.[i] <- extendedSecondIndicesBuffer.[boundaryY]
                    @>

                let auxiliaryArray = Array.zeroCreate allIndices.Length

                let createAuxiliaryArray =
                    <@
                        fun (ndRange: _1D)
                            (allIndicesBuffer: int[])
                            (auxiliaryArrayBuffer: int[]) ->

                            let i = ndRange.GlobalID0

                            if i > 0 && allIndicesBuffer.[i - 1] <> allIndicesBuffer.[i]
                            then auxiliaryArrayBuffer.[0] <- 1
                            else auxiliaryArrayBuffer.[0] <- 0
                    @>

                //let prefixSumArray = Array.zeroCreate allIndices.Length

                let command =
                    <@
                        fun (ndRange: _1D)
                            (allIndicesBuffer: int[])
                            (auxiliaryArrayBuffer: int[])
                            (resultIndicesBuffer: int[]) ->

                            let i = ndRange.GlobalID0

                            let mutable prefixSum = 0
                            for j in 0 .. i do
                                prefixSum <- prefixSum + auxiliaryArrayBuffer.[j]

                            //prefixSumArrayBuffer.[i] <- localResultBuffer
                            resultIndicesBuffer.[i] <- allIndicesBuffer.[prefixSum]
                    @>

                return 42
            }













        let commonIndices =
            leftIndices
            |> Array.filter (fun i ->
                rightIndices
                |> Array.contains i)

        let leftCommonElements, leftUniqueElements =
            this.Elements
            |> Array.partition (fun (i, _) ->
                commonIndices
                |> Array.contains i)
        let rightCommonElements, rightUniqueElements =
            vector.Elements
            |> Array.partition (fun (i, _) ->
                commonIndices
                |> Array.contains i)

        let workflow =
            opencl {
                let command =
                    <@
                        fun (ndRange: _2D)
                            (resultValuesBuffer: 'a[])
                            (leftValuesBuffer: 'a[])
                            (leftIndicesBuffer: int[])
                            (rightValuesBuffer: 'a[])
                            (rightIndicesBuffer: int[]) ->

                            let i, j = ndRange.GlobalID0, ndRange.GlobalID1
                            let index = leftIndicesBuffer.[i]
                            if mask.[index] && rightIndicesBuffer.[j] = index then
                                resultValuesBuffer.[i] <- (%plus) leftValuesBuffer.[i] rightValuesBuffer.[j]
                    @>

                let leftCommonIndices, leftCommonValues = leftCommonElements |> Array.unzip
                let rightCommonIndices, rightCommonValues = rightCommonElements |> Array.unzip

                let resultCommonValues = Array.init leftCommonElements.Length (fun _ -> zero)

                let binder kernelP =
                    let ndRange = _2D(leftCommonElements.Length, rightCommonElements.Length)
                    kernelP
                        ndRange
                        resultCommonValues
                        leftCommonValues
                        leftCommonIndices
                        rightCommonValues
                        rightCommonIndices

                do! RunCommand command binder
                return! ToHost resultCommonValues
            }

        let resultCommonValues = oclContext.RunSync workflow

        let resultCommonIndices =
            leftCommonElements
            |> Array.unzip
            |> fst
        let uniqueIndices =
            (leftUniqueElements, rightUniqueElements)
            ||> Array.append
            |> Array.filter (fun (i, _) -> mask.[i])
        let resultElements =
            (resultCommonIndices, resultCommonValues)
            ||> Array.zip
            |> Array.append uniqueIndices
            |> Array.filter (snd >> (<>) zero)

        upcast SparseVector(resultElements, this.Length)

    override this.EWiseAdd
        (vector: Vector<'a>)
        (vectorMask: Mask1D option)
        (semiring: Semiring<'a>) =

        if vector.Length <> this.Length then
            invalidArg
                "vector"
                (sprintf "Argument has invalid dimension. Need %i, but given %i" this.Length vector.Length)

        let mask =
            match vectorMask with
            | Some m ->
                if m.Length <> this.Length then
                    invalidArg
                        "mask"
                        (sprintf "Argument has invalid dimension. Need %i, but given %i" this.Length m.Length)
                m
            | _ -> Mask1D(Array.empty, this.Length, true) // Empty complemented mask is equal to none

        match vector with
        | :? SparseVector<'a> -> this.EWiseAddSparse (downcast vector) mask semiring
        | _ -> failwith "Not Implemented"

    member internal this.EWiseMultSparse
        (vector: SparseVector<'a>)
        (mask: Mask1D)
        (semiring: Semiring<'a>) : Vector<'a> =

        let (BinaryOp mult) = semiring.Times
        let zero = semiring.PlusMonoid.Zero

        let leftIndices = this.Indices
        let rightIndices = vector.Indices

        let commonIndices =
            leftIndices
            |> Array.filter (fun i ->
                rightIndices
                |> Array.contains i)

        let leftCommonElements, leftUniqueElements =
            this.Elements
            |> Array.partition (fun (i, _) ->
                commonIndices
                |> Array.contains i)
        let rightCommonElements, rightUniqueElements =
            vector.Elements
            |> Array.partition (fun (i, _) ->
                commonIndices
                |> Array.contains i)

        let leftCommonIndices, leftCommonValues = leftCommonElements |> Array.unzip
        let rightCommonIndices, rightCommonValues = rightCommonElements |> Array.unzip

        let workflow =
            opencl {
                let command =
                    <@
                        fun (ndRange: _2D)
                            (resultValuesBuffer: 'a[])
                            (leftValuesBuffer: 'a[])
                            (leftIndicesBuffer: int[])
                            (rightValuesBuffer: 'a[])
                            (rightIndicesBuffer: int[]) ->

                            let i, j = ndRange.GlobalID0, ndRange.GlobalID1
                            let index = leftIndicesBuffer.[i]
                            if mask.[index] && rightIndicesBuffer.[j] = index then
                                resultValuesBuffer.[i] <- (%mult) leftValuesBuffer.[i] rightValuesBuffer.[j]
                    @>

                let resultCommonValues = Array.init leftCommonElements.Length (fun _ -> zero)

                let binder kernelP =
                    let ndRange = _2D(leftCommonElements.Length, rightCommonElements.Length)
                    kernelP
                        ndRange
                        resultCommonValues
                        leftCommonValues
                        leftCommonIndices
                        rightCommonValues
                        rightCommonIndices

                do! RunCommand command binder
                return! ToHost resultCommonValues
            }

        let resultCommonValues = oclContext.RunSync workflow

        let resultElements =
            (leftCommonIndices, resultCommonValues)
            ||> Array.zip
            |> Array.filter (snd >> (<>) zero)

        upcast SparseVector(resultElements, this.Length)

    override this.EWiseMult
        (vector: Vector<'a>)
        (vectorMask: Mask1D option)
        (semiring: Semiring<'a>) =

        if vector.Length <> this.Length then
            invalidArg
                "vector"
                (sprintf "Argument has invalid dimension. Need %i, but given %i" this.Length vector.Length)

        let mask =
            match vectorMask with
            | Some m ->
                if m.Length <> this.Length then
                    invalidArg
                        "mask"
                        (sprintf "Argument has invalid dimension. Need %i, but given %i" this.Length m.Length)
                m
            | _ -> Mask1D(Array.empty, this.Length, true) // Empty complemented mask is equal to none

        match vector with
        | :? SparseVector<'a> -> this.EWiseMultSparse (downcast vector) mask semiring
        | _ -> failwith "Not Implemented"

    override this.Apply
        (vectorMask: Mask1D option)
        (operator: UnaryOp<'a, 'b>) =
        let mask =
            match vectorMask with
            | Some m ->
                if m.Length <> this.Length then
                    invalidArg
                        "mask"
                        (sprintf "Argument has invalid dimension. Need %i, but given %i" this.Length m.Length)
                m
            | _ -> Mask1D(Array.empty, this.Length, true) // Empty complemented mask is equal to none

        let (UnaryOp op) = operator

        let vectorIndices, vectorValues =
            this.Elements
            |> Array.filter (fun (i, _) -> mask.[i])
            |> Array.unzip

        let workflow =
            opencl {
                let command =
                    <@
                        fun (ndRange: _1D)
                            (resultValuesBuffer: 'b[])
                            (vectorValuesBuffer: 'a[]) ->

                            let i = ndRange.GlobalID0
                            resultValuesBuffer.[i] <- (%op) vectorValuesBuffer.[i]
                    @>

                let resultValues = Array.zeroCreate vectorValues.Length // No matter what the initial values are

                let binder kernelP =
                    let ndRange = _1D(resultValues.Length)
                    kernelP
                        ndRange
                        resultValues
                        vectorValues

                do! RunCommand command binder
                return! ToHost resultValues
            }

        let resultValues = oclContext.RunSync workflow
        let resultElements = Array.zip vectorIndices resultValues

        upcast SparseVector(resultElements, this.Length)

    override this.Reduce
        (monoid: Monoid<'a>) =
        let mutable complementedLength = 1
        let mutable iterations = 0
        while complementedLength < this.Elements.Length do
            complementedLength <- complementedLength * 2
            iterations <- iterations + 1

        let (BinaryOp plus) = monoid.Append
        let zero = monoid.Zero

        let mutable offset = complementedLength / 2
        let mutable resultValues =
            Array.init (complementedLength - this.Values.Length) (fun _ -> zero)
            |> Array.append this.Values

        let workflow =
            opencl {
                let values = resultValues

                while offset > 0 do
                    let command =
                        <@
                            fun (ndRange: _1D)
                                (valuesBuffer: 'a[])
                                (offset: int) ->

                                let i = ndRange.GlobalID0
                                valuesBuffer.[i] <- (%plus) valuesBuffer.[i] valuesBuffer.[i + offset]
                        @>

                    let binder kernelP =
                        let ndRange = _1D(offset)
                        kernelP
                            ndRange
                            values
                            offset

                    do! RunCommand command binder
                    offset <- offset / 2

                return! ToHost values
            }

        resultValues <- oclContext.RunSync workflow

        Scalar resultValues.[0]

and DenseVector<'a when 'a : struct and 'a : equality>(vector: 'a[], monoid: Monoid<'a>) =
    inherit Vector<'a>(vector.Length)

    // Not Implemented
    new(monoid: Monoid<'a>) = DenseVector(Array.zeroCreate<'a> 0, monoid)
    // Not Implemented
    new(listOfIndices: int list, monoid: Monoid<'a>) = DenseVector(Array.zeroCreate<'a> 0, monoid)

    member this.Monoid = monoid
    member this.Values = vector

    override this.AsArray = failwith "Not Implemented"
    override this.Clear() = failwith "Not Implemented"

    override this.Mask =
        let indices =
            [| for i in 0 .. this.Length - 1 do
                if this.Values.[i] <> this.Monoid.Zero then yield i |]
        Some <| Mask1D(indices, this.Length, false)

    override this.Complemented =
        let indices =
            [| for i in 0 .. this.Length - 1 do
                if this.Values.[i] <> this.Monoid.Zero then yield i |]
        Some <| Mask1D(indices, this.Length, true)

    override this.Item
        with get (mask: Mask1D option) : Vector<'a> = failwith "Not Implemented"
        and set (mask: Mask1D option) (value: Vector<'a>) = failwith "Not Implemented"
    override this.Item
        with get (idx: int) : Scalar<'a> = failwith "Not Implemented"
        and set (idx: int) (value: Scalar<'a>) = failwith "Not Implemented"
    override this.Fill
        with set (mask: Mask1D option) (value: Scalar<'a>) = failwith "Not Implemented"

    override this.Vxm a b c = failwith "Not Implemented"
    override this.EWiseAdd a b c = failwith "Not Implemented"
    override this.EWiseMult a b c = failwith "Not Implemented"
    override this.Apply a b = failwith "Not Implemented"
    override this.Reduce (monoid: Monoid<'a>) = failwith "Not Implemented"
