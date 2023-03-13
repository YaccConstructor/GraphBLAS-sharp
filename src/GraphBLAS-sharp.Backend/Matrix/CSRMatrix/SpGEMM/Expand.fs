namespace GraphBLAS.FSharp.Backend.Matrix.CSR.SpGEMM

open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Predefined
open GraphBLAS.FSharp.Backend.Objects.ClContext
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Objects.ClCell
open FSharp.Quotations
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions

type Indices = ClArray<int>

type Values<'a> = ClArray<'a>

module Expand =
    /// <summary>
    /// Get the number of non-zero elements for each row of the right matrix for non-zero item in left matrix.
    /// </summary>
    let requiredRawsLengths =
        <@ fun gid (leftMatrixColumnsIndices: Indices) (rightMatrixRawPointers: Indices) ->
            let columnIndex = leftMatrixColumnsIndices.[gid]
            let startRawIndex = rightMatrixRawPointers.[columnIndex]
            let exclusiveRawEndIndex = rightMatrixRawPointers.[columnIndex + 1]

            exclusiveRawEndIndex - startRawIndex @>

    /// <summary>
    /// Get the pointer to right matrix raw for each non-zero in left matrix.
    /// </summary>
    let requiredRawPointers =
        <@ fun gid (leftMatrixColumnsIndices: Indices) (rightMatrixRawPointers: Indices) ->
            let columnIndex = leftMatrixColumnsIndices.[gid]
            let startRawIndex = rightMatrixRawPointers.[columnIndex]

            startRawIndex @>

    let processLeftMatrixColumnsAndRightMatrixRawPointers (clContext: ClContext) workGroupSize writeOperation =

        let kernel =
            <@ fun (ndRange: Range1D) columnsLength (leftMatrixColumnsIndices: Indices) (rightMatrixRawPointers: Indices) (result: Indices) ->

                let gid = ndRange.GlobalID0

                if gid < columnsLength then
                   result.[gid] <- (%writeOperation) gid leftMatrixColumnsIndices rightMatrixRawPointers @>

        let kernel = clContext.Compile kernel

        fun (processor: MailboxProcessor<_>) (leftMatrixColumnsIndices: Indices) (rightMatrixRawPointers: Indices) ->
            let resultLength = leftMatrixColumnsIndices.Length

            let requiredRawsLengths =
                clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, resultLength)

            let kernel = kernel.GetKernel()

            let ndRange =
                Range1D.CreateValid(resultLength, workGroupSize)

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.KernelFunc
                            ndRange
                            resultLength
                            leftMatrixColumnsIndices
                            rightMatrixRawPointers
                            requiredRawsLengths)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            requiredRawsLengths

    let extractLeftMatrixRequiredValuesAndColumns (clContext: ClContext) workGroupSize =

        let getUniqueBitmap =
            ClArray.getUniqueBitmap clContext workGroupSize

        let prefixSumExclude =
            PrefixSum.standardExcludeInplace clContext workGroupSize

        let indicesScatter =
            Scatter.runInplace clContext workGroupSize

        let dataScatter =
            Scatter.runInplace clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (leftMatrix: ClMatrix.CSR<'a>) (globalRightMatrixRawsStartPositions: Indices) ->

            let leftMatrixRequiredPositions, resultLength  =
                let bitmap =
                    getUniqueBitmap processor DeviceOnly globalRightMatrixRawsStartPositions

                let length = (prefixSumExclude processor bitmap).ToHostAndFree processor

                bitmap, length

            let requiredLeftMatrixValues =
                clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, resultLength)

            indicesScatter processor leftMatrixRequiredPositions leftMatrix.Values requiredLeftMatrixValues

            let requiredLeftMatrixColumns =
                clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, resultLength)

            dataScatter processor leftMatrixRequiredPositions leftMatrix.Columns requiredLeftMatrixColumns

            leftMatrixRequiredPositions.Free processor

            requiredLeftMatrixColumns, requiredLeftMatrixValues

    let getGlobalMap (clContext: ClContext) workGroupSize =

        let zeroCreate = ClArray.zeroCreate<int> clContext workGroupSize

        let assignUnits = ClArray.assignManyInit clContext workGroupSize <@ fun _ -> 1 @>

        let prefixSum = PrefixSum.standardIncludeInplace clContext workGroupSize

        fun (processor: MailboxProcessor<_>) resultLength (globalRightMatrixValuesPositions: Indices) ->

            /// We get an array of zeros
            let globalPositions = zeroCreate processor DeviceOnly resultLength

            // Insert units at the beginning of new lines (source positions)
            assignUnits processor globalRightMatrixValuesPositions globalPositions

            // Apply the prefix sum, SIDE EFFECT!!!
            // get an array where different sub-arrays of pointers to elements of the same row differ in values
            (prefixSum processor globalPositions).Free processor

            globalPositions

    let getResultRowPointers (clContext: ClContext) workGroupSize =

        let kernel =
            <@ fun (ndRange: Range1D) length (leftMatrixRowPointers: Indices) (globalArrayRightMatrixRawPointers: Indices) (result: Indices) ->

                let gid = ndRange.GlobalID0

                // do not touch the last element
                if gid < length - 1 then
                    let rowPointer = leftMatrixRowPointers.[gid]
                    let globalPointer = globalArrayRightMatrixRawPointers.[rowPointer]

                    result.[gid] <- globalPointer @>

        let kernel = clContext.Compile kernel

        let createResultPointersBuffer = ClArray.create clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (globalLength: int) (leftMatrixRowPointers: Indices) (globalRightMatrixRowPointers: Indices) ->

            // The last element must be equal to the length of the global array.
            let result =
                createResultPointersBuffer processor DeviceOnly leftMatrixRowPointers.Length globalLength

            let kernel = kernel.GetKernel()

            // do not touch the last element
            let ndRange =
                Range1D.CreateValid(leftMatrixRowPointers.Length - 1, workGroupSize)

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.KernelFunc
                            ndRange
                            leftMatrixRowPointers.Length
                            leftMatrixRowPointers
                            globalRightMatrixRowPointers
                            result)
            )

            processor.Post <| Msg.CreateRunMsg<_, _> kernel

            result

    let processPositions (clContext: ClContext) workGroupSize =

        let getRequiredRawsLengths =
            processLeftMatrixColumnsAndRightMatrixRawPointers clContext workGroupSize requiredRawsLengths

        let removeDuplications = ClArray.removeDuplications clContext workGroupSize

        let prefixSumExclude =
            PrefixSum.standardExcludeInplace clContext workGroupSize

        let extractLeftMatrixRequiredValuesAndColumns =
            extractLeftMatrixRequiredValuesAndColumns clContext workGroupSize

        let getGlobalPositions = getGlobalMap clContext workGroupSize

        let getRowPointers = getResultRowPointers clContext workGroupSize

        let getRequiredRightMatrixValuesPointers =
            processLeftMatrixColumnsAndRightMatrixRawPointers clContext workGroupSize requiredRawPointers

        fun (processor: MailboxProcessor<_>) (leftMatrix: ClMatrix.CSR<'a>) (rightMatrix: ClMatrix.CSR<'b>) ->
            // array of required right matrix rows length obtained by left matrix columns
            let requiredRawsLengths =
                getRequiredRawsLengths processor leftMatrix.Columns rightMatrix.RowPointers

            // global expanded array length (sum of previous length) SIDE EFFECT!!!
            let globalLength =
                (prefixSumExclude processor requiredRawsLengths).ToHostAndFree processor

            // rename array after side effect of prefix sum include
            // positions in global array for right matrix raws with duplicates
            let globalRightMatrixRowsStartPositions = requiredRawsLengths

            /// Extract required left matrix columns and values by global right matrix pointers.
            /// Then get required right matrix rows (pointers to rows) by required left matrix columns.

            // extract required left matrix columns and rows by right matrix rows positions
            let requiredLeftMatrixColumns, requiredLeftMatrixValues =
                extractLeftMatrixRequiredValuesAndColumns processor leftMatrix globalRightMatrixRowsStartPositions

            // pointers to required raws in right matrix values
            // rows to be placed by globalRightMatrixRowsStartPositionsWithoutDuplicates
            let requiredRightMatrixRawPointers =
                getRequiredRightMatrixValuesPointers processor requiredLeftMatrixColumns rightMatrix.RowPointers

            requiredLeftMatrixColumns.Free processor

            // remove duplications in right matrix rows positions in global extended array
            let globalRightMatrixRawsPointersWithoutDuplicates =
                removeDuplications processor globalRightMatrixRowsStartPositions

            // RESULT row pointers into result expanded (obtained by multiplication) array
            let resultRowPointers =
                getRowPointers processor globalLength leftMatrix.RowPointers globalRightMatrixRowsStartPositions

            globalRightMatrixRowsStartPositions.Free processor

            // int map to distinguish different raws in a general array. 1 for first, 2 for second and so forth...
            let globalMap =
                getGlobalPositions processor globalLength globalRightMatrixRawsPointersWithoutDuplicates

            globalMap, globalRightMatrixRawsPointersWithoutDuplicates, requiredLeftMatrixValues, requiredRightMatrixRawPointers, resultRowPointers

    let expandRightMatrixValuesIndices (clContext: ClContext) workGroupSize =

        let kernel =
            <@ fun (ndRange: Range1D) length (globalRightMatrixValuesPositions: Indices) (requiredRightMatrixValuesPointers: Indices) (globalPositions: Indices) (result: Indices) ->

                let gid = ndRange.GlobalID0

                if gid < length then
                    // index corresponding to the position of pointers
                    let positionIndex = globalPositions.[gid] - 1

                    // the position of the beginning of a new line of pointers
                    let sourcePosition = globalRightMatrixValuesPositions.[positionIndex]

                    // offset from the source pointer
                    let offsetFromSourcePosition = gid - sourcePosition

                    // pointer to the first element in the row of the right matrix from which
                    // the offset will be counted to get pointers to subsequent elements in this row
                    let sourcePointer = requiredRightMatrixValuesPointers.[positionIndex]

                    // adding up the mix with the source pointer,
                    // we get a pointer to a specific element in the raw
                    result.[gid] <- sourcePointer + offsetFromSourcePosition @>

        let kernel = clContext.Compile kernel

        fun (processor: MailboxProcessor<_>) (globalRightMatrixRawsStartPositions: Indices) (requiredRightMatrixValuesPointers: Indices) (globalMap: Indices) ->

            let globalRightMatrixValuesPointers =
                clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, globalMap.Length)

            let kernel = kernel.GetKernel()

            let ndRange =
                Range1D.CreateValid(globalMap.Length, workGroupSize)

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.KernelFunc
                            ndRange
                            globalMap.Length
                            globalRightMatrixRawsStartPositions
                            requiredRightMatrixValuesPointers
                            globalMap
                            globalRightMatrixValuesPointers)
            )

            processor.Post <| Msg.CreateRunMsg<_, _> kernel

            globalRightMatrixValuesPointers

    let expandLeftMatrixValues (clContext: ClContext) workGroupSize =

        let kernel =
            <@ fun (ndRange: Range1D) resultLength (globalBitmap: Indices) (leftMatrixValues: Values<'a>) (resultValues: Values<'a>) ->

                let gid = ndRange.GlobalID0

                // globalBitmap.Length == resultValues.Length
                if gid < resultLength then
                    let valueIndex = globalBitmap.[gid] - 1

                    resultValues.[gid] <- leftMatrixValues.[valueIndex] @>

        let kernel = clContext.Compile kernel

        fun (processor: MailboxProcessor<_>) (globalMap: Indices) (leftMatrixValues: Values<'a>) ->

            let expandedLeftMatrixValues =
                clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, globalMap.Length)

            let kernel = kernel.GetKernel()

            let ndRange =
                Range1D.CreateValid(globalMap.Length, workGroupSize)

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.KernelFunc
                            ndRange
                            globalMap.Length
                            globalMap
                            leftMatrixValues
                            expandedLeftMatrixValues)
            )

            processor.Post <| Msg.CreateRunMsg<_, _> kernel

            expandedLeftMatrixValues

    let getRightMatrixColumnsAndValues (clContext: ClContext) workGroupSize =
        let gatherRightMatrixData = Gather.run clContext workGroupSize

        let gatherIndices = Gather.run clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (globalPositions: Indices) (rightMatrix: ClMatrix.CSR<'a>) ->
            // gather all required right matrix values
            let extendedRightMatrixValues =
                clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, globalPositions.Length)

            gatherRightMatrixData processor globalPositions rightMatrix.Values extendedRightMatrixValues

            // gather all required right matrix column indices
            let extendedRightMatrixColumns =
                clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, globalPositions.Length)

            gatherIndices processor globalPositions rightMatrix.Columns extendedRightMatrixColumns

            extendedRightMatrixValues, extendedRightMatrixColumns

    let run (clContext: ClContext) workGroupSize (multiplication: Expr<'a -> 'b -> 'c>) =

        let processPositions = processPositions clContext workGroupSize

        let expandLeftMatrixValues =
            expandLeftMatrixValues clContext workGroupSize

        let expandRightMatrixValuesPointers =
            expandRightMatrixValuesIndices clContext workGroupSize

        let getRightMatrixColumnsAndValues =
            getRightMatrixColumnsAndValues clContext workGroupSize

        let map2 = ClArray.map2 clContext workGroupSize multiplication

        fun (processor: MailboxProcessor<_>) (leftMatrix: ClMatrix.CSR<'a>) (rightMatrix: ClMatrix.CSR<'b>) ->

            let globalMap, globalRightMatrixRowsPointers, requiredLeftMatrixValues, requiredRightMatrixRowPointers, resultRowPointers
                = processPositions processor leftMatrix rightMatrix

            // left matrix values correspondingly to right matrix values
            let extendedLeftMatrixValues =
                expandLeftMatrixValues processor globalMap requiredLeftMatrixValues

            // extended pointers to all required right matrix numbers
            let globalRightMatrixValuesPointers =
                expandRightMatrixValuesPointers processor globalRightMatrixRowsPointers requiredRightMatrixRowPointers globalMap

            let extendedRightMatrixValues, extendedRightMatrixColumns =
                getRightMatrixColumnsAndValues processor globalRightMatrixValuesPointers rightMatrix

            /// Multiplication
            let multiplicationResult  =
                map2 processor DeviceOnly extendedLeftMatrixValues extendedRightMatrixValues

            multiplicationResult, extendedRightMatrixColumns, resultRowPointers
