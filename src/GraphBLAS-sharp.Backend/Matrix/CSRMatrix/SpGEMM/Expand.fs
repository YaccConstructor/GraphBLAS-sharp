namespace GraphBLAS.FSharp.Backend.Matrix.CSR.SpGEMM

open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Predefined
open GraphBLAS.FSharp.Backend.Objects.ClContext
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Objects.ClCell
open FSharp.Quotations

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

    let getGlobalPositions (clContext: ClContext) workGroupSize =

        let zeroCreate = ClArray.zeroCreate<int> clContext workGroupSize

        let assignUnits = ClArray.assignManyInit clContext workGroupSize <@ fun _ -> 1 @>

        let prefixSum = PrefixSum.standardIncludeInplace clContext workGroupSize

        fun (processor: MailboxProcessor<_>) resultLength (globalRightMatrixValuesPositions: Indices) ->

            /// We get an array of zeros
            let globalPositions = zeroCreate processor DeviceOnly resultLength

            // Insert units at the beginning of new lines (source positions)
            assignUnits processor globalRightMatrixValuesPositions globalPositions

            // Apply the prefix sum,
            // get an array where different sub-arrays of pointers to elements of the same row differ in values
            (prefixSum processor globalPositions).Free processor

            globalPositions

    let getRightMatrixPointers (clContext: ClContext) workGroupSize =

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

        fun (processor: MailboxProcessor<_>) (resultLength: int) (globalRightMatrixValuesPositions: Indices) (requiredRightMatrixValuesPointers: Indices) (globalPositions: Indices) ->

            let globalRightMatrixValuesPointers =
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
                            globalRightMatrixValuesPositions
                            requiredRightMatrixValuesPointers
                            globalPositions
                            globalRightMatrixValuesPointers)
            )

            processor.Post <| Msg.CreateRunMsg<_, _> kernel

            globalRightMatrixValuesPointers

    let getLeftMatrixValuesCorrespondinglyToPositionsPattern<'a> (clContext: ClContext) workGroupSize =

        let kernel =
            <@ fun (ndRange: Range1D) globalLength (globalPositions: Indices) (rightMatrixValues: ClArray<'a>) (result: ClArray<'a>) ->

                 let gid = ndRange.GlobalID0

                 if gid < globalLength then
                     let valuePosition = globalPositions.[gid] - 1

                     result.[gid] <- rightMatrixValues.[valuePosition] @>

        let kernel = clContext.Compile kernel

        fun (processor: MailboxProcessor<_>) (globalLength: int) (globalPositions: Indices) (rightMatrixValues: Values<'a>)->

            // globalLength == globalPositions.Length
            let resultLeftMatrixValues =
                clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, globalLength)

            let kernel = kernel.GetKernel()

            let ndRange =
                Range1D.CreateValid(globalLength, workGroupSize)

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.KernelFunc
                            ndRange
                            globalLength
                            globalPositions
                            rightMatrixValues
                            resultLeftMatrixValues)
            )

            processor.Post <| Msg.CreateRunMsg<_, _> kernel

            resultLeftMatrixValues

    let getResultRowPointers (clContext: ClContext) workGroupSize =

        let kernel =
            <@ fun (ndRange: Range1D) length (leftMatrixRowPointers: Indices) (globalArrayRightMatrixRawPointers: Indices) (result: Indices) ->

                let gid = ndRange.GlobalID0

                if gid < length then
                    let rowPointer = leftMatrixRowPointers.[gid]
                    let globalPointer = globalArrayRightMatrixRawPointers.[rowPointer]

                    result.[gid] <- globalPointer
                    @>

        let kernel = clContext.Compile kernel

        fun (processor: MailboxProcessor<_>) (leftMatrixRowPointers: Indices) (globalArrayRightMatrixRawPointers: Indices) ->

            let result =
                clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, leftMatrixRowPointers.Length)

            let kernel = kernel.GetKernel()

            let ndRange =
                Range1D.CreateValid( leftMatrixRowPointers.Length, workGroupSize)

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.KernelFunc
                            ndRange
                            leftMatrixRowPointers.Length
                            leftMatrixRowPointers
                            globalArrayRightMatrixRawPointers
                            result)
            )

            processor.Post <| Msg.CreateRunMsg<_, _> kernel

            result

    let run (clContext: ClContext) workGroupSize (multiplication: Expr<'a -> 'b -> 'c>) =

        let getRequiredRawsLengths =
            processLeftMatrixColumnsAndRightMatrixRawPointers clContext workGroupSize requiredRawsLengths

        let prefixSumExclude =
            PrefixSum.standardExcludeInplace clContext workGroupSize

        let getRequiredRightMatrixValuesPointers =
            processLeftMatrixColumnsAndRightMatrixRawPointers clContext workGroupSize requiredRawPointers

        let getGlobalPositions = getGlobalPositions clContext workGroupSize

        let getRightMatrixValuesPointers =
            getRightMatrixPointers clContext workGroupSize

        let gatherRightMatrixData = Gather.run clContext workGroupSize

        let gatherIndices = Gather.run clContext workGroupSize

        let getLeftMatrixValues =
            getLeftMatrixValuesCorrespondinglyToPositionsPattern clContext workGroupSize

        let map2 = ClArray.map2 clContext workGroupSize multiplication

        let getRawPointers = getResultRowPointers clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (leftMatrix: ClMatrix.CSR<'a>) (rightMatrix: ClMatrix.CSR<'b>) ->

            let requiredRawsLengths =
                getRequiredRawsLengths processor leftMatrix.Columns rightMatrix.RowPointers

            // global expanded array length
            let globalLength =
                (prefixSumExclude processor requiredRawsLengths).ToHostAndFree processor

            // since prefix sum include
            // positions in global array for right matrix
            let globalRightMatrixRawsStartPositions = requiredRawsLengths

            // pointers to required raws in right matrix values
            let requiredRightMatrixValuesPointers =
                getRequiredRightMatrixValuesPointers processor leftMatrix.Columns rightMatrix.RowPointers

            // bitmap to distinguish different raws in a general array
            let globalPositions =
                getGlobalPositions processor globalLength globalRightMatrixRawsStartPositions

            // extended pointers to all required right matrix numbers
            let globalRightMatrixValuesPointers =
                getRightMatrixValuesPointers processor globalLength globalPositions globalRightMatrixRawsStartPositions requiredRightMatrixValuesPointers

            // gather all required right matrix values
            let extendedRightMatrixValues =
                clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, globalLength)

            gatherRightMatrixData processor globalRightMatrixValuesPointers rightMatrix.Values extendedRightMatrixValues

            // gather all required right matrix column indices
            let extendedRightMatrixColumns =
                clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, globalLength)

            gatherIndices processor globalRightMatrixValuesPointers rightMatrix.Columns extendedRightMatrixColumns

            // left matrix values correspondingly to right matrix values
            let extendedLeftMatrixValues =
                getLeftMatrixValues processor globalLength globalPositions leftMatrix.Values

            let multiplicationResult  =
                map2 processor DeviceOnly extendedLeftMatrixValues extendedRightMatrixValues

            let rowPointers =
                getRawPointers processor leftMatrix.RowPointers globalRightMatrixRawsStartPositions

            multiplicationResult, extendedRightMatrixColumns, rowPointers
