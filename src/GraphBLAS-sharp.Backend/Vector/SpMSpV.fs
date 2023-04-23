namespace GraphBLAS.FSharp.Backend.Vector

open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Common
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Objects.ClVector
open GraphBLAS.FSharp.Backend.Objects.ClContext
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions
open GraphBLAS.FSharp.Backend.Objects.ClCell

module SpMSpV =

    //For v in vectorIndices collect R[v] and R[v + 1]
    let private collectRows (clContext: ClContext) workGroupSize =

        let collectRows =
            <@ fun (ndRange: Range1D) inputSize (vectorIndices: ClArray<int>) (rowOffsets: ClArray<int>) (resultArray: ClArray<int>) ->

                let i = ndRange.GlobalID0

                //resultArray is twice vector size
                if i < (inputSize * 2) then
                    let columnIndex = vectorIndices.[i / 2]

                    if i % 2 = 0 then
                        resultArray.[i] <- rowOffsets.[columnIndex]
                    else
                        resultArray.[i] <- rowOffsets.[columnIndex + 1]
                elif i = inputSize * 2 then
                    resultArray.[i] <- 0 @>


        let collectRows = clContext.Compile collectRows

        fun (queue: MailboxProcessor<_>) size (vectorIndices: ClArray<int>) (rowOffsets: ClArray<int>) ->

            let ndRange =
                Range1D.CreateValid(size * 2 + 1, workGroupSize)

            // Last element will contain length of array for gather
            let resultRows =
                clContext.CreateClArrayWithSpecificAllocationMode<int>(DeviceOnly, size * 2 + 1)

            let collectRows = collectRows.GetKernel()

            queue.Post(
                Msg.MsgSetArguments(fun () -> collectRows.KernelFunc ndRange size vectorIndices rowOffsets resultRows)
            )

            queue.Post(Msg.CreateRunMsg<_, _>(collectRows))

            resultRows

    //For above array compute result offsets
    let private computeOffsetsInplace (clContext: ClContext) workGroupSize =

        let prepareOffsets =
            <@ fun (ndRange: Range1D) inputSize (inputArray: ClArray<int>) ->

                let i = ndRange.GlobalID0

                if i < inputSize && i % 2 = 0 then
                    inputArray.[i + 1] <- inputArray.[i + 1] - inputArray.[i]
                    inputArray.[i] <- 0 @>

        let sum =
            PrefixSum.standardExcludeInplace clContext workGroupSize

        let prepareOffsets = clContext.Compile prepareOffsets

        fun (queue: MailboxProcessor<_>) size (input: ClArray<int>) ->

            let ndRange = Range1D.CreateValid(size, workGroupSize)

            let prepareOffsets = prepareOffsets.GetKernel()

            queue.Post(Msg.MsgSetArguments(fun () -> prepareOffsets.KernelFunc ndRange size input))

            queue.Post(Msg.CreateRunMsg<_, _>(prepareOffsets))

            let resSize = (sum queue input).ToHostAndFree queue

            resSize

    //Gather rows from the matrix that will be used in multiplication
    let private gather (clContext: ClContext) workGroupSize =

        let gather =
            <@ fun (ndRange: Range1D) vectorNNZ (rowOffsets: ClArray<int>) (matrixRowPointers: ClArray<int>) (matrixColumns: ClArray<int>) (matrixValues: ClArray<'a>) (vectorIndices: ClArray<int>) (resultIndicesArray: ClArray<int>) (resultValuesArray: ClArray<'a>) ->

                //Serial number of row to gather
                let row = ndRange.GlobalID0

                if row < vectorNNZ then
                    let offsetIndex = row * 2 + 1
                    let rowOffset = rowOffsets.[offsetIndex]

                    //vectorIndices.[row] --- actual number of row in matrix
                    let matrixIndexOffset = matrixRowPointers.[vectorIndices.[row]]

                    if rowOffset <> rowOffsets.[offsetIndex + 1] then
                        let rowSize = rowOffsets.[offsetIndex + 1] - rowOffset

                        for i in 0 .. rowSize - 1 do
                            resultIndicesArray.[i + rowOffset] <- matrixColumns.[matrixIndexOffset + i]
                            resultValuesArray.[i + rowOffset] <- matrixValues.[matrixIndexOffset + i] @>

        let collectRows = collectRows clContext workGroupSize

        let computeOffsetsInplace =
            computeOffsetsInplace clContext workGroupSize

        let gather = clContext.Compile gather

        fun (queue: MailboxProcessor<_>) (matrix: ClMatrix.CSR<'a>) (vector: ClVector.Sparse<'b>) ->

            //Collect R[v] and R[v + 1] for each v in vector
            let collectedRows =
                collectRows queue vector.NNZ vector.Indices matrix.RowPointers

            //Place R[v + 1] - R[v] in previous array and do prefix sum, computing offsets for gather array
            let gatherArraySize =
                computeOffsetsInplace queue (vector.NNZ * 2 + 1) collectedRows

            let ndRange =
                Range1D.CreateValid(vector.NNZ, workGroupSize)

            let gather = gather.GetKernel()

            let resultIndices =
                clContext.CreateClArrayWithSpecificAllocationMode<int>(
                    DeviceOnly,
                    if gatherArraySize > 0 then
                        gatherArraySize
                    else
                        1
                )

            let resultValues =
                clContext.CreateClArrayWithSpecificAllocationMode<'a>(
                    DeviceOnly,
                    if gatherArraySize > 0 then
                        gatherArraySize
                    else
                        1
                )

            if gatherArraySize > 0 then
                queue.Post(
                    Msg.MsgSetArguments
                        (fun () ->
                            gather.KernelFunc
                                ndRange
                                vector.NNZ
                                collectedRows
                                matrix.RowPointers
                                matrix.Columns
                                matrix.Values
                                vector.Indices
                                resultIndices
                                resultValues)
                )

                queue.Post(Msg.CreateRunMsg<_, _>(gather))

            collectedRows.Free queue

            resultIndices, resultValues, gatherArraySize

    let run
        (clContext: ClContext)
        (add: Expr<'c option -> 'c option -> 'c option>)
        (mul: Expr<'a option -> 'b option -> 'c option>)
        workGroupSize
        =

        let gather = gather clContext workGroupSize

        let sort =
            Sort.Radix.standardRunKeysOnly clContext workGroupSize

        let removeDuplicates =
            ClArray.removeDuplications clContext workGroupSize

        let create = ClArray.create clContext workGroupSize

        fun (queue: MailboxProcessor<_>) (matrix: ClMatrix.CSR<'a>) (vector: ClVector.Sparse<'b>) ->

            let gatherIndices, gatherValues, gatherLength = gather queue matrix vector

            gatherValues.Dispose queue

            if gatherLength <= 0 then
                { Context = clContext
                  Indices = gatherIndices
                  Values = clContext.CreateClArray [| false |]
                  Size = 1 }
            else
                let sortedIndices = sort queue gatherIndices

                let resultIndices = removeDuplicates queue sortedIndices

                gatherIndices.Dispose queue
                sortedIndices.Dispose queue

                { Context = clContext
                  Indices = resultIndices
                  Values = create queue DeviceOnly resultIndices.Length true
                  Size = matrix.ColumnCount }
