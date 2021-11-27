namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp.OpenCL
open GraphBLAS.FSharp.Backend

module internal rec Setup =
    let run (clContext: ClContext) workGroupSize =

        let sum =
            ClArray.prefixSumExcludeInplace clContext workGroupSize

        let resultNNZ = Array.zeroCreate 1

        fun (processor: MailboxProcessor<_>) (matrixLeft: CSRMatrix<'a>) (matrixRight: CSRMatrix<'a>) ->

            let resultRowPointers = (getRowLengths clContext workGroupSize) processor matrixLeft matrixRight

            let resultNNZGpu = clContext.CreateClArray<_>(1)

            let _, r = sum processor resultRowPointers resultNNZGpu

            let resultNNZ =
                let res =
                    processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(r, resultNNZ, ch))

                processor.Post(Msg.CreateFreeMsg<_>(r))

                res.[0]

            let resultColumns =
                clContext.CreateClArray(
                    resultNNZ,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.WriteOnly
                )

            let resultValues =
                clContext.CreateClArray(
                    resultNNZ,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.WriteOnly
                )

            {
                RowCount = matrixLeft.RowCount
                ColumnCount = matrixRight.ColumnCount
                RowPointers = resultRowPointers
                Columns = resultColumns
                Values = resultValues
            }

    let private getRowLengths (clContext: ClContext) workGroupSize =

        let getRowLengths =
            <@
                fun (ndRange: Range1D)
                    (firstRowPointersBuffer: ClArray<int>)
                    (firstColumnsBuffer: ClArray<int>)
                    (secondRowPointersBuffer: ClArray<int>)
                    (resultRowLengthsBuffer: ClArray<int>) ->

                    let i = ndRange.GlobalID0
                    let localID = ndRange.LocalID0
                    let workGroupNumber = i / workGroupSize

                    let rowLength = localArray workGroupSize
                    rowLength.[localID] <- 0
                    barrier ()

                    let mutable j = localID + firstRowPointersBuffer.[workGroupNumber]
                    let endIndex = firstRowPointersBuffer.[workGroupNumber + 1]
                    while j < endIndex do
                        let rowIdx = firstColumnsBuffer.[j]
                        rowLength.[localID] <- rowLength.[localID] + secondRowPointersBuffer.[rowIdx + 1] - secondRowPointersBuffer.[rowIdx]
                        j <- j + workGroupSize

                    barrier ()

                    let mutable step = workGroupSize / 2
                    while step > 0 do
                        if localID < step then rowLength.[localID] <- rowLength.[localID] + rowLength.[localID + step]
                        step <- step >>> 1
                        barrier ()

                    if localID = 0 then resultRowLengthsBuffer.[workGroupNumber] <- rowLength.[0]
            @>

        fun (processor: MailboxProcessor<_>) (matrixLeft: CSRMatrix<'a>) (matrixRight: CSRMatrix<'a>) ->
            let resultRowLengths =
                    clContext.CreateClArray(
                        matrixLeft.RowCount + 1,
                        hostAccessMode = HostAccessMode.NotAccessible,
                        deviceAccessMode = DeviceAccessMode.WriteOnly
                    )

            let kernel = clContext.CreateClKernel(getRowLengths)

            let ndRange = Range1D(workGroupSize * matrixLeft.RowCount, workGroupSize)

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.ArgumentsSetter
                            ndRange
                            matrixLeft.RowPointers
                            matrixLeft.Columns
                            matrixRight.RowPointers
                            resultRowLengths)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            resultRowLengths

