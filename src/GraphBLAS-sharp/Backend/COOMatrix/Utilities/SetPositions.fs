namespace GraphBLAS.FSharp.Backend.COOMatrix.Utilities

open Brahma.FSharp.OpenCL
open GraphBLAS.FSharp.Backend.Common

[<AutoOpen>]
module internal SetPositions =
    let setPositions<'a when 'a: struct> (clContext: ClContext) workGroupSize =

        let setPositions =
            <@ fun (ndRange: Range1D) prefixSumArrayLength (allRowsBuffer: ClArray<int>) (allColumnsBuffer: ClArray<int>) (allValuesBuffer: ClArray<'a>) (prefixSumArrayBuffer: ClArray<int>) (resultRowsBuffer: ClArray<int>) (resultColumnsBuffer: ClArray<int>) (resultValuesBuffer: ClArray<'a>) ->

                let i = ndRange.GlobalID0

                if i = prefixSumArrayLength - 1
                   || i < prefixSumArrayLength
                      && prefixSumArrayBuffer.[i]
                         <> prefixSumArrayBuffer.[i + 1] then
                    let index = prefixSumArrayBuffer.[i]

                    resultRowsBuffer.[index] <- allRowsBuffer.[i]
                    resultColumnsBuffer.[index] <- allColumnsBuffer.[i]
                    resultValuesBuffer.[index] <- allValuesBuffer.[i] @>

        let kernel = clContext.CreateClKernel(setPositions)

        let sum =
            GraphBLAS.FSharp.Backend.ClArray.prefixSumExcludeInplace clContext

        let resultLength = Array.zeroCreate 1

        fun (processor: MailboxProcessor<_>) (allRows: ClArray<int>) (allColumns: ClArray<int>) (allValues: ClArray<'a>) (positions: ClArray<int>) ->
            let prefixSumArrayLength = positions.Length

            let resultLengthGpu = clContext.CreateClArray<_>(1)

            let _, r =
                sum processor workGroupSize positions resultLengthGpu

            let resultLength =
                let res =
                    processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(r, resultLength, ch))

                processor.Post(Msg.CreateFreeMsg<_>(r))

                res.[0]

            let resultRows =
                clContext.CreateClArray(
                    resultLength,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.WriteOnly
                )

            let resultColumns =
                clContext.CreateClArray(
                    resultLength,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.WriteOnly
                )

            let resultValues =
                clContext.CreateClArray(
                    resultLength,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.WriteOnly
                )

            let ndRange =
                Range1D(Utils.getDefaultGlobalSize positions.Length, workGroupSize)

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.SetArguments
                            ndRange
                            prefixSumArrayLength
                            allRows
                            allColumns
                            allValues
                            positions
                            resultRows
                            resultColumns
                            resultValues)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            resultRows, resultColumns, resultValues, resultLength
