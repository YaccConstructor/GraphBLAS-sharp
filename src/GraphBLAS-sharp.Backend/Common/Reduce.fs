namespace GraphBLAS.FSharp.Backend.Common

open Brahma.FSharp
open Microsoft.FSharp.Control
open Microsoft.FSharp.Quotations

module Reduce =
    let private scan<'a when 'a: struct> (clContext: ClContext) (workGroupSize: int) (opAdd: Expr<'a -> 'a -> 'a>) =

        let scan =
            <@ fun (ndRange: Range1D) length (inputArray: ClArray<'a>) (resultArray: ClArray<'a>) ->

                let gid = ndRange.GlobalID0
                let lid = ndRange.LocalID0

                let localValues = localArray<'a> workGroupSize

                if gid < length then
                    localValues.[lid] <- inputArray.[gid]

                barrierLocal ()

                let mutable step = 2

                if gid < length then
                    while step <= workGroupSize do
                        if (gid + workGroupSize / step) < length
                           && lid < workGroupSize / step then
                            let firstValue = localValues.[lid]
                            let secondValue = localValues.[lid + workGroupSize / step]

                            localValues.[lid] <- (%opAdd) firstValue secondValue

                        step <- step <<< 1

                        barrierLocal ()

                    if lid = 0 then
                        resultArray.[gid / workGroupSize] <- localValues.[0] @>

        let kernel = clContext.Compile(scan)

        fun (processor: MailboxProcessor<_>) (valuesArray: ClArray<'a>) valuesLength (resultArray: ClArray<'a>) ->

            let ndRange =
                Range1D.CreateValid(valuesArray.Length, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange valuesLength valuesArray resultArray)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

    let run<'a when 'a: struct> (clContext: ClContext) (workGroupSize: int) (opAdd: Expr<'a -> 'a -> 'a>) =

        let scan = scan clContext workGroupSize opAdd

        fun (processor: MailboxProcessor<_>) (inputArray: ClArray<'a>) ->

            let scan = scan processor

            let firstLength =
                (inputArray.Length - 1) / workGroupSize + 1

            let firstVerticesArray =
                clContext.CreateClArray(
                    firstLength,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.ReadWrite,
                    allocationMode = AllocationMode.Default
                )

            let secondLength = (firstLength - 1) / workGroupSize + 1

            let secondVerticesArray =
                clContext.CreateClArray(
                    secondLength,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.ReadWrite,
                    allocationMode = AllocationMode.Default
                )

            let mutable verticesArrays = firstVerticesArray, secondVerticesArray
            let swap (a, b) = (b, a)

            scan inputArray inputArray.Length (fst verticesArrays)

            let mutable verticesLength = firstLength

            while verticesLength > workGroupSize do
                let fstVertices = fst verticesArrays
                let sndVertices = snd verticesArrays

                scan fstVertices verticesLength sndVertices

                verticesArrays <- swap verticesArrays
                verticesLength <- (verticesLength - 1) / workGroupSize + 1

            let fstVertices = fst verticesArrays

            let result =
                clContext.CreateClArray(
                    1,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.ReadWrite,
                    allocationMode = AllocationMode.Default
                )

            scan fstVertices verticesLength result

            processor.Post(Msg.CreateFreeMsg(firstVerticesArray))
            processor.Post(Msg.CreateFreeMsg(secondVerticesArray))

            result
