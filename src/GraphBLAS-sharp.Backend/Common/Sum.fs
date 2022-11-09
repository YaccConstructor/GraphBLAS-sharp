namespace GraphBLAS.FSharp.Backend.Common

open Brahma.FSharp
open GraphBLAS.FSharp.Backend
open Microsoft.FSharp.Quotations

module internal Sum =

    let private scan
        (clContext: ClContext)
        (workGroupSize: int)
        (opAdd: Expr<'a -> 'a -> 'a>)
        zero
        =

        let subSum = SubSum.sequentialSum opAdd

        let scan =
            <@
                fun (ndRange: Range1D) length (inputArray: ClArray<'a>) (resultArray: ClArray<'a>) ->

                    let gid = ndRange.GlobalID0
                    let lid = ndRange.LocalID0

                    let localValues = localArray<'a> workGroupSize

                    if gid < length then
                        localValues[lid] <- inputArray[gid]
                    else
                        localValues[lid] <- zero

                    barrierLocal ()

                    (%subSum) workGroupSize lid localValues

                    resultArray[gid / workGroupSize] <- localValues[0]
            @>

        let kernel = clContext.Compile(scan)

        fun (processor: MailboxProcessor<_>) (valuesArray: ClArray<'a>) valuesLength (resultArray: ClArray<'a>) ->
            let ndRange = Range1D.CreateValid(valuesArray.Length, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments(
                    fun () ->
                        kernel.KernelFunc
                            ndRange
                            valuesLength
                            valuesArray
                            resultArray)
                )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            ()

    let private scanToCell
        (clContext: ClContext)
        (workGroupSize: int)
        (opAdd: Expr<'a -> 'a -> 'a>)
        zero
        =

        let subSum = SubSum.sequentialSum opAdd

        let scan =
            <@
                fun (ndRange: Range1D) length (inputArray: ClArray<'a>) (resultCell: ClCell<'a>) ->

                    let gid = ndRange.GlobalID0
                    let lid = ndRange.LocalID0

                    let localValues = localArray<'a> workGroupSize

                    if gid < length then
                        localValues[lid] <- inputArray[gid]
                    else
                        localValues[lid] <- zero

                    barrierLocal ()

                    (%subSum) workGroupSize lid localValues

                    resultCell.Value <- localValues[0]
            @>

        let kernel = clContext.Compile(scan)

        fun (processor: MailboxProcessor<_>) (valuesArray: ClArray<'a>)  valuesLength ->

            let ndRange = Range1D.CreateValid(valuesArray.Length, workGroupSize)

            let resultCell = clContext.CreateClCell zero

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments(
                    fun () ->
                        kernel.KernelFunc
                            ndRange
                            valuesLength
                            valuesArray
                            resultCell)
                )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            resultCell

    let run
        (clContext: ClContext)
        (workGroupSize: int)
        (opAdd: Expr<'a -> 'a -> 'a>)
        (zero: 'a)
        =

        let scan = scan clContext workGroupSize opAdd zero
        let scanToCell = scanToCell clContext workGroupSize opAdd zero

        fun (processor: MailboxProcessor<_>) (inputArray: ClArray<'a>) ->

            let scan = scan processor

            let firstLength = (inputArray.Length - 1) / workGroupSize + 1

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
            let result = scanToCell processor fstVertices verticesLength

            processor.Post(Msg.CreateFreeMsg(firstVerticesArray))
            processor.Post(Msg.CreateFreeMsg(secondVerticesArray))

            result
