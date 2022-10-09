namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp
open GraphBLAS.FSharp.Backend
open Microsoft.FSharp.Control

module DenseVector =
    let zeroCreate<'a when 'a : struct> (size: int) : Vector<'a> =
            DenseVector.FromArray (Array.zeroCreate size, fun _ -> true)
            |> VectorDense

    let ofList (elements: (int * 'a) list) (isZero: 'a -> bool) : Vector<'a> =
        let _, values =
            elements
            |> Array.ofList
            |> Array.sortBy fst
            |> Array.unzip

        (values, isZero)
        |> DenseVector.FromArray
        |> VectorDense

    let getBitmap (clContext: ClContext) (workGroupSize: int) =
        let getBitmap =
            <@
                fun (range: Range1D) (vector: ClArray<'a option>) (vectorSize: int) (bitmap: ClArray<int>) ->
                    let gid = range.GlobalID0

                    if gid < vectorSize then
                        match vector[gid] with
                        | None -> bitmap[gid] <- 0
                        | _ -> ()
            @>

        let kernel = clContext.Compile(getBitmap)

        fun (processor: MailboxProcessor<_>) (vector: ClDenseVector<'a>) ->
            let vectorSize = vector.Size

            let bitmap = Array.create vectorSize 1

            let clBitmap =
                clContext.CreateClArray(
                    bitmap,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.WriteOnly,
                    allocationMode = AllocationMode.Default
                )

            let ndRange =
                Range1D.CreateValid(vectorSize, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.KernelFunc
                            ndRange
                            vector
                            vectorSize
                            clBitmap)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            clBitmap

    let unzip (clContext: ClContext) (workGroupSize: int) =
        let getMask =
            <@
                fun (ndRange: Range1D) vectorLength (bitmap: ClArray<int>) (vector: ClArray<'a option>) (prefixSumArray: ClArray<int>) (valueArray: ClArray<'a option>) (indicesArray: ClArray<int> ) ->
                    let gid = ndRange.GlobalID0

                    if gid < vectorLength then
                        if bitmap[gid]  = 1 then
                            let resultIndex = prefixSumArray[gid]

                            valueArray[resultIndex] <- vector[gid]
                            indicesArray[resultIndex] <- gid
            @>

        let kernel = clContext.Compile(getMask)

        let sum = ClArray.prefixSumExcludeInplace clContext workGroupSize

        let resultLength = Array.zeroCreate 1

        let getBitmap = getBitmap clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (vector: ClDenseVector<'a>) ->
            let positions = getBitmap processor vector

            let prefixSumArrayLength = positions.Length

            let resultLengthGpu = clContext.CreateClCell 0

            let prefixSumArray, r = sum processor positions resultLengthGpu

            let resultLength =
                let res =
                    processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(r, resultLength, ch))

                processor.Post(Msg.CreateFreeMsg<_>(r))

                res.[0]

            let ndRange =
                Range1D.CreateValid(positions.Length, workGroupSize)

            let kernel = kernel.GetKernel()

            let resultValues =
                clContext.CreateClArray(
                    resultLength,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.WriteOnly,
                    allocationMode = AllocationMode.Default
                )

            let resultIndices =
                clContext.CreateClArray<int>(
                    resultLength,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.WriteOnly,
                    allocationMode = AllocationMode.Default
                )

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.KernelFunc
                            ndRange
                            prefixSumArrayLength
                            positions
                            vector
                            prefixSumArray
                            resultValues
                            resultIndices)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            resultValues, resultIndices

    let mask (clContext: ClContext) (workGroupSize: int) =
        let unzip = unzip clContext workGroupSize
        let toOptionArray = ClArray.toOptionArray clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (vector: ClDenseVector<'a>) ->
            let _, indices = unzip processor vector

            let optionIndices = toOptionArray processor indices

            optionIndices :?> ClDenseVector<int>
