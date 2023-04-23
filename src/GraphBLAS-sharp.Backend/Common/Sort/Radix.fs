namespace GraphBLAS.FSharp.Backend.Common.Sort


open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Objects.ClContext
open GraphBLAS.FSharp.Backend.Objects.ClCell
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions

module Radix =
    // the number of bits considered per iteration
    let defaultBitCount = 4

    let keyBitCount = 32

    let localPrefixSum =
        <@ fun (lid: int) (workGroupSize: int) (array: int []) ->
            let mutable offset = 1

            while offset < workGroupSize do
                barrierLocal ()
                let mutable value = array.[lid]

                if lid >= offset then
                    value <- value + array.[lid - offset]

                offset <- offset * 2

                barrierLocal ()
                array.[lid] <- value @>

    let count (clContext: ClContext) workGroupSize mask =

        let bitCount = mask + 1

        let kernel =
            <@ fun (ndRange: Range1D) length (indices: ClArray<int>) (workGroupCount: ClCell<int>) (shift: ClCell<int>) (globalOffsets: ClArray<int>) (localOffsets: ClArray<int>) ->

                let gid = ndRange.GlobalID0
                let lid = ndRange.LocalID0

                let position = (indices.[gid] >>> shift.Value) &&& mask

                let localMask = localArray<int> workGroupSize

                if gid < length then
                    localMask.[lid] <- position
                else
                    localMask.[lid] <- 0

                let localPositions = localArray<int> workGroupSize

                for currentBit in 0 .. bitCount - 1 do
                    let isCurrentPosition = localMask.[lid] = currentBit

                    if isCurrentPosition && gid < length then
                        localPositions.[lid] <- 1
                    else
                        localPositions.[lid] <- 0

                    barrierLocal ()

                    (%localPrefixSum) lid workGroupSize localPositions

                    barrierLocal ()

                    if gid < length && isCurrentPosition then
                        localOffsets.[gid] <- localPositions.[lid] - 1

                    if lid = 0 then
                        let processedItemsCount = localPositions.[workGroupSize - 1]
                        let wgId = gid / workGroupSize

                        globalOffsets.[workGroupCount.Value * currentBit + wgId] <- processedItemsCount @>

        let kernel = clContext.Compile kernel

        fun (processor: MailboxProcessor<_>) (indices: ClArray<int>) (clWorkGroupCount: ClCell<int>) (shift: ClCell<int>) ->
            let ndRange =
                Range1D.CreateValid(indices.Length, workGroupSize)

            let workGroupCount = (indices.Length - 1) / workGroupSize + 1

            let globalOffsetsLength = bitCount * workGroupCount

            let globalOffsets =
                clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, globalOffsetsLength)

            let localOffsets =
                clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, indices.Length)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.KernelFunc
                            ndRange
                            indices.Length
                            indices
                            clWorkGroupCount
                            shift
                            globalOffsets
                            localOffsets)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            globalOffsets, localOffsets

    let scatter (clContext: ClContext) workGroupSize mask =

        let kernel =
            <@ fun (ndRange: Range1D) length (keys: ClArray<int>) (shift: ClCell<int>) (workGroupCount: ClCell<int>) (globalOffsets: ClArray<int>) (localOffsets: ClArray<int>) (result: ClArray<int>) ->

                let gid = ndRange.GlobalID0
                let wgId = gid / workGroupSize

                let workGroupCount = workGroupCount.Value

                if gid < length then
                    let slot = (keys.[gid] >>> shift.Value) &&& mask

                    let localOffset = localOffsets.[gid]

                    let globalOffset =
                        globalOffsets.[workGroupCount * slot + wgId]

                    let offset = globalOffset + localOffset

                    result.[offset] <- keys.[gid] @>

        let kernel = clContext.Compile kernel

        fun (processor: MailboxProcessor<_>) (keys: ClArray<int>) (shift: ClCell<int>) (workGroupCount: ClCell<int>) (globalOffset: ClArray<int>) (localOffsets: ClArray<int>) (result: ClArray<int>) ->

            let ndRange =
                Range1D.CreateValid(keys.Length, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.KernelFunc ndRange keys.Length keys shift workGroupCount globalOffset localOffsets result)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

    let runKeysOnly (clContext: ClContext) workGroupSize bitCount =
        let copy = ClArray.copy clContext workGroupSize

        let mask = (pown 2 bitCount) - 1

        let count = count clContext workGroupSize mask

        let prefixSum =
            PrefixSum.standardExcludeInplace clContext workGroupSize

        let scatter = scatter clContext workGroupSize mask

        fun (processor: MailboxProcessor<_>) (keys: ClArray<int>) ->
            if keys.Length <= 1 then
                copy processor DeviceOnly keys // TODO(allocation mode)
            else
                let firstKeys = copy processor DeviceOnly keys

                let secondKeys =
                    clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, keys.Length)

                let workGroupCount =
                    clContext.CreateClCell((keys.Length - 1) / workGroupSize + 1)

                let mutable pair = (firstKeys, secondKeys)
                let swap (x, y) = y, x

                let highBound = keyBitCount / bitCount - 1

                for i in 0 .. highBound do
                    let shift = clContext.CreateClCell(bitCount * i)

                    let globalOffset, localOffset =
                        count processor (fst pair) workGroupCount shift

                    (prefixSum processor globalOffset).Free processor

                    scatter processor (fst pair) shift workGroupCount globalOffset localOffset (snd pair)

                    pair <- swap pair

                    globalOffset.Free processor
                    localOffset.Free processor
                    shift.Free processor

                (snd pair).Free processor
                fst pair

    let standardRunKeysOnly clContext workGroupSize =
        runKeysOnly clContext workGroupSize defaultBitCount

    let scatterByKey (clContext: ClContext) workGroupSize mask =

        let kernel =
            <@ fun (ndRange: Range1D) length (keys: ClArray<int>) (values: ClArray<'a>) (shift: ClCell<int>) (workGroupCount: ClCell<int>) (globalOffsets: ClArray<int>) (localOffsets: ClArray<int>) (resultKeys: ClArray<int>) (resultValues: ClArray<'a>) ->

                let gid = ndRange.GlobalID0
                let wgId = gid / workGroupSize

                let workGroupCount = workGroupCount.Value

                if gid < length then
                    let slot = (keys.[gid] >>> shift.Value) &&& mask

                    let localOffset = localOffsets.[gid]

                    let globalOffset =
                        globalOffsets.[workGroupCount * slot + wgId]

                    let offset = globalOffset + localOffset

                    resultKeys.[offset] <- keys.[gid]
                    resultValues.[offset] <- values.[gid] @>

        let kernel = clContext.Compile kernel

        fun (processor: MailboxProcessor<_>) (keys: ClArray<int>) (values: ClArray<'a>) (shift: ClCell<int>) (workGroupCount: ClCell<int>) (globalOffset: ClArray<int>) (localOffsets: ClArray<int>) (resultKeys: ClArray<int>) (resultValues: ClArray<'a>) ->

            let ndRange =
                Range1D.CreateValid(keys.Length, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.KernelFunc
                            ndRange
                            keys.Length
                            keys
                            values
                            shift
                            workGroupCount
                            globalOffset
                            localOffsets
                            resultKeys
                            resultValues)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

    let runByKeys (clContext: ClContext) workGroupSize bitCount =
        let copy = ClArray.copy clContext workGroupSize

        let dataCopy = ClArray.copy clContext workGroupSize

        let mask = (pown 2 bitCount) - 1

        let count = count clContext workGroupSize mask

        let prefixSum =
            PrefixSum.standardExcludeInplace clContext workGroupSize

        let scatterByKey =
            scatterByKey clContext workGroupSize mask

        fun (processor: MailboxProcessor<_>) allocationMode (keys: ClArray<int>) (values: ClArray<'a>) ->
            if values.Length <> keys.Length then
                failwith "Mismatch of key lengths and value. Lengths must be the same"

            if values.Length <= 1 then
                dataCopy processor allocationMode values
            else
                let firstKeys = copy processor DeviceOnly keys

                let secondKeys =
                    clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, keys.Length)

                let firstValues = dataCopy processor DeviceOnly values

                let secondValues =
                    clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, values.Length)

                let workGroupCount =
                    clContext.CreateClCell((keys.Length - 1) / workGroupSize + 1)

                let mutable keysPair = (firstKeys, secondKeys)
                let mutable valuesPair = (firstValues, secondValues)

                let swap (x, y) = y, x
                // compute bound of iterations
                let highBound = keyBitCount / bitCount - 1

                for i in 0 .. highBound do
                    let shift = clContext.CreateClCell(bitCount * i)

                    let currentKeys = fst keysPair
                    let resultKeysBuffer = snd keysPair

                    let currentValues = fst valuesPair
                    let resultValuesBuffer = snd valuesPair

                    let globalOffset, localOffset =
                        count processor currentKeys workGroupCount shift

                    (prefixSum processor globalOffset).Free processor

                    scatterByKey
                        processor
                        currentKeys
                        currentValues
                        shift
                        workGroupCount
                        globalOffset
                        localOffset
                        resultKeysBuffer
                        resultValuesBuffer

                    keysPair <- swap keysPair
                    valuesPair <- swap valuesPair

                    localOffset.Free processor
                    shift.Free processor

                (fst keysPair).Free processor
                (snd keysPair).Free processor
                (snd valuesPair).Free processor

                (fst valuesPair)

    let runByKeysStandard clContext workGroupSize =
        runByKeys clContext workGroupSize defaultBitCount
