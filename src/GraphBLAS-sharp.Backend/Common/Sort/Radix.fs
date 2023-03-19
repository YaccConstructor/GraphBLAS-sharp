namespace GraphBLAS.FSharp.Backend.Common.Sort

open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Objects.ClContext
open GraphBLAS.FSharp.Backend.Objects.ClCell
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions

type Indices = ClArray<int>

module Radix =
    let localPrefixSum =
        <@ fun (lid: int) (workGroupSize: int) (array: int []) ->
            let mutable offset = 1

            while offset < workGroupSize do
                let mutable value = array.[lid]

                if lid >= offset then value <- value + array.[lid - offset]

                offset <- offset * 2

                barrierLocal ()
                array.[lid] <- value
                barrierLocal () @>

    let count (clContext: ClContext) workGroupSize mask bitCount =

        let kernel =
            <@ fun (ndRange: Range1D) length (indices: Indices) (workGroupCount: ClCell<int>) (shift: ClCell<int>) (globalOffsets: Indices) (localOffsets: Indices) ->

                let gid = ndRange.GlobalID0
                let lid = ndRange.LocalID0

                let position = (indices.[gid] >>> shift.Value) &&& mask

                if gid < length then printf "position %i for lid = %i" position lid

                let localMask = localArray<int> workGroupSize

                if gid < length then localMask.[lid] <- position else localMask.[lid] <- 0

                if gid < length then
                    printf "local mask value = %i for lid = %i" localMask.[lid] lid

                let localPositions = localArray<int> workGroupSize

                for currentBit in 0 .. bitCount - 1 do
                    let isCurrentPosition = if localMask.[lid] = currentBit then 1 else 0
                    if gid < length then printf "is current position %i for lid = %i, localMask of i = %i, currentBit = %i" isCurrentPosition lid localMask.[lid] currentBit

                    localPositions.[lid] <- if isCurrentPosition = 1 && gid < length then 1 else 0

                    barrierLocal ()

                    (%localPrefixSum) lid workGroupSize localPositions

                    if gid < length && isCurrentPosition = 1 then
                        localOffsets.[gid] <- localPositions.[lid] - 1

                    if lid = 0 then
                        let processedItemsCount = localPositions.[workGroupSize - 1]
                        printf "%i processed items count" processedItemsCount
                        let workGroupNumber = gid / workGroupSize

                        globalOffsets.[position * workGroupCount.Value + workGroupNumber] <- processedItemsCount @>

        let kernel = clContext.Compile kernel
        printfn $"code: {kernel.Code}"

        fun (processor: MailboxProcessor<_>) (indices: Indices) (clWorkGroupCount: ClCell<int>) (shift: ClCell<int>) ->
            let ndRange = Range1D.CreateValid(indices.Length, workGroupSize)

            let workGroupCount = (indices.Length - 1) / workGroupSize + 1

            let globalOffsetsLength = (pown 2 bitCount) * workGroupCount

            let globalOffsets =
                clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, globalOffsetsLength)

            printfn "local offset length = %d" indices.Length

            let localOffsets =
                clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, indices.Length)

            let kernel = kernel.GetKernel()

            processor.Post(Msg.MsgSetArguments(fun () ->
                kernel.KernelFunc ndRange indices.Length indices clWorkGroupCount shift globalOffsets localOffsets))

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            globalOffsets, localOffsets

    let scatter (clContext: ClContext) workGroupSize mask bitCount =

        let kernel =
            <@ fun (ndRange: Range1D) length (keys: Indices) (shift: ClCell<int>) (workGroupCount: ClCell<int>) (globalOffsets: Indices) (localOffsets: Indices) (result: ClArray<int>) ->

                let gid = ndRange.GlobalID0
                let wgId = gid / workGroupSize

                let workGroupCount = workGroupCount.Value

                if gid < length then
                    let slot = (keys.[gid] >>> shift.Value) &&& mask

                    let localOffset = localOffsets.[gid]
                    let globalOffset = globalOffsets.[workGroupCount * slot + wgId]

                    let offset = globalOffset + localOffset

                    result.[offset] <- keys.[gid]
                    shift.Value <- shift.Value <<< bitCount @>

        let kernel = clContext.Compile kernel

        fun (processor: MailboxProcessor<_>) (keys: Indices) (shift: ClCell<int>) (workGroupCount: ClCell<int>) (globalOffset: Indices) (localOffsets: Indices) (result: ClArray<int>) ->

            let ndRange =
                Range1D.CreateValid(keys.Length, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange keys.Length keys shift workGroupCount globalOffset localOffsets result))

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

    let run (clContext: ClContext) workGroupSize =

        let bitCount = 2
        let mask = (pown 2 bitCount) - 1   // TODO()

        let count = count clContext workGroupSize mask bitCount

        let prefixSum = PrefixSum.standardExcludeInplace clContext workGroupSize

        let scatter = scatter clContext workGroupSize mask bitCount

        fun (processor: MailboxProcessor<_>) (keys: Indices) ->

            let secondKeys = clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, keys.Length)
            let workGroupCount = clContext.CreateClCell((keys.Length - 1) / workGroupSize - 1)
            let shift = clContext.CreateClCell 0

            let mutable pair = (keys, secondKeys)
            let swap (x, y) = y, x

            //for i in 0 .. 4 do
            printfn "keys: %A" <| keys.ToHost processor

            let globalOffset, localOffset = count processor (fst pair) workGroupCount shift

            printfn "globalOffset: %A" <| globalOffset.ToHost processor
            printfn "localOffset: %A" <| localOffset.ToHost processor

            (prefixSum processor globalOffset).Free processor

            scatter processor (fst pair) shift workGroupCount globalOffset localOffset (snd pair)

            //pair <- swap pair

            globalOffset.Free processor
            localOffset.Free processor

            keys

