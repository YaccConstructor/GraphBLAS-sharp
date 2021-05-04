namespace GraphBLAS.FSharp.Backend.Common

open Brahma.OpenCL
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic

module internal rec BitonicSort =
    let sortInplace (array: 'a[]) = opencl {
        let wgSize = Utils.defaultWorkGroupSize
        let n = array.Length

        do! localBegin array

        let mutable segmentLength = wgSize * 2
        while segmentLength < n do
            segmentLength <- segmentLength <<< 1

            do! globalStep array segmentLength true

            let mutable i = segmentLength / 2
            while i > wgSize * 2 do
                do! globalStep array i false
                i <- i >>> 1

            do! localEnd array
    }

    let private localBegin (array: 'a[]) = opencl {
        let wgSize = Utils.defaultWorkGroupSize
        let n = array.Length
        let processedSize = wgSize * 2

        let kernel =
            <@
                fun (range: _1D)
                    (array: 'a[]) ->

                    let lid = range.LocalID0
                    let gid = range.GlobalID0
                    let groupId = gid / wgSize

                    // 1 рабочя группа обрабатывает 2 * wgSize элементов
                    let localBuffer = localArray<'a> processedSize
                    let mutable readIdx = processedSize * groupId + lid

                    // копируем элементы из глобальной памяти в локальную
                    if readIdx < n then
                        localBuffer.[lid] <- array.[readIdx]
                    // else
                    //     localBuffer.[lid] <- -1

                    readIdx <- readIdx + wgSize

                    if readIdx < n then
                        localBuffer.[lid + wgSize] <- array.[readIdx]
                    // else
                    //     localBuffer.[lid + wgSize] <- -1

                    barrier ()

                    let mutable segmentLength = 1
                    while segmentLength < processedSize do
                        segmentLength <- segmentLength <<< 1
                        let localLineId = lid % (segmentLength / 2)
                        let localTwinId = segmentLength - localLineId - 1
                        let groupLineId = lid / (segmentLength / 2)
                        let lineId = segmentLength * groupLineId + localLineId
                        let twinId = segmentLength * groupLineId + localTwinId

                        if localBuffer.[lineId] > localBuffer.[twinId] then
                            let tmp = localBuffer.[lineId]
                            localBuffer.[lineId] <- localBuffer.[twinId]
                            localBuffer.[twinId] <- tmp

                        barrier ()

                        let mutable j = segmentLength / 2
                        while j > 1 do
                            let localLineId = lid % (j / 2)
                            let localTwinId = localLineId + (j / 2)
                            let groupLineId = lid / (segmentLength / 2)
                            let lineId = j * groupLineId + localLineId
                            let twinId = j * groupLineId + localTwinId

                            if localBuffer.[lineId] > localBuffer.[twinId] then
                                let tmp = localBuffer.[lineId]
                                localBuffer.[lineId] <- localBuffer.[twinId]
                                localBuffer.[twinId] <- tmp

                            barrier ()

                            j <- j >>> 1

                    let mutable writeIdx = processedSize * groupId + lid
                    if writeIdx < n then
                        array.[writeIdx] <- localBuffer.[lid]

                    writeIdx <- writeIdx + wgSize
                    if writeIdx < n then
                        array.[writeIdx] <- localBuffer.[lid + wgSize]
            @>

        do! RunCommand kernel <| fun kernelPrepare ->
            kernelPrepare
            <| _1D(Utils.floorToPower2 n |> Utils.getDefaultGlobalSize, wgSize)
            <| array
    }

    let private globalStep (array: 'a[]) (segmentLength: int) (mirror: bool) = opencl {
        let wgSize = Utils.defaultWorkGroupSize
        let n = array.Length

        let kernel =
            <@
                fun (range: _1D)
                    (array: 'a[]) ->

                    let gid = range.GlobalID0
                    let groupId = gid / wgSize

                    let localLineId = gid % (segmentLength / 2)
                    let mutable localTwinId = 0
                    if mirror then
                        localTwinId <- segmentLength - localLineId - 1
                    else
                        localTwinId <- localLineId + (segmentLength / 2)

                    let groupLineId = gid / (segmentLength / 2)
                    let lineId = segmentLength * groupLineId + localLineId
                    let twinId = segmentLength * groupLineId + localTwinId

                    if twinId < n && array.[lineId] > array.[twinId] then
                        let tmp = array.[lineId]
                        array.[lineId] <- array.[twinId]
                        array.[twinId] <- tmp
            @>

        do! RunCommand kernel <| fun kernelPrepare ->
            kernelPrepare
            <| _1D(Utils.floorToPower2 n |> Utils.getDefaultGlobalSize, wgSize)
            <| array
    }

    let private localEnd (array: 'a[]) = opencl {
        let wgSize = Utils.defaultWorkGroupSize
        let n = array.Length
        let processedSize = wgSize * 2

        let kernel =
            <@
                fun (range: _1D)
                    (array: 'a[]) ->

                    let lid = range.LocalID0
                    let gid = range.GlobalID0
                    let groupId = gid / wgSize

                    // 1 рабочя группа обрабатывает 2 * wgSize элементов
                    let localBuffer = localArray<'a> processedSize
                    let mutable readIdx = processedSize * groupId + lid

                    // копируем элементы из глобальной памяти в локальную
                    if readIdx < n then
                        localBuffer.[lid] <- array.[readIdx]
                    // else
                    //     localBuffer.[lid] <- -1

                    readIdx <- readIdx + wgSize

                    if readIdx < n then
                        localBuffer.[lid + wgSize] <- array.[readIdx]
                    // else
                    //     localBuffer.[lid + wgSize] <- -1

                    barrier ()

                    let mutable segmentLength = processedSize
                    let mutable j = segmentLength
                    while j > 1 do
                        let localLineId = lid % (j / 2)
                        let localTwinId = localLineId + (j / 2)
                        let groupLineId = lid / (j / 2)
                        let lineId = j * groupLineId + localLineId
                        let twinId = j * groupLineId + localTwinId

                        if localBuffer.[lineId] > localBuffer.[twinId] then
                            let tmp = localBuffer.[lineId]
                            localBuffer.[lineId] <- localBuffer.[twinId]
                            localBuffer.[twinId] <- tmp

                        barrier ()

                        j <- j >>> 1

                    let mutable writeIdx = processedSize * groupId + lid
                    if writeIdx < n then
                        array.[writeIdx] <- localBuffer.[lid]

                    writeIdx <- writeIdx + wgSize
                    if writeIdx < n then
                        array.[writeIdx] <- localBuffer.[lid + wgSize]
            @>

        do! RunCommand kernel <| fun kernelPrepare ->
            kernelPrepare
            <| _1D(Utils.floorToPower2 n |> Utils.getDefaultGlobalSize, wgSize)
            <| array
    }
