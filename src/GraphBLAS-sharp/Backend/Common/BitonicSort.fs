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


    let sortInplace2 (array: uint64[]) (values: 'a[]) = opencl {
        if array.Length = 0 then
            return ()
        else
            let wgSize = Utils.defaultWorkGroupSize
            let n = array.Length

            do! localBegin2 array values

            let mutable segmentLength = wgSize * 2
            while segmentLength < n do
                segmentLength <- segmentLength <<< 1

                do! globalStep2 array values segmentLength true

                let mutable i = segmentLength / 2
                while i > wgSize * 2 do
                    do! globalStep2 array values i false
                    i <- i >>> 1

                do! localEnd2 array values
    }

    let private localBegin2 (array: uint64[]) (values: 'a[]) = opencl {
        let wgSize = Utils.defaultWorkGroupSize
        let n = array.Length
        let processedSize = wgSize * 2
        let maxx = System.UInt64.MaxValue

        let kernel =
            <@
                fun (range: _1D)
                    (array: uint64[])
                    (values: 'a[]) ->

                    let lid = range.LocalID0
                    let gid = range.GlobalID0
                    let groupId = gid / wgSize

                    // 1 рабочя группа обрабатывает 2 * wgSize элементов
                    let localBufferI = localArray<uint64> processedSize
                    let localBufferV = localArray<'a> processedSize

                    let mutable readIdx = processedSize * groupId + lid

                    // копируем элементы из глобальной памяти в локальную
                    if readIdx < n then
                        localBufferI.[lid] <- array.[readIdx]
                    else
                        localBufferI.[lid] <- maxx

                    if readIdx < n then
                        localBufferV.[lid] <- values.[readIdx]

                    readIdx <- readIdx + wgSize

                    if readIdx < n then
                        localBufferI.[lid + wgSize] <- array.[readIdx]
                    else
                        localBufferI.[lid + wgSize] <- maxx

                    if readIdx < n then
                        localBufferV.[lid + wgSize] <- values.[readIdx]

                    barrier ()

                    let mutable segmentLength = 1
                    while segmentLength < processedSize do
                        segmentLength <- segmentLength <<< 1
                        let localLineId = lid % (segmentLength / 2)
                        let localTwinId = segmentLength - localLineId - 1
                        let groupLineId = lid / (segmentLength / 2)
                        let lineId = segmentLength * groupLineId + localLineId
                        let twinId = segmentLength * groupLineId + localTwinId

                        if localBufferI.[lineId] > localBufferI.[twinId] then
                            let tmpI = localBufferI.[lineId]
                            localBufferI.[lineId] <- localBufferI.[twinId]
                            localBufferI.[twinId] <- tmpI

                            let tmpV = localBufferV.[lineId]
                            localBufferV.[lineId] <- localBufferV.[twinId]
                            localBufferV.[twinId] <- tmpV

                        barrier ()

                        let mutable j = segmentLength / 2
                        while j > 1 do
                            let localLineId = lid % (j / 2)
                            let localTwinId = localLineId + (j / 2)
                            let groupLineId = lid / (j / 2)
                            let lineId = j * groupLineId + localLineId
                            let twinId = j * groupLineId + localTwinId

                            if localBufferI.[lineId] > localBufferI.[twinId] then
                                let tmpI = localBufferI.[lineId]
                                localBufferI.[lineId] <- localBufferI.[twinId]
                                localBufferI.[twinId] <- tmpI

                                let tmpV = localBufferV.[lineId]
                                localBufferV.[lineId] <- localBufferV.[twinId]
                                localBufferV.[twinId] <- tmpV

                            barrier ()

                            j <- j >>> 1

                    let mutable writeIdx = processedSize * groupId + lid
                    if writeIdx < n then
                        array.[writeIdx] <- localBufferI.[lid]
                        values.[writeIdx] <- localBufferV.[lid]

                    writeIdx <- writeIdx + wgSize
                    if writeIdx < n then
                        array.[writeIdx] <- localBufferI.[lid + wgSize]
                        values.[writeIdx] <- localBufferV.[lid + wgSize]
            @>

        do! RunCommand kernel <| fun kernelPrepare ->
            kernelPrepare
            <| _1D(Utils.floorToPower2 n |> Utils.getDefaultGlobalSize, wgSize)
            <| array
            <| values
    }

    let private globalStep2 (array: uint64[]) (values: 'a[]) (segmentLength: int) (mirror: bool) = opencl {
        let wgSize = Utils.defaultWorkGroupSize
        let n = array.Length

        let kernel =
            <@
                fun (range: _1D)
                    (array: uint64[])
                    (values: 'a[]) ->

                    let gid = range.GlobalID0

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

                        let tmpV = values.[lineId]
                        values.[lineId] <- values.[twinId]
                        values.[twinId] <- tmpV
            @>

        do! RunCommand kernel <| fun kernelPrepare ->
            kernelPrepare
            <| _1D(Utils.floorToPower2 n |> Utils.getDefaultGlobalSize, wgSize)
            <| array
            <| values
    }

    let private localEnd2 (array: uint64[]) (values: 'a[]) = opencl {
        let wgSize = Utils.defaultWorkGroupSize
        let n = array.Length
        let processedSize = wgSize * 2
        let maxx = System.UInt64.MaxValue

        let kernel =
            <@
                fun (range: _1D)
                    (array: uint64[])
                    (values: 'a[]) ->

                    let lid = range.LocalID0
                    let gid = range.GlobalID0
                    let groupId = gid / wgSize

                    // 1 рабочя группа обрабатывает 2 * wgSize элементов
                    let localBufferI = localArray<uint64> processedSize
                    let localBufferV = localArray<'a> processedSize

                    let mutable readIdx = processedSize * groupId + lid

                    // копируем элементы из глобальной памяти в локальную
                    if readIdx < n then
                        localBufferI.[lid] <- array.[readIdx]
                    else
                        localBufferI.[lid] <- maxx

                    if readIdx < n then
                        localBufferV.[lid] <- values.[readIdx]

                    readIdx <- readIdx + wgSize

                    if readIdx < n then
                        localBufferI.[lid + wgSize] <- array.[readIdx]
                    else
                        localBufferI.[lid + wgSize] <- maxx

                    if readIdx < n then
                        localBufferV.[lid + wgSize] <- values.[readIdx]

                    barrier ()

                    let mutable segmentLength = processedSize
                    let mutable j = segmentLength
                    while j > 1 do
                        let localLineId = lid % (j / 2)
                        let localTwinId = localLineId + (j / 2)
                        let groupLineId = lid / (j / 2)
                        let lineId = j * groupLineId + localLineId
                        let twinId = j * groupLineId + localTwinId

                        if localBufferI.[lineId] > localBufferI.[twinId] then
                            let tmpI = localBufferI.[lineId]
                            localBufferI.[lineId] <- localBufferI.[twinId]
                            localBufferI.[twinId] <- tmpI

                            let tmpV = localBufferV.[lineId]
                            localBufferV.[lineId] <- localBufferV.[twinId]
                            localBufferV.[twinId] <- tmpV

                        barrier ()

                        j <- j >>> 1

                    let mutable writeIdx = processedSize * groupId + lid
                    if writeIdx < n then
                        array.[writeIdx] <- localBufferI.[lid]
                        values.[writeIdx] <- localBufferV.[lid]

                    writeIdx <- writeIdx + wgSize
                    if writeIdx < n then
                        array.[writeIdx] <- localBufferI.[lid + wgSize]
                        values.[writeIdx] <- localBufferV.[lid + wgSize]
            @>

        do! RunCommand kernel <| fun kernelPrepare ->
            kernelPrepare
            <| _1D(Utils.floorToPower2 n |> Utils.getDefaultGlobalSize, wgSize)
            <| array
            <| values
    }


    let sortInplace3 (array: int[]) = opencl {
        if array.Length = 0 then
            return ()
        else
            let wgSize = Utils.defaultWorkGroupSize
            let n = array.Length

            do! localBegin3 array

            let mutable segmentLength = wgSize * 2
            while segmentLength < n do
                segmentLength <- segmentLength <<< 1

                do! globalStep3 array segmentLength true

                let mutable i = segmentLength / 2
                while i > wgSize * 2 do
                    do! globalStep3 array i false
                    i <- i >>> 1

                do! localEnd3 array
    }

    let private localBegin3 (array: int[]) = opencl {
        let wgSize = Utils.defaultWorkGroupSize
        let n = array.Length
        let processedSize = wgSize * 2
        let maxxx = System.Int32.MaxValue
        // let offset = processedSize - n

        let kernel =
            <@
                fun (range: _1D)
                    (array: int[]) ->

                    let lid = range.LocalID0
                    let gid = range.GlobalID0
                    let groupId = gid / wgSize

                    // 1 рабочя группа обрабатывает 2 * wgSize элементов
                    let localBuffer = localArray<int> processedSize
                    let mutable readIdx = processedSize * groupId + lid // 512 * 0 + 1

                    // копируем элементы из глобальной памяти в локальную
                    if readIdx < n then
                        localBuffer.[lid] <- array.[readIdx]
                    else
                        localBuffer.[lid] <- maxxx

                    readIdx <- readIdx + wgSize

                    if readIdx < n then
                        localBuffer.[lid + wgSize] <- array.[readIdx]
                    else
                        localBuffer.[lid + wgSize] <- maxxx

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

                        // let mutable q = 0
                        // if lid = 0 then
                        //     printf "\nsl = %i %i\n" segmentLength processedSize
                        //     while q < processedSize do
                        //         printf "%i " localBuffer.[q]
                        //         q <- q + 1

                        let mutable j = segmentLength / 2
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

                            // let mutable k = 0
                            // if lid = 0 then
                            //     printf "\nj = %i\n" j
                            //     printf "localLineId = %i\n" localLineId
                            //     printf "localTwinId = %i\n" localTwinId
                            //     printf "groupLineId = %i\n" groupLineId
                            //     printf "lineId = %i\n" lineId
                            //     printf "twinId = %i\n" twinId
                            //     while k < processedSize do
                            //         printf "%i " localBuffer.[k]
                            //         k <- k + 1

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

    let private globalStep3 (array: int[]) (segmentLength: int) (mirror: bool) = opencl {
        let wgSize = Utils.defaultWorkGroupSize
        let n = array.Length
        let maxxx = System.Int32.MaxValue


        let kernel =
            <@
                fun (range: _1D)
                    (array: int[]) ->

                    let gid = range.GlobalID0

                    // if gid = 0 then
                    //     printf "kek\n"

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

                    // let mutable k = 0
                    // if gid = 0 then
                    //     printf "\nsl = %i\n" segmentLength
                    //     while k < n do
                    //         printf "%i " array.[k]
                    //         k <- k + 1
            @>

        do! RunCommand kernel <| fun kernelPrepare ->
            kernelPrepare
            <| _1D(Utils.floorToPower2 n |> Utils.getDefaultGlobalSize, wgSize)
            <| array
    }

    let private localEnd3 (array: int[]) = opencl {
        let wgSize = Utils.defaultWorkGroupSize
        let n = array.Length
        let processedSize = wgSize * 2
        // let offset = processedSize - n
        let maxxx = System.Int32.MaxValue


        let kernel =
            <@
                fun (range: _1D)
                    (array: int[]) ->

                    let lid = range.LocalID0
                    let gid = range.GlobalID0
                    let groupId = gid / wgSize

                    // 1 рабочя группа обрабатывает 2 * wgSize элементов
                    let localBuffer = localArray<int> processedSize
                    let mutable readIdx = processedSize * groupId + lid

                    // копируем элементы из глобальной памяти в локальную
                    if readIdx < n then
                        localBuffer.[lid] <- array.[readIdx]
                    else
                        localBuffer.[lid] <- maxxx

                    readIdx <- readIdx + wgSize

                    if readIdx < n then
                        localBuffer.[lid + wgSize] <- array.[readIdx]
                    else
                        localBuffer.[lid + wgSize] <- maxxx

                    barrier ()

                    let mutable j = processedSize
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

                        // let mutable k = 0
                        // if lid = 0 then
                        //     printf "\nj = %i\n" j
                        //     while k < n do
                        //         printf "%i " array.[k]
                        //         k <- k + 1

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
