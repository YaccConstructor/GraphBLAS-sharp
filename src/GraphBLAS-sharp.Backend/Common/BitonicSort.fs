namespace GraphBLAS.FSharp.Backend.Common

open Brahma.FSharp.OpenCL

module internal rec BitonicSort =
    let sortKeyValuesInplace (keys: uint64 []) (values: 'a []) =
        opencl {
            if keys.Length = 0 then
                return ()
            else
                let wgSize = 256
                let length = keys.Length

                do! localBegin keys values

                let mutable segmentLength = wgSize * 2

                while segmentLength < length do
                    segmentLength <- segmentLength <<< 1

                    do! globalStep keys values segmentLength true

                    let mutable i = segmentLength / 2

                    while i > wgSize * 2 do
                        do! globalStep keys values i false
                        i <- i >>> 1

                    do! localEnd keys values
        }

    let private localBegin (keys: uint64 []) (values: 'a []) =
        opencl {
            let wgSize = 256
            let length = keys.Length
            let processedSize = wgSize * 2
            let positiveInf = System.UInt64.MaxValue

            let kernel =
                <@ fun (range: Range1D) (keys: uint64 []) (values: 'a []) ->

                    let lid = range.LocalID0
                    let gid = range.GlobalID0
                    let groupId = gid / wgSize

                    // 1 рабочя группа обрабатывает 2 * wgSize элементов
                    let localKeys = localArray<uint64> processedSize
                    let localValues = localArray<'a> processedSize

                    let mutable readIdx = processedSize * groupId + lid

                    // копируем элементы из глобальной памяти в локальную
                    if readIdx < length then
                        localKeys.[lid] <- keys.[readIdx]
                    else
                        localKeys.[lid] <- positiveInf

                    if readIdx < length then
                        localValues.[lid] <- values.[readIdx]

                    readIdx <- readIdx + wgSize

                    if readIdx < length then
                        localKeys.[lid + wgSize] <- keys.[readIdx]
                    else
                        localKeys.[lid + wgSize] <- positiveInf

                    if readIdx < length then
                        localValues.[lid + wgSize] <- values.[readIdx]

                    barrier ()

                    let mutable segmentLength = 1

                    while segmentLength < processedSize do
                        segmentLength <- segmentLength <<< 1
                        let localLineId = lid % (segmentLength / 2)
                        let localTwinId = segmentLength - localLineId - 1
                        let groupLineId = lid / (segmentLength / 2)

                        let lineId =
                            segmentLength * groupLineId + localLineId

                        let twinId =
                            segmentLength * groupLineId + localTwinId

                        if localKeys.[lineId] > localKeys.[twinId] then
                            let tmpKey = localKeys.[lineId]
                            localKeys.[lineId] <- localKeys.[twinId]
                            localKeys.[twinId] <- tmpKey

                            let tmpValue = localValues.[lineId]
                            localValues.[lineId] <- localValues.[twinId]
                            localValues.[twinId] <- tmpValue

                        barrier ()

                        let mutable j = segmentLength / 2

                        while j > 1 do
                            let localLineId = lid % (j / 2)
                            let localTwinId = localLineId + (j / 2)
                            let groupLineId = lid / (j / 2)
                            let lineId = j * groupLineId + localLineId
                            let twinId = j * groupLineId + localTwinId

                            if localKeys.[lineId] > localKeys.[twinId] then
                                let tmpKey = localKeys.[lineId]
                                localKeys.[lineId] <- localKeys.[twinId]
                                localKeys.[twinId] <- tmpKey

                                let tmpValue = localValues.[lineId]
                                localValues.[lineId] <- localValues.[twinId]
                                localValues.[twinId] <- tmpValue

                            barrier ()

                            j <- j >>> 1

                    let mutable writeIdx = processedSize * groupId + lid

                    if writeIdx < length then
                        keys.[writeIdx] <- localKeys.[lid]
                        values.[writeIdx] <- localValues.[lid]

                    writeIdx <- writeIdx + wgSize

                    if writeIdx < length then
                        keys.[writeIdx] <- localKeys.[lid + wgSize]
                        values.[writeIdx] <- localValues.[lid + wgSize] @>

            do!
                runCommand kernel
                <| fun kernelPrepare ->
                    kernelPrepare
                    <| Range1D.CreateValid(Utils.floorToPower2 length, wgSize)
                    <| keys
                    <| values
        }

    let private globalStep (keys: uint64 []) (values: 'a []) (segmentLength: int) (mirror: bool) =
        opencl {
            let wgSize = 256
            let length = keys.Length

            let kernel =
                <@ fun (range: Range1D) (keys: uint64 []) (values: 'a []) ->

                    let gid = range.GlobalID0

                    let localLineId = gid % (segmentLength / 2)
                    let mutable localTwinId = 0

                    if mirror then
                        localTwinId <- segmentLength - localLineId - 1
                    else
                        localTwinId <- localLineId + (segmentLength / 2)

                    let groupLineId = gid / (segmentLength / 2)

                    let lineId =
                        segmentLength * groupLineId + localLineId

                    let twinId =
                        segmentLength * groupLineId + localTwinId

                    if twinId < length && keys.[lineId] > keys.[twinId] then
                        let tmp = keys.[lineId]
                        keys.[lineId] <- keys.[twinId]
                        keys.[twinId] <- tmp

                        let tmpV = values.[lineId]
                        values.[lineId] <- values.[twinId]
                        values.[twinId] <- tmpV @>

            do!
                runCommand kernel
                <| fun kernelPrepare ->
                    kernelPrepare
                    <| Range1D.CreateValid(Utils.floorToPower2 length, wgSize)
                    <| keys
                    <| values
        }

    let private localEnd (keys: uint64 []) (values: 'a []) =
        opencl {
            let wgSize = 256
            let length = keys.Length
            let processedSize = wgSize * 2
            let positiveInf = System.UInt64.MaxValue

            let kernel =
                <@ fun (range: Range1D) (keys: uint64 []) (values: 'a []) ->

                    let lid = range.LocalID0
                    let gid = range.GlobalID0
                    let groupId = gid / wgSize

                    // 1 рабочя группа обрабатывает 2 * wgSize элементов
                    let localKeys = localArray<uint64> processedSize
                    let localValues = localArray<'a> processedSize

                    let mutable readIdx = processedSize * groupId + lid

                    // копируем элементы из глобальной памяти в локальную
                    if readIdx < length then
                        localKeys.[lid] <- keys.[readIdx]
                    else
                        localKeys.[lid] <- positiveInf

                    if readIdx < length then
                        localValues.[lid] <- values.[readIdx]

                    readIdx <- readIdx + wgSize

                    if readIdx < length then
                        localKeys.[lid + wgSize] <- keys.[readIdx]
                    else
                        localKeys.[lid + wgSize] <- positiveInf

                    if readIdx < length then
                        localValues.[lid + wgSize] <- values.[readIdx]

                    barrier ()

                    let mutable segmentLength = processedSize
                    let mutable j = segmentLength

                    while j > 1 do
                        let localLineId = lid % (j / 2)
                        let localTwinId = localLineId + (j / 2)
                        let groupLineId = lid / (j / 2)
                        let lineId = j * groupLineId + localLineId
                        let twinId = j * groupLineId + localTwinId

                        if localKeys.[lineId] > localKeys.[twinId] then
                            let tmpKey = localKeys.[lineId]
                            localKeys.[lineId] <- localKeys.[twinId]
                            localKeys.[twinId] <- tmpKey

                            let tmpValue = localValues.[lineId]
                            localValues.[lineId] <- localValues.[twinId]
                            localValues.[twinId] <- tmpValue

                        barrier ()

                        j <- j >>> 1

                    let mutable writeIdx = processedSize * groupId + lid

                    if writeIdx < length then
                        keys.[writeIdx] <- localKeys.[lid]
                        values.[writeIdx] <- localValues.[lid]

                    writeIdx <- writeIdx + wgSize

                    if writeIdx < length then
                        keys.[writeIdx] <- localKeys.[lid + wgSize]
                        values.[writeIdx] <- localValues.[lid + wgSize] @>

            do!
                runCommand kernel
                <| fun kernelPrepare ->
                    kernelPrepare
                    <| Range1D.CreateValid(Utils.floorToPower2 length, wgSize)
                    <| keys
                    <| values
        }
