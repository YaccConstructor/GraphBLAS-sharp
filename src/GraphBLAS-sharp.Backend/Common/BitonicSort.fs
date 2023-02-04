namespace GraphBLAS.FSharp.Backend.Common

open Brahma.FSharp

module internal BitonicSort =
    let private localBegin (clContext: ClContext) workGroupSize =

        let processedSize = workGroupSize * 2

        let localBegin =
            <@ fun (range: Range1D) (rows: ClArray<'n>) (cols: ClArray<'n>) (values: ClArray<'a>) (length: int) ->

                let lid = range.LocalID0
                let gid = range.GlobalID0
                let groupId = gid / workGroupSize

                // 1 рабочая группа обрабатывает 2 * workGroupSize элементов
                let localRows = localArray<'n> processedSize
                let localCols = localArray<'n> processedSize
                let localValues = localArray<'a> processedSize

                let mutable readIdx = processedSize * groupId + lid
                let mutable localLength = local<int> ()
                localLength <- processedSize

                // копируем элементы из глобальной памяти в локальную
                if readIdx < length then
                    localRows.[lid] <- rows.[readIdx]
                    localCols.[lid] <- cols.[readIdx]
                    localValues.[lid] <- values.[readIdx]

                if readIdx = length then
                    localLength <- lid

                readIdx <- readIdx + workGroupSize

                if readIdx < length then
                    localRows.[lid + workGroupSize] <- rows.[readIdx]
                    localCols.[lid + workGroupSize] <- cols.[readIdx]
                    localValues.[lid + workGroupSize] <- values.[readIdx]

                if readIdx = length then
                    localLength <- lid + workGroupSize

                barrierLocal ()

                let mutable segmentLength = 1

                while segmentLength < processedSize do
                    segmentLength <- segmentLength <<< 1
                    let localLineId = lid % (segmentLength >>> 1)
                    let localTwinId = segmentLength - localLineId - 1
                    let groupLineId = lid / (segmentLength >>> 1)

                    let lineId =
                        segmentLength * groupLineId + localLineId

                    let twinId =
                        segmentLength * groupLineId + localTwinId

                    if twinId < localLength
                       && (localRows.[lineId] > localRows.[twinId]
                           || localRows.[lineId] = localRows.[twinId]
                              && localCols.[lineId] > localCols.[twinId]) then
                        let tmpRow = localRows.[lineId]
                        localRows.[lineId] <- localRows.[twinId]
                        localRows.[twinId] <- tmpRow

                        let tmpCol = localCols.[lineId]
                        localCols.[lineId] <- localCols.[twinId]
                        localCols.[twinId] <- tmpCol

                        let tmpValue = localValues.[lineId]
                        localValues.[lineId] <- localValues.[twinId]
                        localValues.[twinId] <- tmpValue

                    barrierLocal ()

                    let mutable j = segmentLength >>> 1

                    while j > 1 do
                        let localLineId = lid % (j >>> 1)
                        let localTwinId = localLineId + (j >>> 1)
                        let groupLineId = lid / (j >>> 1)
                        let lineId = j * groupLineId + localLineId
                        let twinId = j * groupLineId + localTwinId

                        if twinId < localLength
                           && (localRows.[lineId] > localRows.[twinId]
                               || localRows.[lineId] = localRows.[twinId]
                                  && localCols.[lineId] > localCols.[twinId]) then
                            let tmpRow = localRows.[lineId]
                            localRows.[lineId] <- localRows.[twinId]
                            localRows.[twinId] <- tmpRow

                            let tmpCol = localCols.[lineId]
                            localCols.[lineId] <- localCols.[twinId]
                            localCols.[twinId] <- tmpCol

                            let tmpValue = localValues.[lineId]
                            localValues.[lineId] <- localValues.[twinId]
                            localValues.[twinId] <- tmpValue

                        barrierLocal ()

                        j <- j >>> 1

                let mutable writeIdx = processedSize * groupId + lid

                if writeIdx < length then
                    rows.[writeIdx] <- localRows.[lid]
                    cols.[writeIdx] <- localCols.[lid]
                    values.[writeIdx] <- localValues.[lid]

                writeIdx <- writeIdx + workGroupSize

                if writeIdx < length then
                    rows.[writeIdx] <- localRows.[lid + workGroupSize]
                    cols.[writeIdx] <- localCols.[lid + workGroupSize]
                    values.[writeIdx] <- localValues.[lid + workGroupSize] @>

        let program = clContext.Compile(localBegin)

        fun (queue: MailboxProcessor<_>) (rows: ClArray<'n>) (cols: ClArray<'n>) (values: ClArray<'a>) ->

            let ndRange =
                Range1D.CreateValid(Utils.floorToPower2 values.Length, workGroupSize)

            let kernel = program.GetKernel()

            queue.Post(Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange rows cols values values.Length))
            queue.Post(Msg.CreateRunMsg<_, _>(kernel))


    let private globalStep (clContext: ClContext) workGroupSize =

        let globalStep =
            <@ fun (range: Range1D) (rows: ClArray<'n>) (cols: ClArray<'n>) (values: ClArray<'a>) (length: int) (segmentLength: int) (mirror: ClCell<bool>) ->

                let mirror = mirror.Value

                let gid = range.GlobalID0

                let localLineId = gid % (segmentLength >>> 1)
                let mutable localTwinId = 0

                if mirror then
                    localTwinId <- segmentLength - localLineId - 1
                else
                    localTwinId <- localLineId + (segmentLength >>> 1)

                let groupLineId = gid / (segmentLength >>> 1)

                let lineId =
                    segmentLength * groupLineId + localLineId

                let twinId =
                    segmentLength * groupLineId + localTwinId

                if twinId < length
                   && (rows.[lineId] > rows.[twinId]
                       || rows.[lineId] = rows.[twinId]
                          && cols.[lineId] > cols.[twinId]) then
                    let tmpRow = rows.[lineId]
                    rows.[lineId] <- rows.[twinId]
                    rows.[twinId] <- tmpRow

                    let tmpCol = cols.[lineId]
                    cols.[lineId] <- cols.[twinId]
                    cols.[twinId] <- tmpCol

                    let tmpV = values.[lineId]
                    values.[lineId] <- values.[twinId]
                    values.[twinId] <- tmpV @>

        let program = clContext.Compile(globalStep)

        fun (queue: MailboxProcessor<_>) (rows: ClArray<'n>) (cols: ClArray<'n>) (values: ClArray<'a>) (segmentLength: int) (mirror: bool) ->

            let ndRange =
                Range1D.CreateValid(Utils.floorToPower2 values.Length, workGroupSize)

            let mirror = clContext.CreateClCell mirror

            let kernel = program.GetKernel()

            queue.Post(
                Msg.MsgSetArguments
                    (fun () -> kernel.KernelFunc ndRange rows cols values values.Length segmentLength mirror)
            )

            queue.Post(Msg.CreateRunMsg<_, _>(kernel))
            queue.Post(Msg.CreateFreeMsg(mirror))


    let private localEnd (clContext: ClContext) workGroupSize =

        let processedSize = workGroupSize * 2

        let localEnd =
            <@ fun (range: Range1D) (rows: ClArray<'n>) (cols: ClArray<'n>) (values: ClArray<'a>) (length: int) ->

                let lid = range.LocalID0
                let gid = range.GlobalID0
                let groupId = gid / workGroupSize

                // 1 рабочая группа обрабатывает 2 * wgSize элементов
                let localRows = localArray<'n> processedSize
                let localCols = localArray<'n> processedSize
                let localValues = localArray<'a> processedSize

                let mutable readIdx = processedSize * groupId + lid
                let mutable localLength = local<int> ()
                localLength <- processedSize

                // копируем элементы из глобальной памяти в локальную
                if readIdx < length then
                    localRows.[lid] <- rows.[readIdx]
                    localCols.[lid] <- cols.[readIdx]
                    localValues.[lid] <- values.[readIdx]

                if readIdx = length then
                    localLength <- lid

                readIdx <- readIdx + workGroupSize

                if readIdx < length then
                    localRows.[lid + workGroupSize] <- rows.[readIdx]
                    localCols.[lid + workGroupSize] <- cols.[readIdx]
                    localValues.[lid + workGroupSize] <- values.[readIdx]

                if readIdx = length then
                    localLength <- lid + workGroupSize

                barrierLocal ()

                let mutable segmentLength = processedSize
                let mutable j = segmentLength

                while j > 1 do
                    let localLineId = lid % (j / 2)
                    let localTwinId = localLineId + (j / 2)
                    let groupLineId = lid / (j / 2)
                    let lineId = j * groupLineId + localLineId
                    let twinId = j * groupLineId + localTwinId

                    if twinId < localLength
                       && (localRows.[lineId] > localRows.[twinId]
                           || localRows.[lineId] = localRows.[twinId]
                              && localCols.[lineId] > localCols.[twinId]) then
                        let tmpRow = localRows.[lineId]
                        localRows.[lineId] <- localRows.[twinId]
                        localRows.[twinId] <- tmpRow

                        let tmpCol = localCols.[lineId]
                        localCols.[lineId] <- localCols.[twinId]
                        localCols.[twinId] <- tmpCol

                        let tmpValue = localValues.[lineId]
                        localValues.[lineId] <- localValues.[twinId]
                        localValues.[twinId] <- tmpValue

                    barrierLocal ()

                    j <- j >>> 1

                let mutable writeIdx = processedSize * groupId + lid

                if writeIdx < length then
                    rows.[writeIdx] <- localRows.[lid]
                    cols.[writeIdx] <- localCols.[lid]
                    values.[writeIdx] <- localValues.[lid]

                writeIdx <- writeIdx + workGroupSize

                if writeIdx < length then
                    rows.[writeIdx] <- localRows.[lid + workGroupSize]
                    cols.[writeIdx] <- localCols.[lid + workGroupSize]
                    values.[writeIdx] <- localValues.[lid + workGroupSize] @>

        let program = clContext.Compile(localEnd)

        fun (queue: MailboxProcessor<_>) (rows: ClArray<'n>) (cols: ClArray<'n>) (values: ClArray<'a>) ->

            let ndRange =
                Range1D.CreateValid(Utils.floorToPower2 values.Length, workGroupSize)

            let kernel = program.GetKernel()

            queue.Post(Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange rows cols values values.Length))
            queue.Post(Msg.CreateRunMsg<_, _>(kernel))

    let sortKeyValuesInplace<'n, 'a when 'n: comparison> (clContext: ClContext) workGroupSize =

        let localBegin = localBegin clContext workGroupSize
        let globalStep = globalStep clContext workGroupSize
        let localEnd = localEnd clContext workGroupSize

        fun (queue: MailboxProcessor<_>) (rows: ClArray<'n>) (cols: ClArray<'n>) (values: ClArray<'a>) ->

            let lengthCeiled = Utils.ceilToPower2 values.Length

            let rec loopNested i =
                if i > workGroupSize * 2 then
                    globalStep queue rows cols values i false
                    loopNested (i >>> 1)

            let rec mainLoop segmentLength =
                if segmentLength <= lengthCeiled then
                    globalStep queue rows cols values segmentLength true
                    loopNested (segmentLength >>> 1)
                    localEnd queue rows cols values
                    mainLoop (segmentLength <<< 1)

            localBegin queue rows cols values
            mainLoop (workGroupSize <<< 2)
