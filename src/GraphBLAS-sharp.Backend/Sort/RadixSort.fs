namespace GraphBLAS.FSharp.Backend.Common

open Brahma.FSharp.OpenCL
open Microsoft.FSharp.Quotations
open Microsoft.FSharp.Core.Operators
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Predefined

module internal RadixSort =
    let private setPositions
        (getBy: Expr<'b -> uint64 -> int>)
        (clContext: ClContext)
        workGroupSize =

        let setPositions =
            <@
                fun (ndRange: Range1D)
                    (keys: ClArray<uint64>)
                    (values: ClArray<'a>)
                    (positions: ClArray<_>)
                    (resultKeys: ClArray<uint64>)
                    (keysLength: int)
                    (resultValues: ClArray<'a>)
                    (numberOfBits: int)
                    (offset: int) ->

                    let i = ndRange.GlobalID0

                    if i < keysLength then
                        let mutable mask = 0UL
                        for j in 0 .. numberOfBits - 1 do
                            mask <- (mask <<< 1) ||| 1UL
                        let buff = positions.[i]
                        let index = (%getBy) buff ((keys.[i] >>> offset) &&& mask)
                        //let index = (%get) buff ((keys.[i] >>> offset) &&& ~~~(0xFFFFFFFFFFFFFFFFUL <<< numberOfBits))
                        resultKeys.[index] <- keys.[i]
                        resultValues.[index] <- values.[i]
            @>
        let program = clContext.CreateClProgram(setPositions)

        fun (processor: MailboxProcessor<_>)
            (positions: ClArray<_>)
            (keys: ClArray<uint64>)
            (values: ClArray<'a>)
            (resultKeys: ClArray<uint64>)
            (resultValues: ClArray<'a>)
            (numberOfBits: int)
            (stage: int) ->

            let keysLength = keys.Length
            let offset = numberOfBits * stage

            let ndRange = Range1D.CreateValid(keysLength, workGroupSize)

            let kernel = program.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.KernelFunc
                            ndRange
                            keys
                            values
                            positions
                            resultKeys
                            keysLength
                            resultValues
                            numberOfBits
                            offset)
            )
            processor.Post(Msg.CreateRunMsg<_, _> kernel)

    let private updatePositions
        (plus: Expr<'b -> 'b -> 'b>)
        (clContext: ClContext)
        workGroupSize =

        let updatePositions =
            <@
                fun (ndRange: Range1D)
                    (positions: ClArray<_>)
                    (positionsLength: int)
                    (sums: ClCell<_>) ->

                    let i = ndRange.GlobalID0

                    if i < positionsLength then
                        let sums = sums.Value
                        let buff = positions.[i]
                        positions.[i] <- (%plus) buff sums
            @>
        let program = clContext.CreateClProgram(updatePositions)

        fun (processor: MailboxProcessor<_>)
            (positions: ClArray<_>)
            (positionsLength: int)
            (sums: ClCell<_>) ->
            let ndRange = Range1D.CreateValid(positionsLength, workGroupSize)

            let kernel = program.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () -> kernel.KernelFunc ndRange positions positionsLength sums)
            )

            processor.Post(Msg.CreateRunMsg<_, _> kernel)

    let private updateSums
        (scan: Expr<'b -> 'b>)
        (clContext: ClContext)
        workGroupSize =

        let update =
            <@
                fun (ndRange: Range1D)
                    (sums: ClCell<'b>) ->

                    let i = ndRange.GlobalID0
                    if i = 0 then
                        let a = sums.Value
                        sums.Value <- (%scan) a
            @>
        let program = clContext.CreateClProgram(update)

        fun (processor: MailboxProcessor<_>)
            (sums: ClCell<_>) ->

            let ndRange = Range1D(workGroupSize)

            let kernel = program.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () -> kernel.KernelFunc ndRange sums)
            )
            processor.Post(Msg.CreateRunMsg<_, _> kernel)

    let private initPositions
        (get: Expr<uint64 -> 'b>)
        (clContext: ClContext)
        workGroupSize =

        let init =
            <@
                fun (ndRange: Range1D)
                    (keys: ClArray<uint64>)
                    (keysLength: int)
                    (positions: ClArray<_>)
                    (numberOfBits: int)
                    (offset: int) ->

                    let i = ndRange.GlobalID0

                    if i < keysLength then
                        let mutable mask = 0UL
                        for j in 0 .. numberOfBits - 1 do
                            mask <- (mask <<< 1) ||| 1UL
                        positions.[i] <- (%get) ((keys.[i] >>> offset) &&& mask)

                        //let buff = keys.[i]
                        //positions.[i] <- (%get) ((buff >>> offset) &&& ~~~(0xFFFFFFFFFFFFFFFFUL <<< numberOfBits))
            @>
        let program = clContext.CreateClProgram(init)

        fun (processor: MailboxProcessor<_>)
            (keys: ClArray<uint64>)
            (positions: ClArray<_>)
            (numberOfBits: int)
            (stage: int) ->

            let keysLength = keys.Length
            let offset = numberOfBits * stage

            let ndRange = Range1D.CreateValid(keysLength, workGroupSize)

            let kernel = program.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () -> kernel.KernelFunc ndRange keys keysLength positions numberOfBits offset)
            )
            processor.Post(Msg.CreateRunMsg<_, _> kernel)

    let private preparePositions
        (plus: Expr<'b -> 'b -> 'b>)
        (get: Expr<uint64 -> 'b>)
        (scan: Expr<'b -> 'b>)
        (zero: 'b)
        (clContext: ClContext)
        workGroupSize =

        let initPositions = initPositions get clContext workGroupSize
        let scanExcludeInPlace = PrefixSum.runExcludeInplace plus clContext workGroupSize
        let updateSums = updateSums scan clContext workGroupSize
        let updatePositions = updatePositions plus clContext workGroupSize

        fun (processor: MailboxProcessor<_>)
            (keys: ClArray<uint64>)
            (positions: ClArray<_>)
            (sums: ClCell<_>)
            (numberOfBits: int)
            (stage: int) ->

            initPositions processor keys positions numberOfBits stage

            scanExcludeInPlace processor positions sums zero
            |> ignore

            updateSums processor sums

            updatePositions processor positions keys.Length sums

    // TODO: сделать через ClArray.create
    let private createPositions
        (clContext: ClContext)
        workGroupSize =

        let init =
            <@
                fun (range: Range1D)
                    (zero: ClCell<'b>)
                    (positions: ClArray<'b>)
                    (length: int) ->

                    let i = range.GlobalID0
                    let zero = zero.Value
                    if i < length then
                        positions.[i] <- zero
            @>
        let program = clContext.CreateClProgram(init)

        fun (processor: MailboxProcessor<_>)
            (length: int)
            (zero: 'b) ->

            let zero = clContext.CreateClCell(zero)

            let positions = clContext.CreateClArray(length)

            let ndRange = Range1D.CreateValid(length, workGroupSize)

            let kernel = program.GetKernel()

            processor.Post(
                Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange zero positions length)
            )
            processor.Post(Msg.CreateRunMsg<_, _> kernel)

            processor.Post(Msg.CreateFreeMsg(zero))

            positions

    let private sortByKeyInPlaceGeneral
        (plus: Expr<'b -> 'b -> 'b>)
        (get: Expr<uint64 -> 'b>)
        (getBy: Expr<'b -> uint64 -> int>)
        (scan: Expr<'b -> 'b>)
        (zero: 'b)
        (numberOfBits: int)
        (clContext: ClContext)
        workGroupSize =

        let createPositions = createPositions clContext workGroupSize
        let copy = ClArray.copy clContext workGroupSize
        let copyData = ClArray.copy clContext workGroupSize
        let preparePositions =
            preparePositions
                plus
                get
                scan
                zero
                clContext
                workGroupSize
        let setPositions =
            setPositions
                getBy
                clContext
                workGroupSize

        fun (processor: MailboxProcessor<_>)
            (keys: ClArray<uint64>)
            (values: ClArray<'a>) ->

            let numberOfStages = (64 - 1) / numberOfBits + 1

            let positions = createPositions processor keys.Length zero
            let sums = clContext.CreateClCell()

            let auxiliaryKeys =
                if numberOfStages % 2 = 0 then
                    clContext.CreateClArray(
                        keys.Length,
                        hostAccessMode = HostAccessMode.NotAccessible
                    )
                else
                    copy processor keys

            let auxiliaryValues =
                if numberOfStages % 2 = 0 then
                    clContext.CreateClArray(
                        values.Length,
                        hostAccessMode = HostAccessMode.NotAccessible
                    )
                else
                    copyData processor values

            let swap (a, b) = (b, a)

            let mutable keysArrays = auxiliaryKeys, keys
            let mutable valuesArrays = auxiliaryValues, values
            if numberOfStages % 2 = 0 then
                keysArrays <- swap keysArrays
                valuesArrays <- swap valuesArrays

            for stage in 0 .. numberOfStages - 1 do
                preparePositions
                    processor
                    (fst keysArrays)
                    positions
                    sums
                    numberOfBits
                    stage

                setPositions
                    processor
                    positions
                    (fst keysArrays)
                    (fst valuesArrays)
                    (snd keysArrays)
                    (snd valuesArrays)
                    numberOfBits
                    stage

                keysArrays <- swap keysArrays
                valuesArrays <- swap valuesArrays

            processor.Post(Msg.CreateFreeMsg(positions))
            processor.Post(Msg.CreateFreeMsg(sums))
            processor.Post(Msg.CreateFreeMsg(auxiliaryKeys))
            processor.Post(Msg.CreateFreeMsg(auxiliaryValues))

    let sortByKeyInPlace2 clContext =
        sortByKeyInPlaceGeneral
            <@
                fun (struct(x1, x2): struct(int * int))
                    (struct(y1, y2): struct(int * int)) ->
                    struct(x1 + y1, x2 + y2)
            @>
            <@
                fun (n: uint64) ->
                    if n = 0UL then struct(1, 0)
                    elif n = 1UL then struct(0, 1)
                    else struct(0, 0)
            @>
            <@
                fun (struct(x1, x2): struct(int * int))
                    (n: uint64) ->
                    if n = 0UL then x1
                    elif n = 1UL then x2
                    else 0
            @>
            <@
                fun (struct(x1, x2): struct(int * int)) ->
                    struct(0, x1)
            @>
            struct(0, 0)
            1
            clContext

    let sortByKeyInPlace4 clContext =
        sortByKeyInPlaceGeneral
            <@
                fun (struct(x1, x2, x3, x4): struct(int * int * int * int))
                    (struct(y1, y2, y3, y4): struct(int * int * int * int)) ->
                    struct(x1 + y1, x2 + y2, x3 + y3, x4 + y4)
            @>
            <@
                fun (n: uint64) ->
                    if n = 0UL then struct(1, 0, 0, 0)
                    elif n = 1UL then struct(0, 1, 0, 0)
                    elif n = 2UL then struct(0, 0, 1, 0)
                    elif n = 3UL then struct(0, 0, 0, 1)
                    else struct(0, 0, 0, 0)
            @>
            <@
                fun (struct(x1, x2, x3, x4): struct(int * int * int * int))
                    (n: uint64) ->
                    if n = 0UL then x1
                    elif n = 1UL then x2
                    elif n = 2UL then x3
                    elif n = 3UL then x4
                    else 0
            @>
            <@
                fun (struct(x1, x2, x3, x4): struct(int * int * int * int)) ->
                    struct(0, x1, x1 + x2, x1 + x2 + x3)
            @>
            struct(0, 0, 0, 0)
            2
            clContext

    let sortByKeyInPlace8 clContext =
        sortByKeyInPlaceGeneral
            <@
                fun (struct(x1, x2, x3, x4, x5, x6, x7, x8): struct(int * int * int * int * int * int * int * int))
                    (struct(y1, y2, y3, y4, y5, y6, y7, y8): struct(int * int * int * int * int * int * int * int)) ->
                    struct(x1 + y1, x2 + y2, x3 + y3, x4 + y4, x5 + y5, x6 + y6, x7 + y7, x8 + y8)
            @>
            <@
                fun (n: uint64) ->
                    if n = 0UL then struct(1, 0, 0, 0, 0, 0, 0, 0)
                    elif n = 1UL then struct(0, 1, 0, 0, 0, 0, 0, 0)
                    elif n = 2UL then struct(0, 0, 1, 0, 0, 0, 0, 0)
                    elif n = 3UL then struct(0, 0, 0, 1, 0, 0, 0, 0)
                    elif n = 4UL then struct(0, 0, 0, 0, 1, 0, 0, 0)
                    elif n = 5UL then struct(0, 0, 0, 0, 0, 1, 0, 0)
                    elif n = 6UL then struct(0, 0, 0, 0, 0, 0, 1, 0)
                    elif n = 7UL then struct(0, 0, 0, 0, 0, 0, 0, 1)
                    else struct(0, 0, 0, 0, 0, 0, 0, 0)
            @>
            <@
                fun (struct(x1, x2, x3, x4, x5, x6, x7, x8): struct(int * int * int * int * int * int * int * int))
                    (n: uint64) ->
                    if n = 0UL then x1
                    elif n = 1UL then x2
                    elif n = 2UL then x3
                    elif n = 3UL then x4
                    elif n = 4UL then x5
                    elif n = 5UL then x6
                    elif n = 6UL then x7
                    elif n = 7UL then x8
                    else 0
            @>
            <@
                fun (struct(x1, x2, x3, x4, x5, x6, x7, x8): struct(int * int * int * int * int * int * int * int)) ->
                    let y2 = x1 + x2
                    let y3 = y2 + x3
                    let y4 = y3 + x4
                    let y5 = y4 + x5
                    let y6 = y5 + x6
                    let y7 = y6 + x7
                    struct(0, x1, y2, y3, y4, y5, y6, y7)
            @>
            struct(0, 0, 0, 0, 0, 0, 0, 0)
            3
            clContext

    let private createHistograms
        clContext
        workGroupSize =

        let scanExclude = PrefixSum.runExclude <@ (+) @> clContext workGroupSize
        let scatter = Scatter.initInPlace <@ fun i -> i @> clContext workGroupSize
        let zeroCreate = ClArray.zeroCreate clContext workGroupSize

        let createHistograms =
            <@
                fun (range: Range1D)
                    (keys: ClArray<int>)
                    (sectorIndices: ClArray<int>)
                    (sectorPointers: ClArray<int>)
                    (histograms00: ClArray<int>)
                    (histograms01: ClArray<int>)
                    (histograms10: ClArray<int>)
                    (histograms11: ClArray<int>)
                    (offset: int)
                    (length: int) ->

                    let localID = range.LocalID0

                    // Compute actual position of the workgroup relative to the array of keys
                    let mutable sector = local<int>()
                    let mutable shift = local<int>()
                    let mutable sectorEdge = local<int>()
                    if localID = 0 then
                        let i = range.GlobalID0
                        if i >= length then
                            // It's additional workgroup
                            sector <- (i - length) / workGroupSize + 1
                            let ptr = sectorPointers.[sector - 1] + 1
                            sectorEdge <- sectorPointers.[sector]
                            shift <- i - (ptr + workGroupSize * (sectorEdge / workGroupSize - (ptr - 1) / workGroupSize))
                        else
                            sector <- sectorIndices.[i]
                            sectorEdge <- sectorPointers.[sector]
                            if sector = 0 then
                                shift <- 0
                            else
                                let ptr = sectorPointers.[sector - 1] + 1
                                shift <- (i - ptr) % workGroupSize
                    barrierLocal()
                    let i = range.GlobalID0 - shift

                    let localResult = localArray<int> 4
                    let localHistograms = localArray<int> workGroupSize
                    localHistograms.[localID] <- 0

                    let mutable masked = 0
                    if i <= sectorEdge then
                        let key = keys.[i]
                        masked <- int (key >>> offset) &&& 0b11

                    for j in 0b00 .. 0b11 do
                        if masked = j && i <= sectorEdge then
                            localHistograms.[localID] <- 1
                        else
                            localHistograms.[localID] <- 0

                        let mutable step = 2

                        while step <= workGroupSize do
                            barrierLocal ()

                            if localID < workGroupSize / step then
                                let i = step * (localID + 1) - 1
                                localHistograms.[i] <- localHistograms.[i] + localHistograms.[i - (step >>> 1)]

                            step <- step <<< 1

                        barrierLocal ()

                        if localID = workGroupSize - 1 then
                            localResult.[j] <- localHistograms.[localID]

                    barrierLocal ()

                    if localID = 0 then
                        atomic (+) histograms00.[sector] localResult.[0b00] |> ignore
                        atomic (+) histograms01.[sector] localResult.[0b01] |> ignore
                        atomic (+) histograms10.[sector] localResult.[0b10] |> ignore
                        atomic (+) histograms11.[sector] localResult.[0b11] |> ignore
            @>
        let program = clContext.CreateClProgram(createHistograms)

        fun (processor: MailboxProcessor<_>)
            (keys: ClArray<int>)
            (sectorTails: ClArray<int>)
            (globalStep: int) ->

            let sectorIndices, sectorsAmountGPU = scanExclude processor sectorTails (clContext.CreateClCell()) 0
            let sectorsAmount =
                ClCell.toHost sectorsAmountGPU
                |> ClTask.runSync clContext
            processor.Post(Msg.CreateFreeMsg<_>(sectorsAmountGPU))

            // sectorPointers.[i] points on the last element of the i-th sector
            let sectorPointers = zeroCreate processor sectorsAmount
            scatter processor sectorIndices sectorPointers

            let histograms =
                [0..3]
                |> List.map (fun _ ->
                    zeroCreate processor sectorsAmount
                    // clContext.CreateClArray(
                    //     sectorsAmount,
                    //     hostAccessMode = HostAccessMode.NotAccessible,
                    //     deviceAccessMode = DeviceAccessMode.ReadWrite
                    // )
                )

            let kernel = program.GetKernel()
            let ndRange = Range1D.CreateValid(keys.Length + (sectorsAmount - 1) * workGroupSize, workGroupSize)
            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.KernelFunc
                            ndRange
                            keys
                            sectorIndices
                            sectorPointers
                            histograms.[0b00]
                            histograms.[0b01]
                            histograms.[0b10]
                            histograms.[0b11]
                            (32 - ((globalStep + 1) <<< 1))
                            keys.Length)
            )
            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            sectorIndices, sectorPointers, histograms

    let private permute
        (clContext: ClContext)
        workGroupSize =

        // Shift cyclically the part of the array to the right. Begin index inclusive, end index exclusive
        let doShift =
            <@
                fun (input: int[])
                    (output: int[])
                    (length: int)
                    (beginIdx: int)
                    (endIdx: int)
                    (shift: int)
                    (localID: int) ->

                    if localID < length then
                        if beginIdx <= localID && localID < endIdx then
                            let mutable offset = (localID - beginIdx + shift) % (endIdx - beginIdx)
                            if offset < 0 then
                                offset <- offset + endIdx - beginIdx
                            output.[beginIdx + offset] <- input.[localID]
                        else
                            output.[localID] <- input.[localID]
            @>

        // let changeState =
        //     <@
        //         fun (input: int[])
        //             (output: int[])
        //             (histogram: int[])
        //             (length: int)
        //             (initState: int)
        //             (finalState: int)
        //             (garbagePtr: int)
        //             (localID: int) ->

        //             let mutable minState = initState
        //             let mutable maxState = finalState
        //             if initState > finalState then
        //                 minState <- finalState
        //                 maxState <- initState
        //             let mutable value = 0
        //             for j in minState .. maxState - 1 do
        //                 value <- value + histogram.[j]
        //             if initState > finalState then
        //                 value <- -value
        //             (%doShift)
        //                 input
        //                 output
        //                 length
        //                 0
        //                 garbagePtr
        //                 value
        //                 localID
        //     @>

        let permute =
            <@
                fun (range: Range1D)
                    (keys: ClArray<int>)
                    (values: ClArray<'a>)
                    (sectorIndices: ClArray<int>)
                    (sectorPointers: ClArray<int>)
                    (histograms00: ClArray<int>)
                    (histograms01: ClArray<int>)
                    (histograms10: ClArray<int>)
                    (histograms11: ClArray<int>)
                    (garbage00: ClArray<int>)
                    (garbage01: ClArray<int>)
                    (garbage10: ClArray<int>)
                    (garbage11: ClArray<int>)
                    (isFinished: ClCell<bool>)
                    (offset: int)
                    (length: int) ->

                    let localID = range.LocalID0

                    let sectorHistogram = localArray<int> 4
                    let garbage = localArray<int> 4

                    // Compute actual position of the workgroup relative to the array of keys
                    let mutable sector = local<int>()
                    let mutable shift = local<int>()
                    let mutable sectorEdge = local<int>()
                    let mutable sectorPtr = local<int>()
                    let mutable allGarbage = local<int>()
                    if localID = 0 then
                        let i = range.GlobalID0
                        if i >= length then
                            // It's additional workgroup
                            sector <- (i - length) / workGroupSize + 1
                            sectorPtr <- sectorPointers.[sector - 1] + 1
                            sectorEdge <- sectorPointers.[sector]
                            shift <- i - (sectorPtr + workGroupSize *
                                (sectorEdge / workGroupSize - (sectorPtr - 1) / workGroupSize))
                        else
                            sector <- sectorIndices.[i]
                            sectorEdge <- sectorPointers.[sector]
                            if sector = 0 then
                                sectorPtr <- 0
                                shift <- 0
                            else
                                sectorPtr <- sectorPointers.[sector - 1] + 1
                                shift <- (i - sectorPtr) % workGroupSize

                        sectorHistogram.[0b00] <- histograms00.[sector]
                        sectorHistogram.[0b01] <- histograms01.[sector]
                        sectorHistogram.[0b10] <- histograms10.[sector]
                        sectorHistogram.[0b11] <- histograms11.[sector]

                        garbage.[0b00] <- garbage00.[sector]
                        garbage.[0b01] <- garbage01.[sector]
                        garbage.[0b10] <- garbage10.[sector]
                        garbage.[0b11] <- garbage11.[sector]
                        allGarbage <- 0
                        for j in 0 .. 3 do
                            allGarbage <- allGarbage + garbage.[j]
                    barrierLocal()
                    let i = range.GlobalID0 - shift

                    // let workGroupAmount = 1 + (sectorEdge - sectorPtr) / workGroupSize

                    let mutable workGroupAmount = 0
                    if allGarbage > 0 then
                        workGroupAmount <- 1 + (allGarbage - 1) / workGroupSize
                    let workGroupNumber = (i - sectorPtr) / workGroupSize
                    if workGroupNumber < workGroupAmount then
                        // Compute basic local histogram proportionally to the global one and remainders
                        let localHistogram = localArray<int> 4
                        let remainders = localArray<int> 4
                        if localID < 4 then
                            let hist = garbage.[localID]
                            localHistogram.[localID] <- hist / workGroupAmount
                            remainders.[localID] <- hist % workGroupAmount
                        barrierLocal()

                        // Compute inclusive prefix sum for remainders
                        if localID < 2 then
                            remainders.[1 + (localID <<< 1)] <- remainders.[1 + (localID <<< 1)] + remainders.[localID <<< 1]
                        barrierLocal()
                        if localID < 2 then
                            remainders.[localID + 2] <- remainders.[localID + 2] + remainders.[1]
                        barrierLocal()

                        // Compute exclusive prefix sum for sectorHistogram
                        if localID = 1 then
                            sectorHistogram.[localID + 2] <- sectorHistogram.[localID] + sectorHistogram.[localID - 1]
                            sectorHistogram.[localID] <- 0
                        barrierLocal()
                        if localID < 2 then
                            let i = 1 + (localID <<< 1)
                            let j = i - 1

                            let buff = sectorHistogram.[i]
                            sectorHistogram.[i] <- sectorHistogram.[j] + buff
                            sectorHistogram.[j] <- buff
                        barrierLocal()

                        // Compute sum and demand
                        let mutable sum = local<int>()
                        let mutable dem = local<int>()
                        if localID = 0 then
                            sum <- 0
                            for j in 0b00 .. 0b11 do
                                sum <- sum + localHistogram.[j]
                            dem <- workGroupSize - sum
                        barrierLocal()

                        // Compute bucket pointers
                        let globalBucketPtrs = localArray<int> 4
                        let ancilla = localArray<int> 4
                        if localID < 4 then
                            let mutable remOffset = 0
                            if localID > 0 then
                                remOffset <- remainders.[localID - 1]
                            ancilla.[localID] <- max (workGroupNumber * dem - remOffset) 0
                        barrierLocal()
                        if localID < 4 then
                            let mutable next = 0
                            if localID < 3 then
                                next <- ancilla.[localID + 1]
                            let mutable prev = 0
                            if localID > 0 then
                                prev <- remainders.[localID - 1]
                            globalBucketPtrs.[localID] <- min (ancilla.[localID] - next) (remainders.[localID] - prev) +
                                workGroupNumber * localHistogram.[localID] + sectorHistogram.[localID] + sectorPtr
                        barrierLocal()

                        // Compute final local histogram
                        // if localID < dem then
                        //     let h = workGroupNumber * dem + localID
                        //     let mutable idx = 0
                        //     while remainders.[idx] <= h do
                        //         idx <- idx + 1
                        //     if idx < 4 then
                        //         atomic (+) localHistogram.[idx] 1 |> ignore
                        if localID = 0 then
                            let mutable idx = 0
                            for j in workGroupNumber * dem .. (workGroupNumber + 1) * dem - 1 do
                                while idx < 4 && remainders.[idx] <= j do
                                    idx <- idx + 1
                                if idx < 4 then
                                    localHistogram.[idx] <- localHistogram.[idx] + 1

                        // Recompute sum
                        if localID = 0 then
                            sum <- 0
                            for j in 0b00 .. 0b11 do
                                sum <- sum + localHistogram.[j]
                        barrierLocal()

                        // Load data to the local array
                        let keysLocal = localArray<int> workGroupSize
                        let valuesLocal = localArray<'a> workGroupSize
                        if localID < sum then
                            let mutable j = 0
                            let mutable k = localID
                            let mutable m = localHistogram.[0]
                            while k >= m do
                                k <- k - m
                                j <- j + 1
                                m <- localHistogram.[j]
                            keysLocal.[localID] <- keys.[globalBucketPtrs.[j] + k]
                            valuesLocal.[localID] <- values.[globalBucketPtrs.[j] + k]
                        barrierLocal()

                        // Sort local array
                        let actualHistogram = localArray<int> 4
                        let keysLocalAncilla = localArray<int> workGroupSize
                        let bitmap = localArray<int> workGroupSize
                        let offsets = localArray<int> 2
                        if localID < 2 then
                            offsets.[localID] <- 0
                        for j in 0b00 .. 0b11 do
                            // Initialize bitmap
                            let mutable masked = 0
                            if localID < sum then
                                masked <- int (keysLocal.[localID] >>> offset) &&& 0b11
                            if localID < sum && masked = j then
                                bitmap.[localID] <- 1
                            else
                                bitmap.[localID] <- 0
                            barrierLocal()

                            // Exclusive prefix sum
                            let mutable step = 2

                            while step <= workGroupSize do
                                barrierLocal ()

                                if localID < workGroupSize / step then
                                    let i = step * (localID + 1) - 1

                                    let buff = bitmap.[i - (step >>> 1)] + bitmap.[i]
                                    bitmap.[i] <- buff

                                step <- step <<< 1

                            barrierLocal ()

                            if localID = workGroupSize - 1 then
                                let buff = bitmap.[localID]
                                actualHistogram.[j] <- buff
                                offsets.[0] <- offsets.[1]
                                offsets.[1] <- offsets.[1] + buff
                                bitmap.[localID] <- 0

                            step <- workGroupSize

                            while step > 1 do
                                barrierLocal ()

                                if localID < workGroupSize / step then
                                    let i = step * (localID + 1) - 1
                                    let j = i - (step >>> 1)

                                    let tmp = bitmap.[i]
                                    let buff = (+) tmp bitmap.[j]
                                    bitmap.[i] <- buff
                                    bitmap.[j] <- tmp

                                step <- step >>> 1

                            barrierLocal ()

                            // Permute
                            if localID < sum && masked = j then
                                keysLocalAncilla.[bitmap.[localID] + offsets.[0]] <- keysLocal.[localID]

                        // Update data for garbage
                        if localID = 0 then
                            let mutable delta = min actualHistogram.[0] localHistogram.[0]
                            atomic (+) garbage00.[sector] (-delta) |> ignore

                            delta <- min actualHistogram.[1] localHistogram.[1]
                            atomic (+) garbage01.[sector] (-delta) |> ignore

                            delta <- min actualHistogram.[2] localHistogram.[2]
                            atomic (+) garbage10.[sector] (-delta) |> ignore

                            delta <- min actualHistogram.[3] localHistogram.[3]
                            atomic (+) garbage11.[sector] (-delta) |> ignore

                        // Shift local array

                        let oddElements = localArray<int> 4
                        if localID < 4 then
                            let buff = localHistogram.[localID] - actualHistogram.[localID]
                            if buff > 0 then
                                isFinished.Value <- false
                            oddElements.[localID] <- buff
                        barrierLocal()

                        let mutable state = 0
                        let mutable garbagePtr = sum
                        for k in 0 .. 1 do
                            for j in 0 .. 3 do
                                let odd = oddElements.[j]
                                if odd * ((k <<< 1) - 1) > 0 then
                                    // (%changeState)
                                    //     keysLocalAncilla
                                    //     keysLocal
                                    //     actualHistogram
                                    //     sum
                                    //     state
                                    //     j
                                    //     garbagePtr
                                    //     localID

                                    let minState = min state j
                                    let maxState = max state j
                                    let mutable value = 0
                                    for j in minState .. maxState - 1 do
                                        value <- value + actualHistogram.[j]
                                    if state < j then
                                        value <- -value
                                    (%doShift)
                                        keysLocalAncilla
                                        keysLocal
                                        length
                                        0
                                        garbagePtr
                                        value
                                        localID
                                    barrierLocal()

                                    state <- j
                                    let mutable len = 0
                                    let mutable rightEdge = garbagePtr
                                    if k = 1 then
                                        len <- actualHistogram.[state]
                                        rightEdge <- sum
                                    else
                                        len <- localHistogram.[state]
                                    (%doShift)
                                        keysLocal
                                        keysLocalAncilla
                                        sum
                                        len
                                        rightEdge
                                        odd
                                        localID
                                    garbagePtr <- garbagePtr + odd
                                    actualHistogram.[state] <- actualHistogram.[state] + odd
                                    barrierLocal()
                        // (%changeState)
                        //     keysLocalAncilla
                        //     keysLocal
                        //     actualHistogram
                        //     sum
                        //     state
                        //     0
                        //     sum
                        //     localID
                        let mutable value = 0
                        for j in 0 .. state - 1 do
                            value <- value + actualHistogram.[j]
                        (%doShift)
                            keysLocalAncilla
                            keysLocal
                            length
                            0
                            garbagePtr
                            value
                            localID
                        barrierLocal()

                        // Load data to the global array
                        if localID < sum then
                            let mutable j = 0
                            let mutable k = localID
                            let mutable m = localHistogram.[0]
                            while k >= m do
                                k <- k - m
                                j <- j + 1
                                m <- localHistogram.[j]
                            keys.[globalBucketPtrs.[j] + k] <- keysLocal.[localID]
                            values.[globalBucketPtrs.[j] + k] <- valuesLocal.[localID]
                        barrierLocal()
            @>
        let program = clContext.CreateClProgram(permute)

        let copy = ClArray.copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>)
            (keys: ClArray<int>)
            (values: ClArray<'a>)
            (sectorIndices: ClArray<int>)
            (sectorPointers: ClArray<int>)
            (histograms: list<ClArray<int>>)
            (garbage: list<ClArray<int>>)
            (isFinished: ClCell<bool>)
            (globalStep: int) ->

            let sectorsAmount = sectorPointers.Length

            let kernel = program.GetKernel()
            let ndRange = Range1D.CreateValid(keys.Length + (sectorsAmount - 1) * workGroupSize, workGroupSize)
            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.KernelFunc
                            ndRange
                            keys
                            values
                            sectorIndices
                            sectorPointers
                            histograms.[0b00]
                            histograms.[0b01]
                            histograms.[0b10]
                            histograms.[0b11]
                            garbage.[0b00]
                            garbage.[0b01]
                            garbage.[0b10]
                            garbage.[0b11]
                            isFinished
                            (32 - ((globalStep + 1) <<< 1))
                            keys.Length)
            )
            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

    // let private computeGarbage
    //     (clContext: ClContext)
    //     workGroupSize =

    //     let computeGarbage =
    //         <@
    //             fun (range: Range1D)
    //                 (keys: ClArray<int>)
    //                 (sectorIndices: ClArray<int>)
    //                 (sectorPointers: ClArray<int>)
    //                 (histograms00: ClArray<int>)
    //                 (histograms01: ClArray<int>)
    //                 (histograms10: ClArray<int>)
    //                 (histograms11: ClArray<int>)
    //                 (garbageOffsets: ClArray<int>)
    //                 (bucketHeads: ClArray<int>)
    //                 (offset: int)
    //                 (length: int) ->

    //                 let localID = range.LocalID0

    //                 let sectorHistogram = localArray<int> 4

    //                 // // Compute position of the workgroup relative to the array of keys for sector
    //                 // let mutable sector = local<int>()
    //                 // let mutable shift = local<int>()
    //                 // let mutable sectorPtr = local<int>()
    //                 // let mutable sectorEdge = local<int>()
    //                 // let mutable bucket = local<int>()
    //                 // let mutable isAdditional = local<bool>()
    //                 // if localID = 0 then
    //                 //     let i = range.GlobalID0
    //                 //     if i >= length then
    //                 //         // It's additional workgroup
    //                 //         sector <- (i - length) / (4 * workGroupSize)
    //                 //         bucket <- ((i - length) / workGroupSize) % 4
    //                 //         let mutable left = -1
    //                 //         sectorPtr <- 0
    //                 //         if sector > 0 then
    //                 //             sectorPtr <- sectorPointers.[sector - 1] + 1
    //                 //         if sectorPtr > 0 then
    //                 //             left <- (sectorPtr - 1) / workGroupSize
    //                 //         sectorEdge <- sectorPointers.[sector]
    //                 //         shift <- i - (sectorPtr + workGroupSize *
    //                 //             (bucket + sectorEdge / workGroupSize - left))
    //                 //         isAdditional <- true
    //                 //     else
    //                 //         sector <- sectorIndices.[i]
    //                 //         sectorEdge <- sectorPointers.[sector]
    //                 //         if sector = 0 then
    //                 //             shift <- 0
    //                 //             sectorPtr <- 0
    //                 //         else
    //                 //             sectorPtr <- sectorPointers.[sector - 1] + 1
    //                 //             shift <- (i - sectorPtr) % workGroupSize
    //                 //         isAdditional <- false

    //                 //     sectorHistogram.[0b00] <- histograms00.[sector]
    //                 //     sectorHistogram.[0b01] <- histograms01.[sector]
    //                 //     sectorHistogram.[0b10] <- histograms10.[sector]
    //                 //     sectorHistogram.[0b11] <- histograms11.[sector]
    //                 // barrierLocal()
    //                 // let mutable i = range.GlobalID0 - shift
    //                 // let mutable k = i - sectorPtr // k is index within the sector

    //                 // // Compute actual position of the workgroup relative to the array of keys for bucket
    //                 // let mutable bucketEdge = local<int>()
    //                 // let mutable bucketPtr = local<int>()
    //                 // if localID = 0 then
    //                 //     if isAdditional then
    //                 //         // It's additional workgroup
    //                 //         // bucket <- (i - sectorEdge - 1) / workGroupSize
    //                 //         bucketPtr <- 0
    //                 //         for j in 0 .. bucket - 1 do
    //                 //             bucketPtr <- bucketPtr + sectorHistogram.[j]
    //                 //         let mutable left = -1
    //                 //         if bucketPtr > 0 then
    //                 //             left <- (bucketPtr - 1) / workGroupSize
    //                 //         bucketEdge <- bucketPtr + sectorHistogram.[bucket] - 1

    //                 //         let mutable left = -1
    //                 //         if sectorPtr > 0 then
    //                 //             left <- (sectorPtr - 1) / workGroupSize
    //                 //         let workGroupsAmountWithinSector = sectorEdge / workGroupSize - left
    //                 //         let mutable workGroupsAmountBeforeBucket = 0
    //                 //         if bucketPtr > 0 then
    //                 //             workGroupsAmountBeforeBucket <- (bucketPtr - 2) / workGroupSize + 1
    //                 //         if workGroupsAmountWithinSector > 0 && workGroupsAmountBeforeBucket < workGroupsAmountWithinSector then
    //                 //             left <- 0
    //                 //             if bucketPtr > 0 then
    //                 //                 left <- (bucketPtr - 1) / workGroupSize + 1

    //                 //             let rem = min (workGroupsAmountWithinSector - workGroupsAmountBeforeBucket) (bucketEdge / workGroupSize - left + 1)

    //                 //             shift <- k - (bucketPtr + workGroupSize * rem)
    //                 //         else
    //                 //             shift <- k - bucketPtr
    //                 //     else
    //                 //         bucket <- 0
    //                 //         bucketEdge <- sectorHistogram.[0]
    //                 //         while k >= bucketEdge do
    //                 //             bucket <- bucket + 1
    //                 //             bucketEdge <- bucketEdge + sectorHistogram.[bucket]

    //                 //         bucketPtr <- bucketEdge - sectorHistogram.[bucket]
    //                 //         bucketEdge <- bucketEdge - 1
    //                 //         shift <- (k - bucketPtr) % workGroupSize
    //                 // barrierLocal()
    //                 // i <- i - shift
    //                 // k <- k - shift

    //                 // Compute position of the workgroup relative to the array of keys for sector
    //                 let mutable sector = local<int>()
    //                 let mutable shift = local<int>()
    //                 let mutable sectorPtr = local<int>()
    //                 let mutable sectorEdge = local<int>()
    //                 let mutable bucket = local<int>()
    //                 let mutable bucketPtr = local<int>()
    //                 let mutable bucketEdge = local<int>()
    //                 let mutable k = 0 // k is index within the sector
    //                 if localID = 0 then
    //                     let i = range.GlobalID0
    //                     if i >= length then
    //                         // It's additional workgroup
    //                         sector <- (i - length) / (4 * workGroupSize)
    //                         bucket <- ((i - length) / workGroupSize) % 4
    //                         sectorPtr <- 0
    //                         if sector > 0 then
    //                             sectorPtr <- sectorPointers.[sector - 1] + 1

    //                         sectorEdge <- sectorPointers.[sector]

    //                         sectorHistogram.[0b00] <- histograms00.[sector]
    //                         sectorHistogram.[0b01] <- histograms01.[sector]
    //                         sectorHistogram.[0b10] <- histograms10.[sector]
    //                         sectorHistogram.[0b11] <- histograms11.[sector]

    //                         bucketPtr <- 0
    //                         for j in 0 .. bucket - 1 do
    //                             bucketPtr <- bucketPtr + sectorHistogram.[j]
    //                         bucketEdge <- bucketPtr + sectorHistogram.[bucket] - 1

    //                         let mutable left = 0
    //                         if sectorPtr + bucketPtr > 0 then
    //                             left <- (sectorPtr + bucketPtr - 1) / workGroupSize + 1

    //                         let mutable right = -1
    //                         if sectorPtr + bucketEdge > -1 then
    //                             right <- (sectorPtr + bucketEdge) / workGroupSize

    //                         shift <- i - (sectorPtr + bucketPtr + workGroupSize * (right - left + 1))
    //                     else
    //                         sector <- sectorIndices.[i]
    //                         sectorEdge <- sectorPointers.[sector]
    //                         if sector = 0 then
    //                             sectorPtr <- 0
    //                         else
    //                             sectorPtr <- sectorPointers.[sector - 1] + 1

    //                         sectorHistogram.[0b00] <- histograms00.[sector]
    //                         sectorHistogram.[0b01] <- histograms01.[sector]
    //                         sectorHistogram.[0b10] <- histograms10.[sector]
    //                         sectorHistogram.[0b11] <- histograms11.[sector]

    //                         k <- i - sectorPtr

    //                         bucket <- 0
    //                         bucketEdge <- sectorHistogram.[0]
    //                         while k >= bucketEdge do
    //                             bucket <- bucket + 1
    //                             bucketEdge <- bucketEdge + sectorHistogram.[bucket]
    //                         bucketPtr <- bucketEdge - sectorHistogram.[bucket]
    //                         bucketEdge <- bucketEdge - 1

    //                         shift <- (k - bucketPtr) % workGroupSize
    //                 barrierLocal()
    //                 let i = range.GlobalID0 - shift
    //                 k <- i - sectorPtr

    //                 // Point out garbage
    //                 let bitmap = localArray<int> workGroupSize
    //                 if k <= bucketEdge then
    //                     let masked = int (keys.[i] >>> offset) &&& 0b11
    //                     if masked = bucket then
    //                         bitmap.[localID] <- 0
    //                     else
    //                         bitmap.[localID] <- 1
    //                 else
    //                     bitmap.[localID] <- 0
    //                 barrierLocal()

    //                 // Calculate local sum of garbage
    //                 let mutable step = 2

    //                 while step <= workGroupSize do
    //                     barrierLocal ()

    //                     if localID < workGroupSize / step then
    //                         let i = step * (localID + 1) - 1
    //                         bitmap.[i] <- bitmap.[i] + bitmap.[i - (step >>> 1)]

    //                     step <- step <<< 1
    //                 barrierLocal ()

    //                 if localID = 0 then
    //                     let mutable a = 0
    //                     if i > 0 then
    //                         a <- (i - 1) / workGroupSize + 1
    //                     let idx = a + 4 * sector + bucket

    //                     garbageOffsets.[idx] <- bitmap.[workGroupSize - 1]
    //                     // if isAdditional || (bucket = 0 && k + workGroupSize - 1 >= bucketEdge) then
    //                     if k = bucketPtr then
    //                         bucketHeads.[idx] <- 1
    //                     else
    //                         bucketHeads.[idx] <- 0
    //         @>
    //     let program = clContext.CreateClProgram(computeGarbage)

    //     let scan = PrefixSum.byHeadFlagsInclude <@ (+) @> clContext workGroupSize
    //     let zeroCreate = ClArray.zeroCreate clContext workGroupSize

    //     fun (processor: MailboxProcessor<_>)
    //         (keys: ClArray<int>)
    //         (sectorIndices: ClArray<int>)
    //         (sectorPointers: ClArray<int>)
    //         (histograms: list<ClArray<int>>)
    //         (globalStep: int) ->

    //         let sectorsAmount = sectorPointers.Length

    //         let workGroupAmount = (keys.Length - 1) / workGroupSize + 1 + 4 * sectorsAmount
    //         let garbageLengths = zeroCreate processor workGroupAmount
    //         let bucketHeads = zeroCreate processor workGroupAmount

    //         let kernel = program.GetKernel()
    //         let ndRange = Range1D.CreateValid(keys.Length + 4 * sectorsAmount * workGroupSize, workGroupSize)
    //         processor.Post(
    //             Msg.MsgSetArguments
    //                 (fun () ->
    //                     kernel.KernelFunc
    //                         ndRange
    //                         keys
    //                         sectorIndices
    //                         sectorPointers
    //                         histograms.[0b00]
    //                         histograms.[0b01]
    //                         histograms.[0b10]
    //                         histograms.[0b11]
    //                         garbageLengths
    //                         bucketHeads
    //                         (64 - ((globalStep + 1) <<< 1))
    //                         keys.Length)
    //         )
    //         processor.Post(Msg.CreateRunMsg<_, _>(kernel))

    //         // let arr = Array.zeroCreate garbageLengths.Length
    //         // let _ = processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(garbageLengths, arr, ch))
    //         // printfn "garbageLengths \n%A" arr

    //         let garbageOffsets = scan processor bucketHeads garbageLengths 0

    //         // let arr = Array.zeroCreate bucketHeads.Length
    //         // let _ = processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(bucketHeads, arr, ch))
    //         // printfn "bucketHeads \n%A" arr

    //         processor.Post(Msg.CreateFreeMsg<_>(garbageLengths))
    //         processor.Post(Msg.CreateFreeMsg<_>(bucketHeads))

    //         garbageOffsets

    // let private repair
    //     (clContext: ClContext)
    //     workGroupSize =

    //     let repair =
    //         <@
    //             fun (range: Range1D)
    //                 (keys: ClArray<int>)
    //                 (keysAncilla: ClArray<int>)
    //                 (sectorTails: ClArray<int>)
    //                 (sectorIndices: ClArray<int>)
    //                 (sectorPointers: ClArray<int>)
    //                 (histograms00: ClArray<int>)
    //                 (histograms01: ClArray<int>)
    //                 (histograms10: ClArray<int>)
    //                 (histograms11: ClArray<int>)
    //                 (garbage00: ClArray<int>)
    //                 (garbage01: ClArray<int>)
    //                 (garbage10: ClArray<int>)
    //                 (garbage11: ClArray<int>)
    //                 (garbageOffsets: ClArray<int>)
    //                 (offset: int)
    //                 (length: int) ->

    //                 let localID = range.LocalID0

    //                 let sectorHistogram = localArray<int> 4

    //                 // // Compute position of the workgroup relative to the array of keys for sector
    //                 // let mutable sector = local<int>()
    //                 // let mutable shift = local<int>()
    //                 // let mutable sectorPtr = local<int>()
    //                 // let mutable sectorEdge = local<int>()
    //                 // let mutable bucket = local<int>()
    //                 // let mutable isAdditional = local<bool>()
    //                 // if localID = 0 then
    //                 //     let i = range.GlobalID0
    //                 //     if i >= length then
    //                 //         // It's additional workgroup
    //                 //         sector <- (i - length) / (4 * workGroupSize)
    //                 //         bucket <- ((i - length) / workGroupSize) % 4
    //                 //         let mutable left = -1
    //                 //         sectorPtr <- 0
    //                 //         if sector > 0 then
    //                 //             sectorPtr <- sectorPointers.[sector - 1] + 1
    //                 //         if sectorPtr > 0 then
    //                 //             left <- (sectorPtr - 1) / workGroupSize
    //                 //         sectorEdge <- sectorPointers.[sector]
    //                 //         shift <- i - (sectorPtr + workGroupSize *
    //                 //             (bucket + sectorEdge / workGroupSize - left))
    //                 //         isAdditional <- true
    //                 //     else
    //                 //         sector <- sectorIndices.[i]
    //                 //         sectorEdge <- sectorPointers.[sector]
    //                 //         if sector = 0 then
    //                 //             shift <- 0
    //                 //             sectorPtr <- 0
    //                 //         else
    //                 //             sectorPtr <- sectorPointers.[sector - 1] + 1
    //                 //             shift <- (i - sectorPtr) % workGroupSize
    //                 //         isAdditional <- false

    //                 //     sectorHistogram.[0b00] <- histograms00.[sector]
    //                 //     sectorHistogram.[0b01] <- histograms01.[sector]
    //                 //     sectorHistogram.[0b10] <- histograms10.[sector]
    //                 //     sectorHistogram.[0b11] <- histograms11.[sector]
    //                 // barrierLocal()
    //                 // let mutable i = range.GlobalID0 - shift
    //                 // let mutable k = i - sectorPtr // k is index within the sector

    //                 // // Compute actual position of the workgroup relative to the array of keys for bucket
    //                 // let mutable bucketPtr = local<int>()
    //                 // let mutable bucketEdge = local<int>()
    //                 // if localID = 0 then
    //                 //     if isAdditional then
    //                 //         // It's additional workgroup
    //                 //         // bucket <- (i - sectorEdge - 1) / workGroupSize
    //                 //         bucketPtr <- 0
    //                 //         for j in 0 .. bucket - 1 do
    //                 //             bucketPtr <- bucketPtr + sectorHistogram.[j]
    //                 //         bucketEdge <- bucketPtr + sectorHistogram.[bucket] - 1

    //                 //         let mutable left = -1
    //                 //         if sectorPtr > 0 then
    //                 //             left <- (sectorPtr - 1) / workGroupSize
    //                 //         let workGroupsAmountWithinSector = sectorEdge / workGroupSize - left
    //                 //         let mutable workGroupsAmountBeforeBucket = 0
    //                 //         if bucketPtr > 0 then
    //                 //             workGroupsAmountBeforeBucket <- (bucketPtr - 2) / workGroupSize + 1
    //                 //         if workGroupsAmountWithinSector > 0 && workGroupsAmountBeforeBucket < workGroupsAmountWithinSector then
    //                 //             left <- 0
    //                 //             if bucketPtr > 0 then
    //                 //                 left <- (bucketPtr - 1) / workGroupSize + 1

    //                 //             let rem = min (workGroupsAmountWithinSector - workGroupsAmountBeforeBucket) (bucketEdge / workGroupSize - left + 1)

    //                 //             shift <- k - (bucketPtr + workGroupSize * rem)
    //                 //         else
    //                 //             shift <- k - bucketPtr
    //                 //     else
    //                 //         bucket <- 0
    //                 //         bucketEdge <- sectorHistogram.[0]
    //                 //         while k >= bucketEdge do
    //                 //             bucket <- bucket + 1
    //                 //             bucketEdge <- bucketEdge + sectorHistogram.[bucket]

    //                 //         bucketPtr <- bucketEdge - sectorHistogram.[bucket]
    //                 //         bucketEdge <- bucketEdge - 1
    //                 //         shift <- (k - bucketPtr) % workGroupSize
    //                 // barrierLocal()
    //                 // i <- i - shift
    //                 // k <- k - shift

    //                 // Compute position of the workgroup relative to the array of keys for sector
    //                 let mutable sector = local<int>()
    //                 let mutable shift = local<int>()
    //                 let mutable sectorPtr = local<int>()
    //                 let mutable sectorEdge = local<int>()
    //                 let mutable bucket = local<int>()
    //                 let mutable bucketPtr = local<int>()
    //                 let mutable bucketEdge = local<int>()
    //                 let mutable k = 0 // k is index within the sector
    //                 if localID = 0 then
    //                     let i = range.GlobalID0
    //                     if i >= length then
    //                         // It's additional workgroup
    //                         sector <- (i - length) / (4 * workGroupSize)
    //                         bucket <- ((i - length) / workGroupSize) % 4
    //                         sectorPtr <- 0
    //                         if sector > 0 then
    //                             sectorPtr <- sectorPointers.[sector - 1] + 1

    //                         sectorEdge <- sectorPointers.[sector]

    //                         sectorHistogram.[0b00] <- histograms00.[sector]
    //                         sectorHistogram.[0b01] <- histograms01.[sector]
    //                         sectorHistogram.[0b10] <- histograms10.[sector]
    //                         sectorHistogram.[0b11] <- histograms11.[sector]

    //                         bucketPtr <- 0
    //                         for j in 0 .. bucket - 1 do
    //                             bucketPtr <- bucketPtr + sectorHistogram.[j]
    //                         bucketEdge <- bucketPtr + sectorHistogram.[bucket] - 1

    //                         let mutable left = 0
    //                         if sectorPtr + bucketPtr > 0 then
    //                             left <- (sectorPtr + bucketPtr - 1) / workGroupSize + 1

    //                         let mutable right = -1
    //                         if sectorPtr + bucketEdge > -1 then
    //                             right <- (sectorPtr + bucketEdge) / workGroupSize

    //                         shift <- i - (sectorPtr + bucketPtr + workGroupSize * (right - left + 1))
    //                     else
    //                         sector <- sectorIndices.[i]
    //                         sectorEdge <- sectorPointers.[sector]
    //                         if sector = 0 then
    //                             sectorPtr <- 0
    //                         else
    //                             sectorPtr <- sectorPointers.[sector - 1] + 1

    //                         sectorHistogram.[0b00] <- histograms00.[sector]
    //                         sectorHistogram.[0b01] <- histograms01.[sector]
    //                         sectorHistogram.[0b10] <- histograms10.[sector]
    //                         sectorHistogram.[0b11] <- histograms11.[sector]

    //                         k <- i - sectorPtr

    //                         bucket <- 0
    //                         bucketEdge <- sectorHistogram.[0]
    //                         while k >= bucketEdge do
    //                             bucket <- bucket + 1
    //                             bucketEdge <- bucketEdge + sectorHistogram.[bucket]
    //                         bucketPtr <- bucketEdge - sectorHistogram.[bucket]
    //                         bucketEdge <- bucketEdge - 1

    //                         shift <- (k - bucketPtr) % workGroupSize
    //                 barrierLocal()
    //                 let i = range.GlobalID0 - shift
    //                 k <- i - sectorPtr

    //                 // Update tails
    //                 if localID = 0 then
    //                     sectorTails.[sectorPtr + bucketEdge] <- 1
    //                 barrierLocal()

    //                 // Point out garbage
    //                 let bitmap = localArray<int> workGroupSize
    //                 let mutable masked = 0
    //                 let mutable key = 0UL
    //                 if k <= bucketEdge then
    //                     key <- keys.[i]
    //                     masked <- int (key >>> offset) &&& 0b11
    //                     if masked = bucket then
    //                         bitmap.[localID] <- 0
    //                     else
    //                         bitmap.[localID] <- 1
    //                 else
    //                     bitmap.[localID] <- 0
    //                 barrierLocal()

    //                 // Exclusive prefix sum
    //                 let mutable step = 2

    //                 while step <= workGroupSize do
    //                     barrierLocal ()

    //                     if localID < workGroupSize / step then
    //                         let i = step * (localID + 1) - 1

    //                         let buff = bitmap.[i - (step >>> 1)] + bitmap.[i]
    //                         bitmap.[i] <- buff

    //                     step <- step <<< 1

    //                 barrierLocal ()

    //                 if localID = workGroupSize - 1 then
    //                     bitmap.[localID] <- 0

    //                 step <- workGroupSize

    //                 while step > 1 do
    //                     barrierLocal ()

    //                     if localID < workGroupSize / step then
    //                         let i = step * (localID + 1) - 1
    //                         let j = i - (step >>> 1)

    //                         let tmp = bitmap.[i]
    //                         let buff = (+) tmp bitmap.[j]
    //                         bitmap.[i] <- buff
    //                         bitmap.[j] <- tmp

    //                     step <- step >>> 1

    //                 barrierLocal()

    //                 // Load offset
    //                 let mutable offset = local<int>()
    //                 if localID = 0 then
    //                     if k = bucketPtr then
    //                         offset <- 0
    //                     else
    //                         let mutable a = 0
    //                         if i > 0 then
    //                             a <- (i - 1) / workGroupSize + 1
    //                         let idx = a + 4 * sector + bucket

    //                         offset <- garbageOffsets.[idx - 1]
    //                 barrierLocal()

    //                 let mutable sum = local<int>()
    //                 if bucket = 0 then
    //                     sum <- garbage00.[sector]
    //                 if bucket = 1 then
    //                     sum <- garbage01.[sector]
    //                 if bucket = 2 then
    //                     sum <- garbage10.[sector]
    //                 if bucket = 3 then
    //                     sum <- garbage11.[sector]
    //                 barrierLocal()

    //                 // Repair
    //                 if k <= bucketEdge then
    //                     if masked <> bucket then
    //                         // It's garbage

    //                         keysAncilla.[sectorPtr + bucketPtr + offset + bitmap.[localID]] <- key
    //                     else
    //                         // It's not garbage

    //                         // Workgroup number within the bucket
    //                         let workGroupNumber = (k - bucketPtr) / workGroupSize
    //                         let idx =
    //                             sectorPtr + bucketPtr + sum + (workGroupSize * workGroupNumber - offset) +
    //                             localID - bitmap.[localID]

    //                         keysAncilla.[idx] <- key
    //         @>
    //     let program = clContext.CreateClProgram(repair)

    //     fun (processor: MailboxProcessor<_>)
    //         (keys: ClArray<int>)
    //         (keysAncilla: ClArray<int>)
    //         (sectorTails: ClArray<int>)
    //         (sectorIndices: ClArray<int>)
    //         (sectorPointers: ClArray<int>)
    //         (histograms: list<ClArray<int>>)
    //         (garbage: list<ClArray<int>>)
    //         (garbageOffsets: ClArray<int>)
    //         (globalStep: int) ->

    //         let sectorsAmount = sectorPointers.Length

    //         let kernel = program.GetKernel()
    //         let ndRange = Range1D.CreateValid(keys.Length + 4 * sectorsAmount * workGroupSize, workGroupSize)
    //         processor.Post(
    //             Msg.MsgSetArguments
    //                 (fun () ->
    //                     kernel.KernelFunc
    //                         ndRange
    //                         keys
    //                         keysAncilla
    //                         sectorTails
    //                         sectorIndices
    //                         sectorPointers
    //                         histograms.[0b00]
    //                         histograms.[0b01]
    //                         histograms.[0b10]
    //                         histograms.[0b11]
    //                         garbage.[0b00]
    //                         garbage.[0b01]
    //                         garbage.[0b10]
    //                         garbage.[0b11]
    //                         garbageOffsets
    //                         (64 - ((globalStep + 1) <<< 1))
    //                         keys.Length)
    //         )
    //         processor.Post(Msg.CreateRunMsg<_, _>(kernel))

    let private computeGarbage
        (clContext: ClContext)
        workGroupSize =

        let computeGarbage =
            <@
                fun (range: Range1D)
                    (keys: ClArray<int>)
                    (sectorIndices: ClArray<int>)
                    (sectorPointers: ClArray<int>)
                    (histograms00: ClArray<int>)
                    (histograms01: ClArray<int>)
                    (histograms10: ClArray<int>)
                    (histograms11: ClArray<int>)
                    (garbageOffsets: ClArray<int>)
                    (bucketHeads: ClArray<int>)
                    (offset: int)
                    (length: int) ->

                    let localID = range.LocalID0
                    let i = range.GlobalID0

                    let mutable sector = 0
                    if i < length then
                        sector <- sectorIndices.[i]

                    let mutable sectorPtr = 0
                    if sector > 0 then
                        sectorPtr <- sectorPointers.[sector - 1] + 1

                    let mutable bucket = 0
                    let mutable bucketPtr = 0
                    let mutable bucketEdge = histograms00.[sector] - 1
                    if bucket = 0 then
                        if i - sectorPtr > bucketEdge then
                            bucket <- bucket + 1
                            bucketPtr <- bucketEdge + 1
                            bucketEdge <- bucketEdge + histograms01.[sector]

                    if bucket = 1 then
                        if i - sectorPtr > bucketEdge then
                            bucket <- bucket + 1
                            bucketPtr <- bucketEdge + 1
                            bucketEdge <- bucketEdge + histograms10.[sector]

                    if bucket = 2 then
                        if i - sectorPtr > bucketEdge then
                            bucket <- bucket + 1
                            bucketPtr <- bucketEdge + 1
                            bucketEdge <- bucketEdge + histograms11.[sector]

                    if i < length then
                        if int (keys.[i] >>> offset) &&& 0b11 = bucket then
                            garbageOffsets.[i] <- 0
                        else
                            garbageOffsets.[i] <- 1

                        if i = sectorPtr + bucketPtr then
                            bucketHeads.[i] <- 1
                        else
                            bucketHeads.[i] <- 0
            @>
        let program = clContext.CreateClProgram(computeGarbage)

        let scan = PrefixSum.byHeadFlagsInclude <@ (+) @> clContext workGroupSize
        let zeroCreate = ClArray.zeroCreate clContext workGroupSize

        fun (processor: MailboxProcessor<_>)
            (keys: ClArray<int>)
            (sectorIndices: ClArray<int>)
            (sectorPointers: ClArray<int>)
            (histograms: list<ClArray<int>>)
            (globalStep: int) ->

            let garbageLengths = zeroCreate processor keys.Length
            let bucketHeads = zeroCreate processor keys.Length

            let kernel = program.GetKernel()
            let ndRange = Range1D.CreateValid(keys.Length, workGroupSize)
            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.KernelFunc
                            ndRange
                            keys
                            sectorIndices
                            sectorPointers
                            histograms.[0b00]
                            histograms.[0b01]
                            histograms.[0b10]
                            histograms.[0b11]
                            garbageLengths
                            bucketHeads
                            (32 - ((globalStep + 1) <<< 1))
                            keys.Length)
            )
            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            let garbageOffsets = scan processor bucketHeads garbageLengths 0

            processor.Post(Msg.CreateFreeMsg<_>(garbageLengths))
            processor.Post(Msg.CreateFreeMsg<_>(bucketHeads))

            garbageOffsets

    let private repair
        (clContext: ClContext)
        workGroupSize =

        let repair =
            <@
                fun (range: Range1D)
                    (keys: ClArray<int>)
                    (values: ClArray<'a>)
                    (keysAncilla: ClArray<int>)
                    (valuesAncilla: ClArray<'a>)
                    (sectorTails: ClArray<int>)
                    (sectorIndices: ClArray<int>)
                    (sectorPointers: ClArray<int>)
                    (histograms00: ClArray<int>)
                    (histograms01: ClArray<int>)
                    (histograms10: ClArray<int>)
                    (histograms11: ClArray<int>)
                    (garbage00: ClArray<int>)
                    (garbage01: ClArray<int>)
                    (garbage10: ClArray<int>)
                    (garbage11: ClArray<int>)
                    (garbageOffsets: ClArray<int>)
                    (offset: int)
                    (length: int) ->

                    let i = range.GlobalID0

                    let mutable sector = 0
                    if i < length then
                        sector <- sectorIndices.[i]

                    let mutable sectorPtr = 0
                    if sector > 0 then
                        sectorPtr <- sectorPointers.[sector - 1] + 1

                    let mutable bucket = 0
                    let mutable bucketPtr = 0
                    let mutable bucketEdge = histograms00.[sector] - 1
                    if bucket = 0 then
                        if i - sectorPtr > bucketEdge then
                            bucket <- bucket + 1
                            bucketPtr <- bucketEdge + 1
                            bucketEdge <- bucketEdge + histograms01.[sector]

                    if bucket = 1 then
                        if i - sectorPtr > bucketEdge then
                            bucket <- bucket + 1
                            bucketPtr <- bucketEdge + 1
                            bucketEdge <- bucketEdge + histograms10.[sector]

                    if bucket = 2 then
                        if i - sectorPtr > bucketEdge then
                            bucket <- bucket + 1
                            bucketPtr <- bucketEdge + 1
                            bucketEdge <- bucketEdge + histograms11.[sector]

                    // Update tails
                    if i - sectorPtr = bucketEdge then
                        sectorTails.[i] <- 1

                    let mutable sum = 0
                    if bucket = 0 then
                        sum <- garbage00.[sector]
                    if bucket = 1 then
                        sum <- garbage01.[sector]
                    if bucket = 2 then
                        sum <- garbage10.[sector]
                    if bucket = 3 then
                        sum <- garbage11.[sector]

                    // Repair
                    if i < length then
                        let mutable idx = 0
                        if i <> sectorPtr + bucketPtr then
                            idx <- garbageOffsets.[i - 1]
                        if int (keys.[i] >>> offset) &&& 0b11 = bucket then
                            keysAncilla.[sectorPtr + bucketPtr + sum + i - sectorPtr - bucketPtr - idx] <- keys.[i]
                            valuesAncilla.[sectorPtr + bucketPtr + sum + i - sectorPtr - bucketPtr - idx] <- values.[i]
                        else
                            keysAncilla.[sectorPtr + bucketPtr + idx] <- keys.[i]
                            valuesAncilla.[sectorPtr + bucketPtr + idx] <- values.[i]
            @>
        let program = clContext.CreateClProgram(repair)

        fun (processor: MailboxProcessor<_>)
            (keys: ClArray<int>)
            (values: ClArray<'a>)
            (keysAncilla: ClArray<int>)
            (valuesAncilla: ClArray<'a>)
            (sectorTails: ClArray<int>)
            (sectorIndices: ClArray<int>)
            (sectorPointers: ClArray<int>)
            (histograms: list<ClArray<int>>)
            (garbage: list<ClArray<int>>)
            (garbageOffsets: ClArray<int>)
            (globalStep: int) ->

            let kernel = program.GetKernel()
            let ndRange = Range1D.CreateValid(keys.Length, workGroupSize)
            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.KernelFunc
                            ndRange
                            keys
                            values
                            keysAncilla
                            valuesAncilla
                            sectorTails
                            sectorIndices
                            sectorPointers
                            histograms.[0b00]
                            histograms.[0b01]
                            histograms.[0b10]
                            histograms.[0b11]
                            garbage.[0b00]
                            garbage.[0b01]
                            garbage.[0b10]
                            garbage.[0b11]
                            garbageOffsets
                            (32 - ((globalStep + 1) <<< 1))
                            keys.Length)
            )
            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

    let private takeStep
        (clContext: ClContext)
        workGroupSize =

        let permute = permute clContext workGroupSize
        let computeGarbage = computeGarbage clContext workGroupSize
        let repair = repair clContext workGroupSize

        fun (processor: MailboxProcessor<_>)
            (keys: ClArray<int>)
            (values: ClArray<'a>)
            (keysAncilla: ClArray<int>)
            (valuesAncilla: ClArray<'a>)
            (sectorTails: ClArray<int>)
            (sectorIndices: ClArray<int>)
            (sectorPointers: ClArray<int>)
            (histograms: list<ClArray<int>>)
            (garbage: list<ClArray<int>>)
            (isFinished: ClCell<bool>)
            (globalStep: int) ->

            permute processor keys values sectorIndices sectorPointers histograms garbage isFinished globalStep

            let garbageOffsets = computeGarbage processor keys sectorIndices sectorPointers histograms globalStep

            repair processor keys values keysAncilla valuesAncilla sectorTails sectorIndices sectorPointers histograms garbage garbageOffsets globalStep

            processor.Post(Msg.CreateFreeMsg<_>(garbageOffsets))

    let run
        (clContext: ClContext)
        workGroupSize =

        let takeStep = takeStep clContext workGroupSize
        let createHistograms = createHistograms clContext workGroupSize
        let copy = ClArray.copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>)
            (keys: ClArray<int>)
            (values: ClArray<'a>)
            (sectorTails: ClArray<int>)
            (maxValue: int) ->

            let mutable log = 0
            while log < 32 && (1 <<< log) <= maxValue do
                log <- log + 1

            let keysAncilla = clContext.CreateClArray(keys.Length)
            let valuesAncilla = clContext.CreateClArray(values.Length)
            let mutable keyArrays = keys, keysAncilla
            let mutable valuesArrays = values, valuesAncilla

            for globalStep in (32 - log) / 2 .. 32 / 2 - 1 do
                let sectorIndices, sectorPointers, histograms = createHistograms processor (fst keyArrays) sectorTails globalStep

                let garbage =
                    [0..3]
                    |> List.map (fun i ->
                        copy processor histograms.[i]
                        // clContext.CreateClArray(
                        //     sectorsAmount,
                        //     hostAccessMode = HostAccessMode.NotAccessible,
                        //     deviceAccessMode = DeviceAccessMode.ReadWrite
                        // )
                    )

                let mutable isFinished = false
                while not isFinished do
                    let firstKeys = fst keyArrays
                    let mutable secondKeys = snd keyArrays

                    let firstValues = fst valuesArrays
                    let mutable secondValues = snd valuesArrays

                    isFinished <- true
                    let isFinishedGPU = clContext.CreateClCell<bool>(isFinished)

                    takeStep processor firstKeys firstValues secondKeys secondValues sectorTails sectorIndices sectorPointers histograms garbage isFinishedGPU globalStep

                    isFinished <-
                        ClCell.toHost isFinishedGPU
                        |> ClTask.runSync clContext
                    processor.Post(Msg.CreateFreeMsg<_>(isFinishedGPU))

                    keyArrays <- secondKeys, firstKeys
                    valuesArrays <- secondValues, firstValues

                histograms
                |> List.iter (fun hist ->
                    processor.Post(Msg.CreateFreeMsg<_>(hist))
                )
                garbage
                |> List.iter (fun gar ->
                    processor.Post(Msg.CreateFreeMsg<_>(gar))
                )
                processor.Post(Msg.CreateFreeMsg<_>(sectorIndices))
                processor.Post(Msg.CreateFreeMsg<_>(sectorPointers))

            processor.Post(Msg.CreateFreeMsg<_>(snd keyArrays))
            processor.Post(Msg.CreateFreeMsg<_>(snd valuesArrays))

            fst keyArrays, fst valuesArrays

