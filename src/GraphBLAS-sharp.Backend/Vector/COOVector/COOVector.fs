namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Common
open Microsoft.FSharp.Control
open Microsoft.FSharp.Quotations

module COOVector =
    let private merge<'a, 'b when 'a: struct and 'b: struct> (clContext: ClContext) (workGroupSize: int) =

        let merge =
            <@
                fun (ndRange: Range1D) (firstSide: int) (secondSide: int) (sumOfSides: int) (firstIndicesBuffer: ClArray<int>) (firstValuesBuffer: ClArray<'a>) (secondIndicesBuffer: ClArray<int>) (secondValuesBuffer: ClArray<'b>) (allIndicesBuffer: ClArray<int>) (firstResultValues: ClArray<'a>) (secondResultValues: ClArray<'b>) (isLeftBitMap: ClArray<int>) ->

                    let i = ndRange.GlobalID0

                    let mutable beginIdxLocal = local ()
                    let mutable endIdxLocal = local ()
                    let localID = ndRange.LocalID0

                    if localID < 2 then
                        let mutable x = localID * (workGroupSize - 1) + i - 1

                        if x >= sumOfSides then
                            x <- sumOfSides - 1

                        let diagonalNumber = x

                        let mutable leftEdge = diagonalNumber + 1 - secondSide
                        if leftEdge < 0 then leftEdge <- 0

                        let mutable rightEdge = firstSide - 1

                        if rightEdge > diagonalNumber then
                            rightEdge <- diagonalNumber

                        while leftEdge <= rightEdge do
                            let middleIdx = (leftEdge + rightEdge) / 2
                            let firstIndex = firstIndicesBuffer[middleIdx]

                            let secondIndex =
                                secondIndicesBuffer[diagonalNumber - middleIdx]

                            if firstIndex <= secondIndex then
                                leftEdge <- middleIdx + 1
                            else
                                rightEdge <- middleIdx - 1

                        // Here localID equals either 0 or 1
                        if localID = 0 then
                            beginIdxLocal <- leftEdge
                        else
                            endIdxLocal <- leftEdge

                    barrierLocal ()

                    let beginIdx = beginIdxLocal
                    let endIdx = endIdxLocal
                    let firstLocalLength = endIdx - beginIdx
                    let mutable x = workGroupSize - firstLocalLength

                    if endIdx = firstSide then
                        x <- secondSide - i + localID + beginIdx

                    let secondLocalLength = x

                    //First indices are from 0 to firstLocalLength - 1 inclusive
                    //Second indices are from firstLocalLength to firstLocalLength + secondLocalLength - 1 inclusive
                    let localIndices = localArray<int> workGroupSize

                    if localID < firstLocalLength then
                        localIndices[localID] <- firstIndicesBuffer[beginIdx + localID]

                    if localID < secondLocalLength then
                        localIndices[firstLocalLength + localID] <- secondIndicesBuffer[i - beginIdx]

                    barrierLocal ()

                    if i < sumOfSides then
                        let mutable leftEdge = localID + 1 - secondLocalLength
                        if leftEdge < 0 then leftEdge <- 0

                        let mutable rightEdge = firstLocalLength - 1

                        if rightEdge > localID then
                            rightEdge <- localID

                        while leftEdge <= rightEdge do
                            let middleIdx = (leftEdge + rightEdge) / 2
                            let firstIndex = localIndices[middleIdx]

                            let secondIndex =
                                localIndices[firstLocalLength + localID - middleIdx]

                            if firstIndex <= secondIndex then
                                leftEdge <- middleIdx + 1
                            else
                                rightEdge <- middleIdx - 1

                        let boundaryX = rightEdge
                        let boundaryY = localID - leftEdge

                        // boundaryX and boundaryY can't be off the right edge of array (only off the left edge)
                        let isValidX = boundaryX >= 0
                        let isValidY = boundaryY >= 0

                        let mutable fstIdx = 0

                        if isValidX then
                            fstIdx <- localIndices[boundaryX]

                        let mutable sndIdx = 0

                        if isValidY then
                            sndIdx <- localIndices[firstLocalLength + boundaryY]

                        if not isValidX || isValidY && fstIdx <= sndIdx then
                            allIndicesBuffer[i] <- sndIdx
                            secondResultValues[i] <- secondValuesBuffer[i - localID - beginIdx + boundaryY]
                            isLeftBitMap[i] <- 0
                        else
                            allIndicesBuffer[i] <- fstIdx
                            firstResultValues[i] <- firstValuesBuffer[beginIdx + boundaryX]
                            isLeftBitMap[i] <- 1
            @>

        let kernel = clContext.Compile(merge)

        fun (processor: MailboxProcessor<_>) (firstIndices: ClArray<int>) (firstValues: ClArray<'a>) (secondIndices: ClArray<int>) (secondValues: ClArray<'b>) ->

            let firstSide = firstIndices.Length

            let secondSide = secondIndices.Length

            let sumOfSides = firstIndices.Length + secondIndices.Length

            let allIndices =
                clContext.CreateClArray<int>(
                    sumOfSides,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.ReadWrite,
                    allocationMode = AllocationMode.Default
                )

            let firstResultValues =
                clContext.CreateClArray<'a>(
                    sumOfSides,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.ReadWrite,
                    allocationMode = AllocationMode.Default
                )

            let secondResultValues =
                clContext.CreateClArray<'b>(
                    sumOfSides,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.ReadWrite,
                    allocationMode = AllocationMode.Default
                )

            let isLeftBitmap =
                clContext.CreateClArray<int>(
                    sumOfSides,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.ReadWrite,
                    allocationMode = AllocationMode.Default
                )

            let ndRange = Range1D.CreateValid(sumOfSides, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments(
                    fun () ->
                        kernel.KernelFunc
                            ndRange
                            firstSide
                            secondSide
                            sumOfSides
                            firstIndices
                            firstValues
                            secondIndices
                            secondValues
                            allIndices
                            firstResultValues
                            secondResultValues
                            isLeftBitmap)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            allIndices, firstResultValues, secondResultValues, isLeftBitmap

    let private preparePositionsAtLeasOne<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct>
        (clContext: ClContext)
        (opAdd: Expr<AtLeastOne<'a, 'b> -> 'c option>)
        (workGroupSize: int)
        =

        let preparePositions =
            <@
                fun (ndRange: Range1D) length (allIndices: ClArray<int>) (leftValues: ClArray<'a>) (rightValues: ClArray<'b>) (isLeft: ClArray<int>) (allValues: ClArray<'c>) (positions: ClArray<int>) ->

                    let gid = ndRange.GlobalID0

                    if gid < length - 1 && allIndices[gid] = allIndices[gid + 1] then
                        positions[gid] <- 0

                        match (%opAdd) (Both (leftValues[gid], rightValues[gid + 1])) with
                        | Some value ->
                            allValues[gid + 1] <- value
                            positions[gid + 1] <- 1
                        | None ->
                            positions[gid + 1] <- 0
                    elif (gid < length && gid > 0 && allIndices[gid - 1] <> allIndices[gid]) || gid = 0 then
                        if isLeft[gid] = 1 then
                            match (%opAdd) (Left leftValues[gid]) with
                            | Some value ->
                                allValues[gid] <- value
                                positions[gid] <- 1
                            | None ->
                                positions[gid] <- 0
                        else
                            match (%opAdd) (Right rightValues[gid]) with
                            | Some value ->
                                allValues[gid] <- value
                                positions[gid] <- 1
                            | None ->
                                positions[gid] <- 0
            @>

        let kernel = clContext.Compile(preparePositions)

        fun (processor: MailboxProcessor<_>) (allIndices: ClArray<int>) (leftValues: ClArray<'a>) (rightValues: ClArray<'b>) (isLeft: ClArray<int>) ->

            let length = allIndices.Length

            let allValues =
                clContext.CreateClArray(
                    length,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.ReadWrite,
                    allocationMode = AllocationMode.Default
                )

            let positions =
                clContext.CreateClArray(
                    length,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.ReadWrite,
                    allocationMode = AllocationMode.Default
                )

            let ndRange = Range1D.CreateValid(length, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments(
                    fun () ->
                        kernel.KernelFunc
                            ndRange
                            length
                            allIndices
                            leftValues
                            rightValues
                            isLeft
                            allValues
                            positions)
                )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            allValues, positions

    let setPositions (clContext: ClContext) (workGroupSize: int) =

        let setPositions =
            <@
                fun (ndRange: Range1D) prefixSumArrayLength (allValues: ClArray<'a>) (allIndices: ClArray<int>) (prefixSumBuffer: ClArray<int>) (resultValues: ClArray<'a>) (resultIndices: ClArray<int>) ->

                    let i = ndRange.GlobalID0

                    if i = prefixSumArrayLength - 1
                       || i < prefixSumArrayLength
                          && prefixSumBuffer[i]
                             <> prefixSumBuffer[i + 1] then
                        let index = prefixSumBuffer[i]

                        resultValues[index] <- allValues[i]
                        resultIndices[index] <- allIndices[i]
            @>

        let kernel = clContext.Compile(setPositions)

        let sum =
            ClArray.prefixSumExcludeInplace clContext workGroupSize

        let resultLength = Array.zeroCreate 1

        fun (processor: MailboxProcessor<_>) (allValues: ClArray<'a>) (allIndices: ClArray<int>) (positions: ClArray<int>) ->

            let prefixSumArrayLength = positions.Length

            let resultLengthGpu = clContext.CreateClCell 0

            let _, r = sum processor positions resultLengthGpu

            let resultLength =
                let res =
                    processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(r, resultLength, ch))

                processor.Post(Msg.CreateFreeMsg<_>(r))

                res[0]

            let resultValues =
                clContext.CreateClArray<'a>(
                    resultLength,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.ReadWrite,
                    allocationMode = AllocationMode.Default
                )

            let resultIndices =
                clContext.CreateClArray<int>(
                    resultLength,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.ReadWrite,
                    allocationMode = AllocationMode.Default
                )

            let ndRange = Range1D.CreateValid(prefixSumArrayLength, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.KernelFunc
                            ndRange
                            prefixSumArrayLength
                            allValues
                            allIndices
                            positions
                            resultValues
                            resultIndices)
                )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            resultValues, resultIndices

    ///<param name="clContext">.</param>
    ///<param name="opAdd">.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let elementWiseAddAtLeastOne<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct>
        (clContext: ClContext)
        (opAdd: Expr<AtLeastOne<'a, 'b> -> 'c option>)
        (workGroupSize: int)
        =

        let merge = merge clContext workGroupSize

        let prepare = preparePositionsAtLeasOne clContext opAdd workGroupSize

        let setPositions = setPositions clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (leftVector: ClCooVector<'a>) (rightVector: ClCooVector<'b>) ->

            let allIndices, leftValues, rightValues, isLeft =
                merge
                    processor
                    leftVector.Indices
                    leftVector.Values
                    rightVector.Indices
                    rightVector.Values

            let allValues, positions =
                prepare
                    processor
                    allIndices
                    leftValues
                    rightValues
                    isLeft

            processor.Post(Msg.CreateFreeMsg<_>(leftValues))
            processor.Post(Msg.CreateFreeMsg<_>(rightValues))
            processor.Post(Msg.CreateFreeMsg<_>(isLeft))

            let resultValues, resultIndices =
                setPositions
                    processor
                    allValues
                    allIndices
                    positions

            processor.Post(Msg.CreateFreeMsg<_>(allIndices))
            processor.Post(Msg.CreateFreeMsg<_>(allValues))
            processor.Post(Msg.CreateFreeMsg<_>(positions))

            { ClCooVector.Context = clContext
              Values = resultValues
              Indices = resultIndices
              Size = leftVector.Size }

    ///<param name="clContext">.</param>
    ///<param name="opAdd">.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let fillSubVector (clContext: ClContext) (workGroupSize: int) (zero: 'a) =

        let create = ClArray.create clContext workGroupSize

        let opAdd = StandardOperations.maks zero

        let eWiseAdd = elementWiseAddAtLeastOne clContext opAdd workGroupSize

        fun (processor: MailboxProcessor<_>) (leftVector: ClCooVector<'a>) (maskVector: ClCooVector<'b>) (scalar: 'a) ->

            let maskValues = create processor maskVector.Size scalar

            let maskIndices = maskVector.Indices

            let rightVector =
                { ClCooVector.Context = clContext
                  Indices = maskIndices
                  Values = maskValues
                  Size = maskVector.Size } //TODO()

            eWiseAdd processor leftVector rightVector

    let preparePositionsComplemented (clContext: ClContext) (workGroupSize: int) =

        let preparePositions =
            <@
                fun (ndRange: Range1D) indicesArrayLength (inputIndices: ClArray<int>) (positions: ClArray<int>) ->

                    let gid = ndRange.GlobalID0

                    if gid < indicesArrayLength then
                        let index = inputIndices[gid]

                        positions[index] <- 0
            @> //TODO

        let kernel = clContext.Compile(preparePositions)

        let creat =  ClArray.create clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (inputIndices: ClArray<int>) (vectorSize: int) ->

            let positions = creat processor vectorSize 1

            let ndRange = Range1D.CreateValid(inputIndices.Length, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments(
                    fun () ->
                        kernel.KernelFunc
                            ndRange
                            inputIndices.Length
                            inputIndices
                            positions)
                )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            positions

    let complemented<'a when 'a: struct> (clContext: ClContext) (workGroupSize: int) =

        let preparePositions =
            preparePositionsComplemented clContext workGroupSize

        let init =
            ClArray.init <@ fun x -> x @> clContext workGroupSize //TODO remove lambda ?

        let create =
            ClArray.zeroCreate clContext workGroupSize

        let setPositions =
            setPositions clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (vector: ClCooVector<'a>) ->

            let positions =
                preparePositions processor vector.Indices vector.Size

            let allIndices =
                init processor vector.Size

            let (values: ClArray<'a>) = create processor vector.Size //TODO()

            let resultValues, resultIndices =
                setPositions processor values allIndices positions

            processor.Post(Msg.CreateFreeMsg<_>(positions))
            processor.Post(Msg.CreateFreeMsg<_>(allIndices))

            { ClCooVector.Context = clContext
              Indices = resultIndices
              Values = resultValues
              Size = vector.Size }

    let reduce
        (clContext: ClContext)
        (workGroupSize: int)
        (opAdd: Expr<'a -> 'a -> 'a>)
        zero
        =

        let reduce = Reduce.run clContext workGroupSize opAdd zero

        fun (processor: MailboxProcessor<_>) (vector: ClCooVector<'a>) ->
            reduce processor vector.Values
