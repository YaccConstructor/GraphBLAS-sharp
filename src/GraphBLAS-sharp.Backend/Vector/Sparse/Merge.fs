namespace GraphBLAS.FSharp.Backend.Vector.Sparse

open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Objects.ClContextExtensions

module internal Merge =
    let run<'a, 'b when 'a: struct and 'b: struct> (clContext: ClContext) workGroupSize =

        let merge =
            <@ fun (ndRange: Range1D) (firstSide: int) (secondSide: int) (sumOfSides: int) (firstIndicesBuffer: ClArray<int>) (firstValuesBuffer: ClArray<'a>) (secondIndicesBuffer: ClArray<int>) (secondValuesBuffer: ClArray<'b>) (allIndicesBuffer: ClArray<int>) (firstResultValues: ClArray<'a>) (secondResultValues: ClArray<'b>) (isLeftBitMap: ClArray<int>) ->

                let gid = ndRange.GlobalID0
                let lid = ndRange.LocalID0

                let mutable beginIdxLocal = local ()
                let mutable endIdxLocal = local ()

                if lid < 2 then
                    // (n - 1) * wgSize - 1 for lid = 0
                    // n * wgSize - 1 for lid = 1
                    // where n in 1 .. wgGroupCount
                    let x = lid * (workGroupSize - 1) + gid - 1

                    let diagonalNumber = min (sumOfSides - 1) x

                    let mutable leftEdge = max 0 (diagonalNumber + 1 - secondSide)

                    let mutable rightEdge = min (firstSide - 1) diagonalNumber

                    while leftEdge <= rightEdge do
                        let middleIdx = (leftEdge + rightEdge) / 2

                        let firstIndex = firstIndicesBuffer.[middleIdx]

                        let secondIndex =
                            secondIndicesBuffer.[diagonalNumber - middleIdx]

                        if firstIndex <= secondIndex then
                            leftEdge <- middleIdx + 1
                        else
                            rightEdge <- middleIdx - 1

                    // Here localID equals either 0 or 1
                    if lid = 0 then
                        beginIdxLocal <- leftEdge
                    else
                        endIdxLocal <- leftEdge

                barrierLocal ()

                let beginIdx = beginIdxLocal
                let endIdx = endIdxLocal
                let firstLocalLength = endIdx - beginIdx
                let mutable x = workGroupSize - firstLocalLength

                if endIdx = firstSide then
                    x <- secondSide - gid + lid + beginIdx

                let secondLocalLength = x

                //First indices are from 0 to firstLocalLength - 1 inclusive
                //Second indices are from firstLocalLength to firstLocalLength + secondLocalLength - 1 inclusive
                let localIndices = localArray<int> workGroupSize

                if lid < firstLocalLength then
                    localIndices.[lid] <- firstIndicesBuffer.[beginIdx + lid]

                if lid < secondLocalLength then
                    localIndices.[firstLocalLength + lid] <- secondIndicesBuffer.[gid - beginIdx]

                barrierLocal ()

                if gid < sumOfSides then
                    let mutable leftEdge = lid + 1 - secondLocalLength
                    if leftEdge < 0 then leftEdge <- 0

                    let mutable rightEdge = firstLocalLength - 1

                    rightEdge <- min rightEdge lid

                    while leftEdge <= rightEdge do
                        let middleIdx = (leftEdge + rightEdge) / 2
                        let firstIndex = localIndices.[middleIdx]

                        let secondIndex =
                            localIndices.[firstLocalLength + lid - middleIdx]

                        if firstIndex <= secondIndex then
                            leftEdge <- middleIdx + 1
                        else
                            rightEdge <- middleIdx - 1

                    let boundaryX = rightEdge
                    let boundaryY = lid - leftEdge

                    // boundaryX and boundaryY can't be off the right edge of array (only off the left edge)
                    let isValidX = boundaryX >= 0
                    let isValidY = boundaryY >= 0

                    let mutable fstIdx = 0

                    if isValidX then
                        fstIdx <- localIndices.[boundaryX]

                    let mutable sndIdx = 0

                    if isValidY then
                        sndIdx <- localIndices.[firstLocalLength + boundaryY]

                    if not isValidX || isValidY && fstIdx <= sndIdx then
                        allIndicesBuffer.[gid] <- sndIdx
                        secondResultValues.[gid] <- secondValuesBuffer.[gid - lid - beginIdx + boundaryY]
                        isLeftBitMap.[gid] <- 0
                    else
                        allIndicesBuffer.[gid] <- fstIdx
                        firstResultValues.[gid] <- firstValuesBuffer.[beginIdx + boundaryX]
                        isLeftBitMap.[gid] <- 1 @>

        let kernel = clContext.Compile merge

        fun (processor: MailboxProcessor<_>) (firstVector: ClVector.Sparse<'a>) (secondVector: ClVector.Sparse<'b>) ->

            let firstSide = firstVector.Indices.Length

            let secondSide = secondVector.Indices.Length

            let sumOfSides = firstSide + secondSide

            let allIndices =
                clContext.CreateClArrayWithSpecificAllocationMode<int>(DeviceOnly, sumOfSides)

            let firstValues =
                clContext.CreateClArrayWithSpecificAllocationMode<'a>(DeviceOnly, sumOfSides)

            let secondValues =
                clContext.CreateClArrayWithSpecificAllocationMode<'b>(DeviceOnly, sumOfSides)

            let isLeftBitmap =
                clContext.CreateClArrayWithSpecificAllocationMode<int>(DeviceOnly, sumOfSides)

            let ndRange =
                Range1D.CreateValid(sumOfSides, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.KernelFunc
                            ndRange
                            firstSide
                            secondSide
                            sumOfSides
                            firstVector.Indices
                            firstVector.Values
                            secondVector.Indices
                            secondVector.Values
                            allIndices
                            firstValues
                            secondValues
                            isLeftBitmap)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            allIndices, firstValues, secondValues, isLeftBitmap
