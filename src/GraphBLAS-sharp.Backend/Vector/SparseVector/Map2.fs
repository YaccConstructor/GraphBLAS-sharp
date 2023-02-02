namespace GraphBLAS.FSharp.Backend.Vector.Sparse

open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Quotes
open FSharp.Quotations

module Map2 =
    let binSearch<'a> =
        <@ fun lenght sourceIndex (indices: ClArray<int>) (values: ClArray<'a>) ->

            let mutable leftEdge = 0
            let mutable rightEdge = lenght - 1

            let mutable result = None

            while leftEdge <= rightEdge do
                let middleIdx = (leftEdge + rightEdge) / 2
                let currentIndex = indices.[middleIdx]

                if sourceIndex = currentIndex then
                    result <- Some values.[middleIdx]

                    rightEdge <- -1 // TODO() break
                elif sourceIndex < currentIndex then
                    rightEdge <- middleIdx - 1
                else
                    leftEdge <- middleIdx + 1

            result @>

    let preparePositionsGeneral (op: Expr<'a option -> 'b option -> 'c option>) =
        <@ fun (ndRange: Range1D) length leftValuesLength rightValuesLength (leftValues: ClArray<'a>) (leftIndices: ClArray<int>) (rightValues: ClArray<'b>) (rightIndices: ClArray<int>) (resultBitmap: ClArray<int>) (resultValues: ClArray<'c>) (resultIndices: ClArray<int>) ->

            let gid = ndRange.GlobalID0

            if gid < length then

                let (leftValue: 'a option) =
                    (%binSearch) leftValuesLength gid leftIndices leftValues

                let (rightValue: 'b option) =
                    (%binSearch) rightValuesLength gid rightIndices rightValues

                match (%op) leftValue rightValue with
                | Some value ->
                    resultValues.[gid] <- value
                    resultIndices.[gid] <- gid

                    resultBitmap.[gid] <- 1
                | None -> resultBitmap.[gid] <- 0 @>

    let prepareAssign op =
        <@ fun (ndRange: Range1D) length leftValuesLength rightValuesLength (leftValues: ClArray<'a>) (leftIndices: ClArray<int>) (rightValues: ClArray<'b>) (rightIndices: ClArray<int>) (value: ClCell<'a>) (resultBitmap: ClArray<int>) (resultValues: ClArray<'c>) (resultIndices: ClArray<int>) ->

            let gid = ndRange.GlobalID0

            let value = value.Value

            if gid < length then

                let (leftValue: 'a option) =
                    (%binSearch) leftValuesLength gid leftIndices leftValues

                let (rightValue: 'b option) =
                    (%binSearch) rightValuesLength gid rightIndices rightValues

                match (%op) leftValue rightValue value with
                | Some value ->
                    resultValues.[gid] <- value
                    resultIndices.[gid] <- gid

                    resultBitmap.[gid] <- 1
                | None -> resultBitmap.[gid] <- 0 @>

    let merge workGroupSize =
        <@ fun (ndRange: Range1D) (firstSide: int) (secondSide: int) (sumOfSides: int) (firstIndicesBuffer: ClArray<int>) (firstValuesBuffer: ClArray<'a>) (secondIndicesBuffer: ClArray<int>) (secondValuesBuffer: ClArray<'b>) (allIndicesBuffer: ClArray<int>) (firstResultValues: ClArray<'a>) (secondResultValues: ClArray<'b>) (isLeftBitMap: ClArray<int>) ->

            let i = ndRange.GlobalID0

            let mutable beginIdxLocal = local ()
            let mutable endIdxLocal = local ()
            let localID = ndRange.LocalID0

            if localID < 2 then
                let x = localID * (workGroupSize - 1) + i - 1

                let diagonalNumber = min (sumOfSides - 1) x

                let mutable leftEdge = diagonalNumber + 1 - secondSide
                leftEdge <- max 0 leftEdge

                let mutable rightEdge = firstSide - 1

                rightEdge <- min rightEdge diagonalNumber

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
                localIndices.[localID] <- firstIndicesBuffer.[beginIdx + localID]

            if localID < secondLocalLength then
                localIndices.[firstLocalLength + localID] <- secondIndicesBuffer.[i - beginIdx]

            barrierLocal ()

            if i < sumOfSides then
                let mutable leftEdge = localID + 1 - secondLocalLength
                if leftEdge < 0 then leftEdge <- 0

                let mutable rightEdge = firstLocalLength - 1

                rightEdge <- min rightEdge localID

                while leftEdge <= rightEdge do
                    let middleIdx = (leftEdge + rightEdge) / 2
                    let firstIndex = localIndices.[middleIdx]

                    let secondIndex =
                        localIndices.[firstLocalLength + localID - middleIdx]

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
                    fstIdx <- localIndices.[boundaryX]

                let mutable sndIdx = 0

                if isValidY then
                    sndIdx <- localIndices.[firstLocalLength + boundaryY]

                if not isValidX || isValidY && fstIdx <= sndIdx then
                    allIndicesBuffer.[i] <- sndIdx
                    secondResultValues.[i] <- secondValuesBuffer.[i - localID - beginIdx + boundaryY]
                    isLeftBitMap.[i] <- 0
                else
                    allIndicesBuffer.[i] <- fstIdx
                    firstResultValues.[i] <- firstValuesBuffer.[beginIdx + boundaryX]
                    isLeftBitMap.[i] <- 1 @>

    let preparePositions opAdd =
        <@ fun (ndRange: Range1D) length (allIndices: ClArray<int>) (leftValues: ClArray<'a>) (rightValues: ClArray<'b>) (isLeft: ClArray<int>) (allValues: ClArray<'c>) (positions: ClArray<int>) ->

            let gid = ndRange.GlobalID0

            if gid < length - 1
               && allIndices.[gid] = allIndices.[gid + 1] then
                let result =
                    (%opAdd) (Some leftValues.[gid]) (Some rightValues.[gid + 1])

                (%PreparePositions.both) gid result positions allValues
            elif (gid < length
                  && gid > 0
                  && allIndices.[gid - 1] <> allIndices.[gid])
                 || gid = 0 then
                let leftResult = (%opAdd) (Some leftValues.[gid]) None
                let rightResult = (%opAdd) None (Some rightValues.[gid])

                (%PreparePositions.leftRight) gid leftResult rightResult isLeft allValues positions @>
