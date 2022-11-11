namespace GraphBLAS.FSharp.Backend.SparseVector

open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Common
open Microsoft.FSharp.Quotations

module ElementwiseConstructor =
        let merge workGroupSize =
            <@ fun (ndRange: Range1D) (firstSide: int) (secondSide: int) (sumOfSides: int) (firstIndicesBuffer: ClArray<int>) (firstValuesBuffer: ClArray<'a>) (secondIndicesBuffer: ClArray<int>) (secondValuesBuffer: ClArray<'b>) (allIndicesBuffer: ClArray<int>) (firstResultValues: ClArray<'a>) (secondResultValues: ClArray<'b>) (isLeftBitMap: ClArray<int>) ->

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

                    if rightEdge > localID then
                        rightEdge <- localID

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

        let private opWriteBothFill (opAdd: Expr<'a option -> 'b option -> 'a -> 'a option>) =
            <@
                fun gid (leftValues: ClArray<'a>) (rightValues: ClArray<'b>) (value: 'a) ->
                     (%opAdd) (Some leftValues.[gid]) (Some rightValues.[gid + 1]) value
            @>

        let private opWriteLeftFill (opAdd: Expr<'a option -> 'b option -> 'a -> 'a option>) =
            <@
                fun gid (leftValues: ClArray<'a>) (value: 'a) ->
                     (%opAdd) (Some leftValues.[gid]) None value
            @>

        let private opWriteRightFill (opAdd: Expr<'a option -> 'b option -> 'a -> 'a option>) =
            <@
                fun gid (rightValues: ClArray<'b>) (value: 'a) ->
                     (%opAdd) None (Some rightValues.[gid + 1]) value
            @>

        let private opWriteAtLeastOneBothFill (opAdd: Expr<AtLeastOne<'a,'b> -> 'a -> 'a option>) =
            <@
                fun gid (leftValues: ClArray<'a>) (rightValues: ClArray<'b>) (value: 'a) ->
                     (%opAdd) (Both(leftValues.[gid], rightValues.[gid + 1])) value
            @>

        let private opWriteAtLeastOneLeftFill (opAdd: Expr<AtLeastOne<'a,'b> -> 'a -> 'a option>) =
            <@
                fun gid (leftValues: ClArray<'a>) (value: 'a) ->
                      (%opAdd) (Left(leftValues.[gid])) value
            @>

        let private opWriteAtLeastOneRightFill (opAdd: Expr<AtLeastOne<'a,'b> -> 'a -> 'a option>) =
            <@
                fun gid (rightValues: ClArray<'b>) (value: 'a) ->
                     (%opAdd) (Right(rightValues.[gid])) value
            @>

        let private opWriteBoth (opAdd: Expr<'a option -> 'b option -> 'c option>) =
            <@
                fun gid (leftValues: ClArray<'a>) (rightValues: ClArray<'b>) ->
                     (%opAdd) (Some leftValues.[gid]) (Some rightValues.[gid + 1])
            @>

        let private opWriteLeft (opAdd: Expr<'a option -> 'b option -> 'c option>) =
            <@
                fun gid (leftValues: ClArray<'a>)->
                     (%opAdd) (Some leftValues.[gid]) None
            @>

        let private opWriteRight (opAdd: Expr<'a option -> 'b option -> 'c option>) =
            <@
                fun gid (rightValues: ClArray<'b>) ->
                     (%opAdd) None (Some rightValues.[gid + 1])
            @>

        let private opWriteAtLeastOneBoth (opAdd: Expr<AtLeastOne<'a,'b> -> 'c option>) =
            <@
                fun gid (leftValues: ClArray<'a>) (rightValues: ClArray<'b>) ->
                     (%opAdd) (Both(leftValues.[gid], rightValues.[gid + 1]))
            @>

        let opWriteAtLeastOneLeft (opAdd: Expr<AtLeastOne<'a,'b> -> 'c option>) =
            <@
                fun gid (leftValues: ClArray<'a>) ->
                      (%opAdd) (Left(leftValues.[gid]))
            @>

        let opWriteAtLeastOneRight (opAdd: Expr<AtLeastOne<'a,'b> -> 'a option>) =
            <@
                fun gid (rightValues: ClArray<'b>) ->
                     (%opAdd) (Right(rightValues.[gid]))
            @>

        let private both<'c> =
            <@ fun index (result: 'c option) (rawPositionsBuffer: ClArray<int>) (allValuesBuffer: ClArray<'c>) ->
                rawPositionsBuffer.[index] <- 0

                match result with
                | Some v ->
                    allValuesBuffer.[index + 1] <- v
                    rawPositionsBuffer.[index + 1] <- 1
                | None -> rawPositionsBuffer.[index + 1] <- 0 @>

        let private leftRight<'c> =
            <@ fun index (leftResult: 'c option) (rightResult: 'c option) (isLeftBitmap: ClArray<int>) (allValuesBuffer: ClArray<'c>) (rawPositionsBuffer: ClArray<int>) ->
                if isLeftBitmap.[index] = 1 then
                    match leftResult with
                    | Some v ->
                        allValuesBuffer.[index] <- v
                        rawPositionsBuffer.[index] <- 1
                    | None -> rawPositionsBuffer.[index] <- 0
                else
                    match rightResult with
                    | Some v ->
                        allValuesBuffer.[index] <- v
                        rawPositionsBuffer.[index] <- 1
                    | None -> rawPositionsBuffer.[index] <- 0 @>

        let private preparePositionsGeneral
            (bothWrite: Expr<(int -> ClArray<'a> -> ClArray<'b> -> 'c option)>)
            leftWrite
            rightWrite
            =

            <@ fun (ndRange: Range1D) length (allIndices: ClArray<int>) (leftValues: ClArray<'a>) (rightValues: ClArray<'b>) (isLeft: ClArray<int>) (allValues: ClArray<'c>) (positions: ClArray<int>) ->

                let gid = ndRange.GlobalID0

                if gid < length - 1
                   && allIndices.[gid] = allIndices.[gid + 1] then
                    let (result: 'c option) = (%bothWrite) gid leftValues rightValues

                    (%both) gid result positions allValues
                elif (gid < length
                      && gid > 0
                      && allIndices.[gid - 1] <> allIndices.[gid])
                     || gid = 0 then

                    let leftResult = (%leftWrite) gid leftValues
                    let rightResult = (%rightWrite) gid rightValues

                    (%leftRight) gid leftResult rightResult isLeft allValues positions @>

        let private prepareFillVectorGeneral bothWrite leftWrite rightWrite =
            <@ fun (ndRange: Range1D) length (allIndices: ClArray<int>) (leftValues: ClArray<'a>) (rightValues: ClArray<'b>) (value: ClCell<'a>) (isLeft: ClArray<int>) (allValues: ClArray<'a>) (positions: ClArray<int>) ->

                let gid = ndRange.GlobalID0

                let value = value.Value

                if gid < length - 1
                   && allIndices.[gid] = allIndices.[gid + 1] then
                    let (result: 'a option) = (%bothWrite) gid leftValues rightValues value

                    (%both) gid result positions allValues
                elif (gid < length
                      && gid > 0
                      && allIndices.[gid - 1] <> allIndices.[gid])
                      || gid = 0 then
                    let leftResult = (%leftWrite) gid leftValues value
                    let rightResult = (%rightWrite) gid rightValues value

                    (%leftRight) gid leftResult rightResult isLeft allValues positions @>

        let preparePositions opAdd = preparePositionsGeneral (opWriteBoth opAdd) (opWriteLeft opAdd) (opWriteRight opAdd)

        let preparePositionsAtLeastOne opAdd = preparePositionsGeneral (opWriteAtLeastOneBoth opAdd) (opWriteAtLeastOneLeft opAdd) (opWriteAtLeastOneRight opAdd)

        let prepareFillVector opAdd = prepareFillVectorGeneral (opWriteBothFill opAdd) (opWriteLeftFill opAdd) (opWriteRightFill opAdd)

        let prepareFillVectorAtLeastOne opAdd = prepareFillVectorGeneral (opWriteAtLeastOneBothFill opAdd) (opWriteAtLeastOneLeftFill opAdd) (opWriteAtLeastOneRightFill opAdd)
