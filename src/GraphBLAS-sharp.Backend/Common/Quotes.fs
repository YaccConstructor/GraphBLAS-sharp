namespace GraphBLAS.FSharp.Backend.Common

open Brahma.FSharp

module SubSum =
    let private treeAccess<'a> opAdd =
        <@ fun step lid wgSize (localBuffer: 'a []) ->
            let i = step * (lid + 1) - 1

            let firstValue = localBuffer.[i - (step >>> 1)]
            let secondValue = localBuffer.[i]

            localBuffer.[i] <- (%opAdd) firstValue secondValue @>

    let private sequentialAccess<'a> opAdd =
        <@ fun step lid wgSize (localBuffer: 'a []) ->
            let firstValue = localBuffer.[lid]
            let secondValue = localBuffer.[lid + wgSize / step]

            localBuffer.[lid] <- (%opAdd) firstValue secondValue @>

    let private sumGeneral<'a> memoryAccess =
        <@ fun wgSize lid (localBuffer: 'a []) ->
            let mutable step = 2

            while step <= wgSize do
                if lid < wgSize / step then
                    (%memoryAccess) step lid wgSize localBuffer

                step <- step <<< 1

                barrierLocal () @>

    let sequentialSum<'a> opAdd =
        sumGeneral<'a> <| sequentialAccess<'a> opAdd

    let treeSum<'a> opAdd = sumGeneral<'a> <| treeAccess<'a> opAdd

module SubReduce =
    let run opAdd =
        <@ fun length wgSize gid lid (localValues: 'a []) ->
            let mutable step = 2

            while step <= wgSize do
                if (gid + wgSize / step) < length
                   && lid < wgSize / step then
                    let firstValue = localValues.[lid]
                    let secondValue = localValues.[lid + wgSize / step]

                    localValues.[lid] <- (%opAdd) firstValue secondValue

                step <- step <<< 1

                barrierLocal () @>

module PreparePositions =
    let both<'c> =
        <@ fun index (result: 'c option) (rawPositionsBuffer: ClArray<int>) (allValuesBuffer: ClArray<'c>) ->
            rawPositionsBuffer.[index] <- 0

            match result with
            | Some v ->
                allValuesBuffer.[index + 1] <- v
                rawPositionsBuffer.[index + 1] <- 1
            | None -> rawPositionsBuffer.[index + 1] <- 0 @>

    let leftRight<'c> =
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
