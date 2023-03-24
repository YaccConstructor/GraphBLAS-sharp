namespace GraphBLAS.FSharp.Backend.Quotes

open Brahma.FSharp

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

    let getUniqueBitmapLocal<'a when 'a: equality> =
        <@ fun (array: 'a []) length lid (result: int []) ->
            if lid < length then
                let isFirst = lid = 0

                let isNotEqualToPrev = array.[lid] <> array.[lid - 1]
                let isUnique = lid > 0 && isNotEqualToPrev

                if isFirst || isUnique then
                    result.[lid] <- 1
                else
                    result.[lid] <- 0 @>
