namespace GraphBLAS.FSharp.Backend.Quotes

open Brahma.FSharp

module SubSum =
    let private treeAccess<'a> opAdd =
        <@ fun step lid _ (localBuffer: 'a []) ->
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

    let sequentialSum<'a> = sumGeneral<'a> << sequentialAccess<'a>

    let upSweep<'a> = sumGeneral<'a> << treeAccess<'a>

    let downSweep opAdd =
        <@ fun wgSize lid (localBuffer: 'a []) ->
            let mutable step = wgSize

            while step > 1 do
                barrierLocal ()

                if lid < wgSize / step then
                    let i = step * (lid + 1) - 1
                    let j = i - (step >>> 1)

                    let tmp = localBuffer.[i]

                    let operand = localBuffer.[j] // brahma error
                    let buff = (%opAdd) tmp operand

                    localBuffer.[i] <- buff
                    localBuffer.[j] <- tmp

                step <- step >>> 1 @>

    let upSweepByKey opAdd =
        <@ fun wgSize lid (localBuffer: 'a []) (localKeys: 'b [])->
            let mutable step = 2

            while step <= wgSize do
                let i = step * (lid + 1) - 1

                let firstIndex = i - (step >>> 1) // TODO(work ?)
                let secondIndex = i

                let firstKey = localKeys.[firstIndex]
                let secondKey = localKeys.[secondIndex]

                if lid < wgSize / step
                   && firstKey = secondKey then

                    let firstValue = localBuffer.[firstIndex]
                    let secondValue = localBuffer.[secondIndex]

                    localBuffer.[secondIndex] <- (%opAdd) firstValue secondValue

                step <- step <<< 1

                barrierLocal () @>

    let downSweepByKey opAdd =
        <@ fun wgSize lid (localBuffer: 'a []) (localKeys: int []) ->
            let mutable step = wgSize

            while step > 1 do
                barrierLocal ()

                let rightIndex = step * (lid + 1) - 1
                let leftIndex = rightIndex - (step >>> 1)

                let rightKey = localKeys.[rightIndex]
                let leftKey = localKeys.[leftIndex]

                if lid < wgSize / step
                    && rightKey = leftKey then

                    let tmp = localBuffer.[rightIndex]

                    let rightOperand = localBuffer.[leftIndex] // Brahma error
                    let buff = (%opAdd) tmp rightOperand

                    localBuffer.[rightIndex] <- buff
                    localBuffer.[leftIndex] <- tmp

                step <- step >>> 1 @>

    let localPrefixSum opAdd =
        <@ fun (lid: int) (workGroupSize: int) (array: 'a []) ->
            let mutable offset = 1

            while offset < workGroupSize do
                barrierLocal ()
                let mutable value = array.[lid]

                if lid >= offset then
                    value <- (%opAdd) value array.[lid - offset]

                offset <- offset * 2

                barrierLocal ()
                array.[lid] <- value @>



    let localIntPrefixSum = localPrefixSum <@ (+) @>
