namespace GraphBLAS.FSharp.Backend.Quotes

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
