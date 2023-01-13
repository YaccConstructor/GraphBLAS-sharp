namespace GraphBLAS.FSharp.Backend.Quotes

open Brahma.FSharp

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
