namespace GraphBLAS.FSharp.Backend.Common

open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Quotes

module internal Scatter =
    let private general<'a> predicate (clContext: ClContext) workGroupSize =

        let run =
            <@ fun (ndRange: Range1D) (positions: ClArray<int>) (positionsLength: int) (values: ClArray<'a>) (result: ClArray<'a>) (resultLength: int) ->

                let gid = ndRange.GlobalID0

                if gid < positionsLength then
                    // positions lengths == values length
                    let predicateResult =
                        (%predicate) gid positionsLength positions

                    let position = positions.[gid]

                    if predicateResult
                       && 0 <= position
                       && position < resultLength then

                        result.[positions.[gid]] <- values.[gid] @>

        let program = clContext.Compile(run)

        fun (processor: MailboxProcessor<_>) (positions: ClArray<int>) (values: ClArray<'a>) (result: ClArray<'a>) ->

            if positions.Length <> values.Length then
                failwith "Lengths must be the same"

            let positionsLength = positions.Length

            let ndRange =
                Range1D.CreateValid(positionsLength, workGroupSize)

            let kernel = program.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () -> kernel.KernelFunc ndRange positions positionsLength values result result.Length)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

    let firstOccurrence clContext =
        general
        <| Predicates.firstOccurrence ()
        <| clContext

    let lastOccurrence clContext =
        general
        <| Predicates.lastOccurrence ()
        <| clContext

    let private generalInit<'a> predicate valueMap (clContext: ClContext) workGroupSize =

        let run =
            <@ fun (ndRange: Range1D) (positions: ClArray<int>) (positionsLength: int) (result: ClArray<'a>) (resultLength: int) ->

                let gid = ndRange.GlobalID0

                if gid < positionsLength then
                    // positions lengths == values length
                    let predicateResult =
                        (%predicate) gid positionsLength positions

                    let position = positions.[gid]

                    if predicateResult
                       && 0 <= position
                       && position < resultLength then

                        result.[positions.[gid]] <- (%valueMap) gid @>

        let program = clContext.Compile(run)

        fun (processor: MailboxProcessor<_>) (positions: ClArray<int>) (result: ClArray<'a>) ->

            let positionsLength = positions.Length

            let ndRange =
                Range1D.CreateValid(positionsLength, workGroupSize)

            let kernel = program.GetKernel()

            processor.Post(
                Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange positions positionsLength result result.Length)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

    let initFirsOccurrence<'a> valueMap =
        generalInit<'a>
        <| Predicates.firstOccurrence ()
        <| valueMap

    let initLastOccurrence<'a> valueMap =
        generalInit<'a>
        <| Predicates.lastOccurrence ()
        <| valueMap
