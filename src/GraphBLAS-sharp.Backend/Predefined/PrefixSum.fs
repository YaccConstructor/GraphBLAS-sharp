namespace GraphBLAS.FSharp.Backend.Predefined

open Brahma.FSharp.OpenCL
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Common

module internal PrefixSum =
    let standardExcludeInplace
        (clContext: ClContext)
        workGroupSize =

        let scan = PrefixSum.runExcludeInplace <@ (+) @> clContext workGroupSize

        fun (processor: MailboxProcessor<_>)
            (inputArray: ClArray<int>)
            (totalSum: ClCell<int>) ->

            scan
                processor
                inputArray
                totalSum
                0

    let standardIncludeInplace
        (clContext: ClContext)
        workGroupSize =

        let scan = PrefixSum.runIncludeInplace <@ (+) @> clContext workGroupSize

        fun (processor: MailboxProcessor<_>)
            (inputArray: ClArray<int>)
            (totalSum: ClCell<int>) ->

            scan
                processor
                inputArray
                totalSum
                0

    let standardInclude
        (clContext: ClContext)
        workGroupSize =

        let scan = PrefixSum.runInclude <@ (+) @> clContext workGroupSize

        fun (processor: MailboxProcessor<_>)
            (inputArray: ClArray<int>)
            (totalSum: ClCell<int>) ->

            scan
                processor
                inputArray
                totalSum
                0

    let standardExclude
        (clContext: ClContext)
        workGroupSize =

        let scan = PrefixSum.runExclude <@ (+) @> clContext workGroupSize

        fun (processor: MailboxProcessor<_>)
            (inputArray: ClArray<int>)
            (totalSum: ClCell<int>) ->

            scan
                processor
                inputArray
                totalSum
                0

    let byHeadFlagsInclude
        plus
        (clContext: ClContext)
        workGroupSize =

        let plusAdvanced =
            <@
                fun ((x1, x2): struct('a * int))
                    ((y1, y2): struct('a * int)) ->

                    if y2 = 1 then
                        struct(y1, 1)
                    else
                        let buff = (%plus) x1 y1
                        struct(buff, x2)
            @>

        let zip = ClArray.zip clContext workGroupSize
        let unzip = ClArray.unzip clContext workGroupSize
        let scanIncludeInPlace = PrefixSum.runIncludeInplace plusAdvanced clContext workGroupSize

        fun (processor: MailboxProcessor<_>)
            (headFlags: ClArray<int>)
            (inputArray: ClArray<'a>)
            zero ->

            let zippedArrays = zip processor inputArray headFlags
            let total = clContext.CreateClCell<struct('a * int)>()

            scanIncludeInPlace
                processor
                zippedArrays
                total
                struct(zero, 0)
            |> ignore

            processor.Post(Msg.CreateFreeMsg(total))

            let res, heads = unzip processor zippedArrays
            processor.Post(Msg.CreateFreeMsg(zippedArrays))
            processor.Post(Msg.CreateFreeMsg(heads))

            res

    let byTailFlagsInclude
        plus
        zero
        (clContext: ClContext)
        workGroupSize =

        let plusAdvanced =
            <@
                fun ((x1, x2): struct('a * int))
                    ((y1, y2): struct('a * int)) ->

                    if x2 = 1 then
                        struct(y1, y2)
                    else
                        let buff = (%plus) x1 y1
                        struct(buff, y2)
            @>

        let zip = ClArray.zip clContext workGroupSize
        let unzip = ClArray.unzip clContext workGroupSize
        let scanIncludeInPlace = PrefixSum.runIncludeInplace plusAdvanced clContext workGroupSize

        fun (processor: MailboxProcessor<_>)
            (tailFlags: ClArray<int>)
            (inputArray: ClArray<'a>) ->

            let zippedArrays = zip processor inputArray tailFlags
            let total = clContext.CreateClCell<struct('a * int)>()

            scanIncludeInPlace
                processor
                zippedArrays
                total
                struct(zero, 0)
            |> ignore

            processor.Post(Msg.CreateFreeMsg(total))

            let res, tails = unzip processor zippedArrays
            processor.Post(Msg.CreateFreeMsg(zippedArrays))
            processor.Post(Msg.CreateFreeMsg(tails))

            res
