namespace GraphBLAS.FSharp.Backend.Predefined

open Brahma.FSharp.OpenCL
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Common

module internal PrefixSum =
    let standardExcludeInplace
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (inputArray: ClArray<int>)
        (totalSum: ClCell<int>) =

        PrefixSum.runExcludeInplace
            clContext
            workGroupSize
            processor
            inputArray
            totalSum
            <@ (+) @>
            0

    let standardIncludeInplace
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (inputArray: ClArray<int>)
        (totalSum: ClCell<int>) =

        PrefixSum.runIncludeInplace
            clContext
            workGroupSize
            processor
            inputArray
            totalSum
            <@ (+) @>
            0

    let byHeadFlagsInclude
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (headFlags: ClArray<int>)
        (inputArray: ClArray<'a>)
        plus
        zero =

        let zippedArrays = ClArray.zip clContext workGroupSize processor inputArray headFlags
        let total = clContext.CreateClCell<struct('a * int)>()
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

        PrefixSum.runIncludeInplace
            clContext
            workGroupSize
            processor
            zippedArrays
            total
            plusAdvanced
            struct(zero, 0)
        |> ignore

        processor.Post(Msg.CreateFreeMsg(total))

        let res, heads = ClArray.unzip clContext workGroupSize processor zippedArrays
        processor.Post(Msg.CreateFreeMsg(zippedArrays))
        processor.Post(Msg.CreateFreeMsg(heads))

        res
