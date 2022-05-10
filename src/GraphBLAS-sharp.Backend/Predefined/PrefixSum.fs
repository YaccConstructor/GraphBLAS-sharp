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
                fun (x1: 'a)
                    (x2: int)
                    (y1: 'a)
                    (y2: int) ->

                    if y2 = 1 then
                        (y1, 1)
                    else
                        let buff = (%plus) x1 y1
                        (buff, x2)
            @>

        let copy0 = ClArray.copy clContext workGroupSize
        let copy1 = ClArray.copy clContext workGroupSize
        let scanIncludeInPlace = PrefixSum2.runIncludeInplace plusAdvanced clContext workGroupSize

        fun (processor: MailboxProcessor<_>)
            (headFlags: ClArray<int>)
            (inputArray: ClArray<'a>)
            zero ->

            let total0 = clContext.CreateClCell<'a>()
            let total1 = clContext.CreateClCell<int>()

            let inputArrayCopied = copy0 processor inputArray
            let headsCopied = copy1 processor headFlags

            scanIncludeInPlace
                processor
                inputArrayCopied
                headsCopied
                total0
                total1
                zero
                0
            |> ignore

            processor.Post(Msg.CreateFreeMsg(total0))
            processor.Post(Msg.CreateFreeMsg(total1))

            processor.Post(Msg.CreateFreeMsg(headsCopied))

            inputArrayCopied

    // let byHeadFlagsInclude
    //     plus
    //     (clContext: ClContext)
    //     workGroupSize =

    //     let plusAdvanced =
    //         <@
    //             fun ((x1, x2): struct('a * int))
    //                 ((y1, y2): struct('a * int)) ->

    //                 if y2 = 1 then
    //                     struct(y1, 1)
    //                 else
    //                     let buff = (%plus) x1 y1
    //                     struct(buff, x2)
    //         @>

    //     let zip = ClArray.zip clContext workGroupSize
    //     let unzip = ClArray.unzip clContext workGroupSize
    //     let scanIncludeInPlace = PrefixSum.runIncludeInplace plusAdvanced clContext workGroupSize

    //     fun (processor: MailboxProcessor<_>)
    //         (headFlags: ClArray<int>)
    //         (inputArray: ClArray<'a>)
    //         zero ->

    //         let zippedArrays = zip processor inputArray headFlags
    //         let total = clContext.CreateClCell<struct('a * int)>()

    //         scanIncludeInPlace
    //             processor
    //             zippedArrays
    //             total
    //             struct(zero, 0)
    //         |> ignore

    //         processor.Post(Msg.CreateFreeMsg(total))

    //         let res, heads = unzip processor zippedArrays
    //         processor.Post(Msg.CreateFreeMsg(zippedArrays))
    //         processor.Post(Msg.CreateFreeMsg(heads))

    //         res
