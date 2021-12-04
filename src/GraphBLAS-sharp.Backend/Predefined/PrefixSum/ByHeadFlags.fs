namespace GraphBLAS.FSharp.Backend.Predefined

open Brahma.FSharp.OpenCL
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Common

// TODO: переставить аргументы
module internal ByHeadFlags =
    let runInclude
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (headFlags: ClArray<int>)
        (inputArray: ClArray<'a>)
        plus
        zero =

        let zippedArrays = ClArray.zip clContext workGroupSize processor inputArray headFlags
        let total = clContext.CreateClArray<struct('a * int)>(1)
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
            0
            plusAdvanced
            struct(zero, 0)
        |> ignore

        processor.Post(Msg.CreateFreeMsg(total))

        ClArray.unzip clContext workGroupSize processor zippedArrays
        |> fst

