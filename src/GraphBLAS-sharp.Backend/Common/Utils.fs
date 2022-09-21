namespace GraphBLAS.FSharp.Backend.Common

open Brahma.FSharp

module internal Utils =
    let defaultWorkGroupSize = 32

    let floorToPower2 =
        fun x -> x ||| (x >>> 1)
        >> fun x -> x ||| (x >>> 2)
        >> fun x -> x ||| (x >>> 4)
        >> fun x -> x ||| (x >>> 8)
        >> fun x -> x ||| (x >>> 16)
        >> fun x -> x - (x >>> 1)

    let ceilToPower2 =
        fun x -> x - 1
        >> fun x -> x ||| (x >>> 1)
        >> fun x -> x ||| (x >>> 2)
        >> fun x -> x ||| (x >>> 4)
        >> fun x -> x ||| (x >>> 8)
        >> fun x -> x ||| (x >>> 16)
        >> fun x -> x + 1

    let toHost (processor: MailboxProcessor<_>) (src: ClArray<_>) =
        let dst = Array.zeroCreate src.Length
        processor.PostAndReply(fun ch -> Msg.CreateToHostMsg(src, dst, ch))
