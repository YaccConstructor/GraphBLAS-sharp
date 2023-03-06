namespace GraphBLAS.FSharp.Backend.Objects

open Brahma.FSharp

module ClCell =
    type ClCell<'a> with
        member this.ToHostAndFree(processor: MailboxProcessor<_>) =
            let res =
                processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(this, (Array.zeroCreate<'a> 1), ch))

            processor.Post(Msg.CreateFreeMsg<_>(this))

            res.[0]
