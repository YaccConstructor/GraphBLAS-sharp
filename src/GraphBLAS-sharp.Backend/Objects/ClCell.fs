namespace GraphBLAS.FSharp.Backend.Objects

open Brahma.FSharp

module ClCell =
    type ClCell<'a> with
        member this.ToHost(processor: MailboxProcessor<_>) =
            processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(this, (Array.zeroCreate<'a> 1), ch)).[0]

        member this.Free(processor: MailboxProcessor<_>) = processor.Post(Msg.CreateFreeMsg<_>(this))

        member this.ToHostAndFree(processor: MailboxProcessor<_>) =
            let result =  this.ToHost processor
            this.Free processor

            result
