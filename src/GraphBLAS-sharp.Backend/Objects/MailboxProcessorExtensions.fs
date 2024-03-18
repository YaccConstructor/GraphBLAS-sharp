namespace GraphBLAS.FSharp.Objects

open Brahma.FSharp

module MailboxProcessorExtensions =
    let finish (q: MailboxProcessor<Msg>) = q.PostAndReply(Msg.MsgNotifyMe)
