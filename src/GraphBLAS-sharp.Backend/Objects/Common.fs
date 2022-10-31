namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp

type IDeviceMemObject =
    abstract Dispose : MailboxProcessor<Msg> -> unit
