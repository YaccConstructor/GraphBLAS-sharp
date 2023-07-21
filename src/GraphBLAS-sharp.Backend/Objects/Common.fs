namespace GraphBLAS.FSharp.Objects

open Brahma.FSharp

type IDeviceMemObject =
    abstract Dispose : MailboxProcessor<Msg> -> unit
