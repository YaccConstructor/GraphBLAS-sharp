namespace GraphBLAS.FSharp.Backend.Objects

open Brahma.FSharp

type IDeviceMemObject =
    abstract Dispose : MailboxProcessor<Msg> -> unit
