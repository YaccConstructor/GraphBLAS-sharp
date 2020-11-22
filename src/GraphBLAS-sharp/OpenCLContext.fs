namespace GraphBLAS.FSharp

open Brahma.OpenCL
open OpenCL.Net
open Brahma.OpenCL

type OpenCLContext = {
    Provider: ComputeProvider
    CommandQueue: CommandQueue
}

module OpenCLContext =
    let private defaultProvider =
        try ComputeProvider.Create("*", DeviceType.Default)
        with
        | ex -> failwith ex.Message

    let private defaultQueue =
        new CommandQueue(defaultProvider, defaultProvider.Devices |> Seq.head)

    let currentContext = {
        Provider = defaultProvider
        CommandQueue = defaultQueue
    }
