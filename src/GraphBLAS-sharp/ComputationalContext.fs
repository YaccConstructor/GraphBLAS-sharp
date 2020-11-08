namespace GraphBLAS.FSharp

open Brahma.OpenCL
open Brahma.FSharp.OpenCL.Core
open Brahma.FSharp.OpenCL.Extensions
open OpenCL.Net
open Brahma.OpenCL
open GraphBLAS.FSharp

type OpenCLContext = {
    Provider: ComputeProvider
    CommandQueue: CommandQueue
}

module OpenCLContext =
    let inline (!>) (x: ^a) : ^b = ((^a or ^b) : (static member op_Implicit : 'a -> ^b) x)

    let private provider =
        try ComputeProvider.Create("INTEL*", DeviceType.Cpu)
        with
        | ex -> failwith ex.Message
    let private que = new CommandQueue(provider, provider.Devices |> Seq.head)

    let currentContext = {
        Provider = provider
        CommandQueue = que
    }
