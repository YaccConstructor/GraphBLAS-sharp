namespace GraphBLAS.FSharp.Benchmarks

open BenchmarkDotNet.Attributes
open FsCheck
open Brahma.FSharp
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Common

// TODO ?? CSR ??
[<AbstractClass>]
type EWiseAddBatchedBenchmarks<'a when 'a : struct and 'a : equality>(isEqual, zero, op) =
    member val EWiseAddBatched = Unchecked.defaultof<MailboxProcessor<Msg> -> ClCooMatrix<'a>[] -> ClCooMatrix<'a>> with get, set

    member val EWiseAdd = Unchecked.defaultof<MailboxProcessor<Msg> -> ClCooMatrix<'a> -> ClCooMatrix<'a> -> ClCooMatrix<'a>> with get, set

    member val HostMatrices = Unchecked.defaultof<'a[,][]> with get, set

    member val DeviceMatrices = Unchecked.defaultof<ClCooMatrix<'a>[]> with get, set

    member val ResultMatrix = Unchecked.defaultof<ClCooMatrix<'a>> with get, set

    member val Queue = Unchecked.defaultof<MailboxProcessor<Msg>> with get, set

    [<ParamsSource("MatricesCountProvider")>]
    member val MatricesCount = 0 with get, set

    [<ParamsSource("MatricesSizeProvider")>]
    member val MatricesSize = 0 with get, set

    [<ParamsSource("AvaliableContextsProvider")>]
    member val Context = Unchecked.defaultof<RuntimeContext> with get, set

    [<ParamsSource("WgSizeProvider")>]
    member val WgSize = 0 with get, set

    [<GlobalSetup>]
    member this.CompileProgramAndTransferData() =
        this.EWiseAddBatched <- EWiseAddBatched.eWiseAddBatched this.Context.ClContext op this.WgSize
        this.EWiseAdd <- COOMatrix.eWiseAdd this.Context.ClContext op this.WgSize

        this.HostMatrices <-
            Gen.array2DOf Arb.generate<'a>
            |> Gen.sample this.MatricesSize this.MatricesCount
            |> Array.ofList

        this.DeviceMatrices <-
            Array.init this.MatricesCount <| fun i ->
                Utils.createMatrixFromArray2D COO this.HostMatrices.[i] (isEqual zero)
            |> Array.map (fun m -> m.ToDevice this.Context.ClContext)
            |> Array.map (fun (ClMatrixCOO m) -> m)

    [<IterationSetup>]
    member this.GetQueue() =
        this.Queue <- this.Context.ClContext.QueueProvider.CreateQueue()

    [<Benchmark>]
    member this.EWABatched() =
        this.ResultMatrix <- this.EWiseAddBatched this.Queue this.DeviceMatrices
        this.Queue.PostAndReply(Msg.MsgNotifyMe)

    [<Benchmark>]
    member this.EWASequenced() =
        this.ResultMatrix <- this.DeviceMatrices.[0]
        for i in 1 .. this.DeviceMatrices.Length - 1 do
            this.ResultMatrix <- this.EWiseAdd this.Queue this.ResultMatrix this.DeviceMatrices.[i]
        this.Queue.PostAndReply(Msg.MsgNotifyMe)

    [<GlobalSetup>]
    member this.Cleanup() =
        this.DeviceMatrices |> Array.iter (fun m -> m.Dispose this.Queue)
        this.ResultMatrix.Dispose this.Queue

    static member MatricesCountProvider =
        seq {
            10
            20
        }

    static member MatricesSizeProvider =
        seq {
            50
            100
        }

    static member AvaliableContextsProvider =
        ClDevice.GetAvailableDevices(Platform.Nvidia)
        |> Seq.map RuntimeContext

    static member WgSizeProvider =
        seq {
            256
        }

module Concrete =
    type EWiseAddBatchedIntBenchmarks() = inherit EWiseAddBatchedBenchmarks<int>((=), 0, StandardOperations.intSum)
