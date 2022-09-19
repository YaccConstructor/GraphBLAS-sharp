namespace GraphBLAS.FSharp.Benchmarks

open BenchmarkDotNet.Attributes
open FsCheck
open Brahma.FSharp
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Common

// TODO ?? CSR ??
[<AbstractClass>]
[<WarmupCount(5)>]
[<IterationCount(10)>]
type EWiseAddBatchedBenchmarks<'a when 'a : struct and 'a : equality>(isEqual, zero, op) =
    member val EWiseAddBatched = Unchecked.defaultof<MailboxProcessor<Msg> -> ClCooMatrix<'a>[] -> ClCooMatrix<'a>> with get, set

    member val EWiseAdd = Unchecked.defaultof<MailboxProcessor<Msg> -> ClCooMatrix<'a> -> ClCooMatrix<'a> -> ClCooMatrix<'a>> with get, set

    member val HostMatrices = Unchecked.defaultof<COOMatrix<'a>[]> with get, set

    member val DeviceMatrices = Unchecked.defaultof<ClCooMatrix<'a>[]> with get, set

    member val ResultMatrix = Unchecked.defaultof<ClCooMatrix<'a>> with get, set

    member val Queue = Unchecked.defaultof<MailboxProcessor<Msg>> with get, set

    [<ParamsSource("MatricesCountProvider")>]
    member val MatricesCount = 0 with get, set

    [<ParamsSource("MatricesSizeProvider")>]
    member val MatricesSize = (0 , 0) with get, set

    [<ParamsSource("AvaliableContextsProvider")>]
    member val Context = Unchecked.defaultof<RuntimeContext> with get, set

    [<ParamsSource("WgSizeProvider")>]
    member val WgSize = 0 with get, set

    [<GlobalSetup>]
    member this.CompileProgramAndTransferData() =
        this.EWiseAddBatched <- EWiseAddBatched.eWiseAddBatched this.Context.ClContext op this.WgSize
        this.EWiseAdd <- COOMatrix.eWiseAdd this.Context.ClContext op this.WgSize

        let matrixGenerator = gen {
            let! indices =
                Gen.choose (0, fst >> (fun x -> float x ** 2.) >> (fun x -> int x - 1) <| this.MatricesSize)
                |> Gen.arrayOfLength (snd this.MatricesSize)
                |> Gen.map (fun indices -> indices |> Array.sort |> Array.distinct)

            let rows, cols =
                indices
                |> Array.map (fun x -> x / fst this.MatricesSize, x % fst this.MatricesSize)
                |> Array.unzip

            let! values =
                Arb.generate<'a>
                |> Gen.filter (fun x -> not <| isEqual x zero)
                |> Gen.arrayOfLength indices.Length

            return {
                RowCount = fst this.MatricesSize
                ColumnCount = fst this.MatricesSize
                Rows = rows
                Columns = cols
                Values = values
            }
        }

        this.HostMatrices <-
            matrixGenerator
            |> Gen.sample 1000 this.MatricesCount
            |> Array.ofList

        this.DeviceMatrices <-
            this.HostMatrices
            |> Array.map (fun m -> m.ToDevice this.Context.ClContext)

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

    [<GlobalCleanup>]
    member this.Cleanup() =
        this.DeviceMatrices |> Array.iter (fun m -> m.Dispose this.Queue)
        this.ResultMatrix.Dispose this.Queue

    static member MatricesCountProvider =
        seq {
            10
//            20
//            30
        }

    // row (col) count, max nnz
    static member MatricesSizeProvider =
        seq {
            1000, 10_000
//            1_000_000, 10_000_000
//            10_000, 1_000_000
        }

    static member AvaliableContextsProvider =
        ClDevice.GetAvailableDevices(Platform.Nvidia)
        |> Seq.map RuntimeContext

    static member WgSizeProvider =
        seq {
//            64
            256
//            1024
        }

module Concrete =
    type EWiseAddBatchedIntBenchmarks() = inherit EWiseAddBatchedBenchmarks<int>((=), 0, StandardOperations.intSum)
