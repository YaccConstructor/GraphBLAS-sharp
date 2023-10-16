namespace GraphBLAS.FSharp.Benchmarks.Algorithms.PageRank

open System.IO
open BenchmarkDotNet.Attributes
open GraphBLAS.FSharp
open GraphBLAS.FSharp.IO
open Brahma.FSharp
open Microsoft.FSharp.Core
open GraphBLAS.FSharp.Objects.ArraysExtensions
open GraphBLAS.FSharp.Benchmarks
open GraphBLAS.FSharp.Objects

[<AbstractClass>]
[<IterationCount(10)>]
[<WarmupCount(3)>]
[<Config(typeof<Configs.Matrix>)>]
type Benchmarks(
    buildFunToBenchmark,
    converter: string -> float32,
    binaryConverter)
    =

    let mutable funToBenchmark = None
    let mutable matrix = Unchecked.defaultof<ClMatrix.CSR<float32>>
    let mutable matrixPrepared = Unchecked.defaultof<ClMatrix.CSR<float32>>
    let mutable matrixHost = Unchecked.defaultof<_>

    member val Result = Unchecked.defaultof<ClArray<float32 option>> with get,set

    [<ParamsSource("AvailableContexts")>]
    member val OclContextInfo = Unchecked.defaultof<Utils.BenchmarkContext * int> with get, set

    [<ParamsSource("InputMatrixProvider")>]
    member val InputMatrixReader = Unchecked.defaultof<MtxReader> with get, set

    member this.OclContext = (fst this.OclContextInfo).ClContext
    member this.WorkGroupSize = snd this.OclContextInfo

    member this.Processor =
        let p = (fst this.OclContextInfo).Queue
        p.Error.Add(fun e -> failwithf "%A" e)
        p

    static member AvailableContexts = Utils.availableContexts

    static member InputMatrixProviderBuilder pathToConfig =
        let datasetFolder = ""
        pathToConfig
        |> Utils.getMatricesFilenames
        |> Seq.map
            (fun matrixFilename ->
                printfn "%A" matrixFilename

                match Path.GetExtension matrixFilename with
                | ".mtx" -> MtxReader(Utils.getFullPathToMatrix datasetFolder matrixFilename)
                | _ -> failwith "Unsupported matrix format")

    member this.FunToBenchmark =
        match funToBenchmark with
        | None ->
            let x = buildFunToBenchmark this.OclContext this.WorkGroupSize
            funToBenchmark <- Some x
            x
        | Some x -> x

    member this.PageRank() =
        this.Result <- this.FunToBenchmark this.Processor matrixPrepared

    member this.ClearInputMatrix() =
        (matrix :> IDeviceMemObject).Dispose this.Processor

    member this.ClearPreparedMatrix() =
        (matrixPrepared :> IDeviceMemObject).Dispose this.Processor

    member this.ClearResult() = this.Result.FreeAndWait this.Processor

    member this.ReadMatrix() =
        let converter =
            match this.InputMatrixReader.Field with
            | Pattern -> binaryConverter
            | _ -> converter

        matrixHost <- this.InputMatrixReader.ReadMatrix converter

    member this.LoadMatrixToGPU() =
        matrix <- matrixHost.ToCSR.ToDevice this.OclContext

    member this.PrepareMatrix() =
        matrixPrepared <- Algorithms.PageRank.prepareMatrix this.OclContext this.WorkGroupSize this.Processor matrix

    abstract member GlobalSetup : unit -> unit

    abstract member IterationCleanup : unit -> unit

    abstract member GlobalCleanup : unit -> unit

    abstract member Benchmark : unit -> unit

type PageRankWithoutTransferBenchmarkFloat32() =

    inherit Benchmarks(
        Algorithms.PageRank.run,
        float32,
        (fun _ -> float32 <| Utils.nextInt (System.Random()))
    )

    static member InputMatrixProvider =
        Benchmarks.InputMatrixProviderBuilder "BFSBenchmarks.txt"

    [<GlobalSetup>]
    override this.GlobalSetup() =
        this.ReadMatrix()
        this.LoadMatrixToGPU()
        this.Processor.PostAndReply(Msg.MsgNotifyMe)
        this.PrepareMatrix()
        this.ClearInputMatrix()

    [<IterationCleanup>]
    override this.IterationCleanup() =
        this.ClearResult()

    [<GlobalCleanup>]
    override this.GlobalCleanup() =
        this.ClearPreparedMatrix()

    [<Benchmark>]
    override this.Benchmark() =
        this.PageRank()
        this.Processor.PostAndReply(Msg.MsgNotifyMe)
