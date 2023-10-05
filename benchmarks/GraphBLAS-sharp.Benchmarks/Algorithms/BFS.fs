namespace GraphBLAS.FSharp.Benchmarks.Algorithms.BFS

open System.IO
open BenchmarkDotNet.Attributes
open Microsoft.FSharp.Core
open Brahma.FSharp
open GraphBLAS.FSharp
open GraphBLAS.FSharp.IO
open GraphBLAS.FSharp.Benchmarks
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Objects.ArraysExtensions
open GraphBLAS.FSharp.Backend.Quotes

[<AbstractClass>]
[<IterationCount(100)>]
[<WarmupCount(10)>]
[<Config(typeof<Configs.Matrix>)>]
type Benchmarks<'elem when 'elem : struct>(
    buildFunToBenchmark,
    converter: string -> 'elem,
    binaryConverter,
    vertex: int,
    buildMatrix)
    =

    let mutable funToBenchmark = None
    let mutable matrix = Unchecked.defaultof<ClMatrix<'elem>>
    let mutable matrixHost = Unchecked.defaultof<_>

    member val ResultLevels = Unchecked.defaultof<ClArray<'elem option>> with get,set

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

    member this.BFS() =
        this.ResultLevels <- this.FunToBenchmark this.Processor matrix vertex

    member this.ClearInputMatrix() =
        matrix.Dispose this.Processor

    member this.ClearResult() = this.ResultLevels.FreeAndWait this.Processor

    member this.ReadMatrix() =
        let converter =
            match this.InputMatrixReader.Field with
            | Pattern -> binaryConverter
            | _ -> converter

        matrixHost <- this.InputMatrixReader.ReadMatrix converter

    member this.LoadMatrixToGPU() =
        matrix <- buildMatrix this.OclContext matrixHost

    abstract member GlobalSetup : unit -> unit

    abstract member IterationCleanup : unit -> unit

    abstract member GlobalCleanup : unit -> unit

    abstract member Benchmark : unit -> unit

type WithoutTransferBenchmark<'elem when 'elem : struct>(
    buildFunToBenchmark,
    converter: string -> 'elem,
    boolConverter,
    vertex,
    buildMatrix) =

    inherit Benchmarks<'elem>(
        buildFunToBenchmark,
        converter,
        boolConverter,
        vertex,
        buildMatrix)

    [<GlobalSetup>]
    override this.GlobalSetup() =
        this.ReadMatrix()
        this.LoadMatrixToGPU()

    [<IterationCleanup>]
    override this.IterationCleanup() =
        this.ClearResult()

    [<GlobalCleanup>]
    override this.GlobalCleanup() =
        this.ClearInputMatrix()

    [<Benchmark>]
    override this.Benchmark() =
        this.BFS()
        this.Processor.PostAndReply Msg.MsgNotifyMe

type BFSWithoutTransferBenchmarkInt32() =

    inherit WithoutTransferBenchmark<int>(
        (Algorithms.BFS.singleSource ArithmeticOperations.intSumOption ArithmeticOperations.intMulOption),
        int32,
        (fun _ -> Utils.nextInt (System.Random())),
        0,
        (fun context matrix -> ClMatrix.CSR <| matrix.ToCSR.ToDevice context))

    static member InputMatrixProvider =
        Benchmarks<_>.InputMatrixProviderBuilder "BFSBenchmarks.txt"

type WithTransferBenchmark<'elem when 'elem : struct>(
    buildFunToBenchmark,
    converter: string -> 'elem,
    boolConverter,
    vertex,
    buildMatrix) =

    inherit Benchmarks<'elem>(
        buildFunToBenchmark,
        converter,
        boolConverter,
        vertex,
        buildMatrix)

    [<GlobalSetup>]
    override this.GlobalSetup() =
        this.ReadMatrix()

    [<GlobalCleanup>]
    override this.GlobalCleanup() =
        this.ClearResult()

    [<IterationCleanup>]
    override this.IterationCleanup() =
        this.ClearInputMatrix()
        this.ClearResult()

    [<Benchmark>]
    override this.Benchmark() =
        this.LoadMatrixToGPU()
        this.BFS()
        this.ResultLevels.ToHost this.Processor |> ignore
        this.Processor.PostAndReply Msg.MsgNotifyMe

type BFSWithTransferBenchmarkInt32() =

    inherit WithTransferBenchmark<int>(
        (Algorithms.BFS.singleSource ArithmeticOperations.intSumOption ArithmeticOperations.intMulOption),
        int32,
        (fun _ -> Utils.nextInt (System.Random())),
        0,
        (fun context matrix -> ClMatrix.CSR <| matrix.ToCSR.ToDevice context))

    static member InputMatrixProvider =
        Benchmarks<_>.InputMatrixProviderBuilder "BFSBenchmarks.txt"

