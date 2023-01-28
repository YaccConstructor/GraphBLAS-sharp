namespace GraphBLAS.FSharp.Benchmarks

open System.IO
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.IO
open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Configs
open BenchmarkDotNet.Columns
open Brahma.FSharp
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Algorithms
open MatrixExtensions
open ArraysExtensions

[<AbstractClass>]
[<IterationCount(10)>]
[<WarmupCount(5)>]
[<Config(typeof<AlgorithmConfig>)>]
type BFSBenchmarks<'matrixT, 'elem when 'matrixT :> IDeviceMemObject and 'elem : struct>(
        buildFunToBenchmark,
        converter: string -> 'elem,
        converterBool,
        buildMatrix) =

    let mutable funToBenchmark = None
    let mutable matrix = Unchecked.defaultof<'matrixT>
    let mutable matrixHost = Unchecked.defaultof<_>

    let source = 0

    member val ResultVector = Unchecked.defaultof<ClArray<'elem option>> with get,set

    [<ParamsSource("AvaliableContexts")>]
    member val OclContextInfo = Unchecked.defaultof<Utils.BenchmarkContext * int> with get, set

    [<ParamsSource("InputMatricesProvider")>]
    member val InputMatrixReader = Unchecked.defaultof<MtxReader> with get, set

    member this.OclContext:ClContext = (fst this.OclContextInfo).ClContext
    member this.WorkGroupSize = snd this.OclContextInfo

    member this.Processor =
        let p = (fst this.OclContextInfo).Queue
        p.Error.Add(fun e -> failwithf "%A" e)
        p

    static member AvaliableContexts = Utils.avaliableContexts

    static member InputMatricesProviderBuilder pathToConfig =
        let datasetFolder = ""
        pathToConfig
        |> Utils.getMatricesFilenames
        |> Seq.map
            (fun matrixFilename ->
                printfn "%A" matrixFilename

                match Path.GetExtension matrixFilename with
                | ".mtx" ->
                    MtxReader(Utils.getFullPathToMatrix datasetFolder matrixFilename)
                | _ -> failwith "Unsupported matrix format")

    member this.FunToBenchmark =
        match funToBenchmark with
        | None ->
            let x = buildFunToBenchmark this.OclContext this.WorkGroupSize
            funToBenchmark <- Some x
            x
        | Some x -> x

    member this.ReadMatrix (reader:MtxReader) =
        let converter =
            match reader.Field with
            | Pattern -> converterBool
            | _ -> converter

        reader.ReadMatrix converter

    member this.BFS() =
        this.ResultVector <- this.FunToBenchmark this.Processor matrix source

    member this.ClearInputMatrix() =
        (matrix :> IDeviceMemObject).Dispose this.Processor

    member this.ClearResult() =
        this.ResultVector.Dispose this.Processor

    member this.ReadMatrix() =
        let matrixReader = this.InputMatrixReader
        matrixHost <- this.ReadMatrix matrixReader

    member this.LoadMatrixToGPU() =
        matrix <- buildMatrix this.OclContext matrixHost

    abstract member GlobalSetup : unit -> unit

    abstract member IterationCleanup : unit -> unit

    abstract member GlobalCleanup : unit -> unit

    abstract member Benchmark : unit -> unit

type BFSBenchmarksWithoutDataTransfer() =

    inherit BFSBenchmarks<ClMatrix.CSR<int>, int>(
        (fun context wgSize -> BFS.singleSource context ArithmeticOperations.intSum ArithmeticOperations.intMul wgSize),
        int,
        (fun _ -> Utils.nextInt (System.Random())),
        Matrix.ToBackendCSR)

    static member InputMatricesProvider =
        BFSBenchmarks<_,_>.InputMatricesProviderBuilder "BFSBenchmarks.txt"

    [<GlobalSetup>]
    override this.GlobalSetup() =
        this.ReadMatrix ()
        this.LoadMatrixToGPU ()

    [<IterationCleanup>]
    override this.IterationCleanup() =
        this.ClearResult()

    [<GlobalCleanup>]
    override this.GlobalCleanup() =
        this.ClearInputMatrix()

    [<Benchmark>]
    override this.Benchmark() =
        this.BFS()
        this.Processor.PostAndReply(Msg.MsgNotifyMe)

type BFSBenchmarksWithDataTransfer<'matrixT, 'elem when 'matrixT :> IDeviceMemObject and 'elem : struct>(
        buildFunToBenchmark,
        converter: string -> 'elem,
        converterBool,
        buildMatrix,
        resultToHost) =

    inherit BFSBenchmarks<'matrixT, 'elem>(
        buildFunToBenchmark,
        converter,
        converterBool,
        buildMatrix)

    [<GlobalSetup>]
    override this.GlobalSetup() =
        this.ReadMatrix()

    [<GlobalCleanup>]
    override this.GlobalCleanup() = ()

    [<IterationCleanup>]
    override this.IterationCleanup() =
        this.ClearInputMatrix()
        this.ClearResult()

    [<Benchmark>]
    override this.Benchmark() =
        this.LoadMatrixToGPU()
        this.BFS()
        this.Processor.PostAndReply Msg.MsgNotifyMe
        let res = resultToHost this.ResultVector this.Processor
        this.Processor.PostAndReply Msg.MsgNotifyMe

