namespace GraphBLAS.FSharp.Benchmarks.Vector

namespace GraphBLAS.FSharp.Benchmarks.Synthetic

open FsCheck
open BenchmarkDotNet.Attributes

open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Benchmarks
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Objects.ClVectorExtensions
open GraphBLAS.FSharp.Backend.Vector
open GraphBLAS.FSharp.Backend.Objects.ClContext

[<AbstractClass>]
[<IterationCount(100)>]
[<WarmupCount(10)>]
[<Config(typeof<Configs.MinMaxMean>)>]
type Map2<'elem when 'elem : struct>(
    buildFunToBenchmark,
    generator: Gen<Vector<'elem> * Vector<'elem>>) =

    let mutable funToBenchmark = None

    let mutable firstVector = Unchecked.defaultof<ClVector<'elem>>

    let mutable secondVector = Unchecked.defaultof<ClVector<'elem>>

    member val HostVectorPair = Unchecked.defaultof<Vector<'elem> * Vector<'elem>> with get, set

    member val ResultVector = Unchecked.defaultof<ClVector<'elem>> with get,set

    [<ParamsSource("AvailableContexts")>]
    member val OclContextInfo = Unchecked.defaultof<Utils.BenchmarkContext * int> with get, set

    [<Params(1000000)>]
    member val Size = Unchecked.defaultof<int> with get, set

    member this.OclContext: ClContext = (fst this.OclContextInfo).ClContext
    member this.WorkGroupSize = snd this.OclContextInfo

    member this.Processor =
        let p = (fst this.OclContextInfo).Queue
        p.Error.Add(fun e -> failwithf $"%A{e}")
        p

    static member AvailableContexts = Utils.avaliableContexts

    member this.FunToBenchmark =
        match funToBenchmark with
        | None ->
            let x = buildFunToBenchmark this.OclContext this.WorkGroupSize
            funToBenchmark <- Some x
            x
        | Some x -> x

    member this.Map2() =
        try

        this.ResultVector <- this.FunToBenchmark this.Processor HostInterop firstVector secondVector

        with
            | ex when ex.Message = "InvalidBufferSize" -> ()
            | ex -> raise ex

    member this.ClearInputVectors()=
        firstVector.Dispose this.Processor
        secondVector.Dispose this.Processor

    member this.ClearResult() =
        this.ResultVector.Dispose this.Processor

    member this.CreateVectors()  =
        this.HostVectorPair <- List.last (Gen.sample this.Size 1 generator)

    member this.LoadVectorsToGPU() =
        firstVector <- (fst this.HostVectorPair).ToDevice this.OclContext
        secondVector <- (snd this.HostVectorPair).ToDevice this.OclContext

    abstract member GlobalSetup: unit -> unit

    abstract member IterationSetup: unit -> unit

    abstract member Benchmark: unit -> unit

    abstract member IterationCleanup: unit -> unit

    abstract member GlobalCleanup: unit -> unit


type VectorEWiseBenchmarksWithoutDataTransfer<'elem when 'elem : struct>(
        buildFunToBenchmark,
        generator) =

    inherit Map2<'elem>(
        buildFunToBenchmark,
        generator)

    [<GlobalSetup>]
    override this.GlobalSetup() = ()

    [<IterationSetup>]
    override this.IterationSetup() =
        this.CreateVectors()
        this.LoadVectorsToGPU()
        this.Processor.PostAndReply Msg.MsgNotifyMe

    [<Benchmark>]
    override this.Benchmark() =
        this.Map2()
        this.Processor.PostAndReply Msg.MsgNotifyMe

    [<IterationCleanup>]
    override this.IterationCleanup() =
        this.ClearResult()
        this.ClearInputVectors()

    [<GlobalCleanup>]
    override this.GlobalCleanup() = ()

type VectorEWiseBenchmarksWithDataTransfer<'elem when 'elem : struct>(
        buildFunToBenchmark,
        generator) =

    inherit Map2<'elem>(
        buildFunToBenchmark,
        generator)

    [<GlobalSetup>]
    override this.GlobalSetup() = ()

    [<IterationSetup>]
    override this.IterationSetup() =
        this.CreateVectors()

    [<Benchmark>]
    override this.Benchmark () =
        this.LoadVectorsToGPU()
        this.Map2()
        this.ResultVector.ToHost this.Processor |> ignore
        this.Processor.PostAndReply Msg.MsgNotifyMe

    [<IterationCleanup>]
    override this.IterationCleanup () =
        this.ClearInputVectors()
        this.ClearResult()

    [<GlobalCleanup>]
    override this.GlobalCleanup() = ()

/// Without data transfer
type VectorSparseMap2FloatWithoutTransferBenchmark() =

    inherit VectorEWiseBenchmarksWithoutDataTransfer<float>(
        (fun context -> Vector.map2 context ArithmeticOperations.floatSumOption),
        VectorGenerator.floatPair Sparse)

type VectorSparseMap2Int32WithoutTransferBenchmark() =

    inherit VectorEWiseBenchmarksWithoutDataTransfer<int32>(
        (fun context -> Vector.map2 context ArithmeticOperations.intSumOption),
        VectorGenerator.intPair Sparse)

/// General
type VectorSparseMap2GeneralFloatWithoutTransferBenchmark() =

    inherit VectorEWiseBenchmarksWithoutDataTransfer<float>(
        (fun context -> Vector.map2 context ArithmeticOperations.floatSumOption),
        VectorGenerator.floatPair Sparse)

type VectorSparseMap2GeneralInt32WithoutTransferBenchmark() =

    inherit VectorEWiseBenchmarksWithoutDataTransfer<int32>(
        (fun context -> Vector.map2 context ArithmeticOperations.intSumOption),
        VectorGenerator.intPair Sparse)

/// With data transfer
type VectorSparseMap2FloatWithTransferBenchmark() =

    inherit VectorEWiseBenchmarksWithDataTransfer<float>(
        (fun context -> Vector.map2 context ArithmeticOperations.floatSumOption),
        VectorGenerator.floatPair Sparse)

type VectorSparseMap2Int32WithTransferBenchmark() =

    inherit VectorEWiseBenchmarksWithDataTransfer<int32>(
        (fun context -> Vector.map2 context ArithmeticOperations.intSumOption),
        VectorGenerator.intPair Sparse)

/// Map2 with data transfer
type VectorMap2GeneralFloatSparseWithTransferBenchmark() =

    inherit VectorEWiseBenchmarksWithDataTransfer<float>(
        (fun context -> Vector.map2 context ArithmeticOperations.floatSumOption),
        VectorGenerator.floatPair Sparse)

type VectorMap2GeneralInt32SparseWithTransferBenchmark() =

    inherit VectorEWiseBenchmarksWithDataTransfer<int32>(
        (fun context -> Vector.map2 context ArithmeticOperations.intSumOption),
        VectorGenerator.intPair Sparse)
