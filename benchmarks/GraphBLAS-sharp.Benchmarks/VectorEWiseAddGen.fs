namespace GraphBLAS.FSharp.Benchmarks

open Expecto
open FsCheck
open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Configs
open BenchmarkDotNet.Columns
open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Objects.ClVectorExtensions
open GraphBLAS.FSharp.Backend.Vector
open GraphBLAS.FSharp.Backend.Objects.ClContext

type VectorConfig() =
    inherit ManualConfig()

    do
        base.AddColumn(
            StatisticColumn.Min,
            StatisticColumn.Max
        )
        |> ignore

[<AbstractClass>]
[<IterationCount(100)>]
[<WarmupCount(10)>]
[<Config(typeof<VectorConfig>)>]
type VectorEWiseBenchmarks<'elem when 'elem : struct>(
    buildFunToBenchmark,
    generator: Gen<Vector<'elem> * Vector<'elem>>) =

    let mutable funToBenchmark = None

    let mutable firstVector = Unchecked.defaultof<ClVector<'elem>>

    let mutable secondVector = Unchecked.defaultof<ClVector<'elem>>


    member val HostVectorPair = Unchecked.defaultof<Vector<'elem> * Vector<'elem>> with get, set

    member val ResultVector = Unchecked.defaultof<ClVector<'elem>> with get,set

    [<ParamsSource("AvaliableContexts")>]
    member val OclContextInfo = Unchecked.defaultof<Utils.BenchmarkContext * int> with get, set

    [<Params(100000, 10000000, 1000000000)>]
    member val Size = Unchecked.defaultof<int> with get, set

    member this.OclContext: ClContext = (fst this.OclContextInfo).ClContext
    member this.WorkGroupSize = snd this.OclContextInfo

    member this.Processor =
        let p = (fst this.OclContextInfo).Queue
        p.Error.Add(fun e -> failwithf "%A" e)
        p

    static member AvaliableContexts = Utils.avaliableContexts

    member this.FunToBenchmark =
        match funToBenchmark with
        | None ->
            let x = buildFunToBenchmark this.OclContext this.WorkGroupSize
            funToBenchmark <- Some x
            x
        | Some x -> x

    member this.EWiseAddition() =
        this.ResultVector <- this.FunToBenchmark this.Processor firstVector secondVector

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

    abstract member GlobalSetup : unit -> unit

    abstract member IterationCleanup : unit -> unit

    abstract member GlobalCleanup : unit -> unit

    abstract member Benchmark : unit -> unit

type VectorEWiseBenchmarksWithoutDataTransfer<'elem when 'elem : struct>(
        buildFunToBenchmark,
        generator) =

    inherit VectorEWiseBenchmarks<'elem>(
        buildFunToBenchmark,
        generator)

    [<GlobalSetup>]
    override this.GlobalSetup() =
        this.CreateVectors ()
        this.LoadVectorsToGPU ()

    [<IterationCleanup>]
    override this.IterationCleanup () =
        this.ClearResult()

    [<GlobalCleanup>]
    override this.GlobalCleanup () =
        this.ClearInputVectors()

    [<Benchmark>]
    override this.Benchmark () =
        this.EWiseAddition()
        this.Processor.PostAndReply(Msg.MsgNotifyMe)

type VectorEWiseBenchmarksWithDataTransfer<'elem when 'elem : struct>(
        buildFunToBenchmark,
        generator) =

    inherit VectorEWiseBenchmarks<'elem>(
        buildFunToBenchmark,
        generator)

    [<GlobalSetup>]
    override this.GlobalSetup() =
        this.CreateVectors()

    [<GlobalCleanup>]
    override this.GlobalCleanup () = ()

    [<IterationCleanup>]
    override this.IterationCleanup () =
        this.ClearInputVectors()
        this.ClearResult()

    [<Benchmark>]
    override this.Benchmark () =
        this.LoadVectorsToGPU()
        this.EWiseAddition()
        this.Processor.PostAndReply Msg.MsgNotifyMe
        this.ResultVector.ToHost this.Processor |> ignore
        this.Processor.PostAndReply Msg.MsgNotifyMe

module VectorGenerator =
    let private pairOfVectorsOfEqualSize (valuesGenerator: Gen<'a>) createVector =
        gen {
            let! length = Gen.sized <| fun size -> Gen.constant size

            let! leftArray = Gen.arrayOfLength length valuesGenerator

            let! rightArray = Gen.arrayOfLength length valuesGenerator

            return (createVector leftArray, createVector rightArray)
        }

    let intPair format =
        let createVector array = Utils.createVectorFromArray format array ((=) 0)

        pairOfVectorsOfEqualSize Arb.generate<int32> createVector

    let floatPair format =
        let normalFloatGenerator =
            (Arb.Default.NormalFloat()
            |> Arb.toGen
            |> Gen.map float)

        let fIsEqual x y = abs (x - y) < Accuracy.medium.absolute || x = y

        let createVector array = Utils.createVectorFromArray format array (fIsEqual 0.0)

        pairOfVectorsOfEqualSize normalFloatGenerator createVector

/// Without data transfer

type VectorEWiseBenchmarks4FloatSparseWithoutDataTransfer() =

    inherit VectorEWiseBenchmarksWithoutDataTransfer<float>(
        (fun context wgSize -> Vector.elementWise context ArithmeticOperations.floatSum wgSize HostInterop),
        VectorGenerator.floatPair Sparse)

type VectorEWiseBenchmarks4Int32SparseWithoutDataTransfer() =

    inherit VectorEWiseBenchmarksWithoutDataTransfer<int32>(
        (fun context wgSize -> Vector.elementWise context ArithmeticOperations.intSum wgSize HostInterop),
        VectorGenerator.intPair Sparse)

/// General

type VectorEWiseGeneralBenchmarks4FloatSparseWithoutDataTransfer() =

    inherit VectorEWiseBenchmarksWithoutDataTransfer<float>(
        (fun context wgSize -> Vector.elementwiseGeneral context ArithmeticOperations.floatSum wgSize HostInterop),
        VectorGenerator.floatPair Sparse)

type VectorEWiseGeneralBenchmarks4Int32SparseWithoutDataTransfer() =

    inherit VectorEWiseBenchmarksWithoutDataTransfer<int32>(
        (fun context wgSize -> Vector.elementwiseGeneral context ArithmeticOperations.intSum wgSize HostInterop),
        VectorGenerator.intPair Sparse)

/// With data transfer

type VectorEWiseBenchmarks4FloatSparseWithDataTransfer() =

    inherit VectorEWiseBenchmarksWithDataTransfer<float>(
        (fun context wgSize -> Vector.elementWise context ArithmeticOperations.floatSum wgSize HostInterop),
        VectorGenerator.floatPair Sparse)

type VectorEWiseBenchmarks4Int32SparseWithDataTransfer() =

    inherit VectorEWiseBenchmarksWithDataTransfer<int32>(
        (fun context wgSize -> Vector.elementWise context ArithmeticOperations.intSum wgSize HostInterop),
        VectorGenerator.intPair Sparse)

/// General with data transfer

type VectorEWiseGeneralBenchmarks4FloatSparseWithDataTransfer() =

    inherit VectorEWiseBenchmarksWithDataTransfer<float>(
        (fun context wgSize -> Vector.elementwiseGeneral context ArithmeticOperations.floatSum wgSize HostInterop),
        VectorGenerator.floatPair Sparse)

type VectorEWiseGeneralBenchmarks4Int32SparseWithDataTransfer() =

    inherit VectorEWiseBenchmarksWithDataTransfer<int32>(
        (fun context wgSize -> Vector.elementwiseGeneral context ArithmeticOperations.intSum wgSize HostInterop),
        VectorGenerator.intPair Sparse)
