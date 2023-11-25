module GraphBLAS.FSharp.Benchmarks.Vector.Map2

open FsCheck
open BenchmarkDotNet.Attributes
open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Benchmarks
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Objects.ClVectorExtensions
open GraphBLAS.FSharp.Objects.ClContextExtensions

[<AbstractClass>]
[<IterationCount(100)>]
[<WarmupCount(10)>]
[<Config(typeof<Configs.MinMaxMean>)>]
type Benchmarks<'elem when 'elem : struct>(
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

    static member AvailableContexts = Utils.availableContexts

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

module WithoutTransfer =
    type Benchmark<'elem when 'elem : struct>(
            buildFunToBenchmark,
            generator) =

        inherit Benchmarks<'elem>(
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

    type Float() =

        inherit Benchmark<float>(
            (Operations.Vector.map2 ArithmeticOperations.floatSumOption),
            VectorGenerator.floatPair Sparse)

    type Int32() =

        inherit Benchmark<int32>(
            (Operations.Vector.map2 ArithmeticOperations.intSumOption),
            VectorGenerator.intPair Sparse)

    module AtLeastOne =
        type Float() =

            inherit Benchmark<float>(
                (Operations.Vector.map2AtLeastOne ArithmeticOperations.floatSumAtLeastOne),
                VectorGenerator.floatPair Sparse)

        type Int32() =

            inherit Benchmark<int32>(
                (Operations.Vector.map2AtLeastOne ArithmeticOperations.intSumAtLeastOne),
                VectorGenerator.intPair Sparse)

module WithTransfer =
    type Benchmark<'elem when 'elem : struct>(
            buildFunToBenchmark,
            generator) =

        inherit Benchmarks<'elem>(
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

    type Float() =

        inherit Benchmark<float>(
            (Operations.Vector.map2 ArithmeticOperations.floatSumOption),
            VectorGenerator.floatPair Sparse)

    type Int32() =

        inherit Benchmark<int32>(
            (Operations.Vector.map2 ArithmeticOperations.intSumOption),
            VectorGenerator.intPair Sparse)

    module AtLeastOne =
        type Float() =

            inherit Benchmark<float>(
                (Operations.Vector.map2AtLeastOne ArithmeticOperations.floatSumAtLeastOne),
                VectorGenerator.floatPair Sparse)

        type Int32() =

            inherit Benchmark<int32>(
                (Operations.Vector.map2AtLeastOne ArithmeticOperations.intSumAtLeastOne),
                VectorGenerator.intPair Sparse)
