namespace GraphBLAS.FSharp.Benchmarks.Matrix.Map2

open System.IO
open BenchmarkDotNet.Attributes
open Brahma.FSharp
open GraphBLAS.FSharp.IO
open GraphBLAS.FSharp.Operations
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Objects.MatrixExtensions
open GraphBLAS.FSharp.Objects.ClContextExtensions
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Benchmarks

[<AbstractClass>]
[<IterationCount(100)>]
[<WarmupCount(10)>]
[<Config(typeof<Configs.Matrix2>)>]
type Benchmarks<'matrixT, 'elem when 'matrixT :> IDeviceMemObject and 'elem : struct>(
        buildFunToBenchmark,
        converter: string -> 'elem,
        converterBool,
        buildMatrix: Matrix.COO<_> -> Matrix<_>) =

    let mutable funToBenchmark = None
    let mutable firstMatrix = Unchecked.defaultof<ClMatrix<'elem>>
    let mutable secondMatrix = Unchecked.defaultof<ClMatrix<'elem>>
    let mutable firstMatrixHost = Unchecked.defaultof<_>
    let mutable secondMatrixHost = Unchecked.defaultof<_>

    member val ResultMatrix = Unchecked.defaultof<ClMatrix<'elem>> with get,set

    [<ParamsSource("AvailableContexts")>]
    member val OclContextInfo = Unchecked.defaultof<Utils.BenchmarkContext * int> with get, set

    [<ParamsSource("InputMatricesProvider")>]
    member val InputMatrixReader = Unchecked.defaultof<MtxReader*MtxReader> with get, set

    member this.OclContext: ClContext = (fst this.OclContextInfo).ClContext
    member this.WorkGroupSize = snd this.OclContextInfo

    member this.Processor =
        let p = (fst this.OclContextInfo).Queue
        p.Error.Add(fun e -> failwithf "%A" e)
        p

    static member AvailableContexts = Utils.availableContexts

    static member InputMatricesProviderBuilder pathToConfig =
        let datasetFolder = "EWiseAdd"
        pathToConfig
        |> Utils.getMatricesFilenames
        |> Seq.map
            (fun matrixFilename ->
                printfn "%A" matrixFilename

                match Path.GetExtension matrixFilename with
                | ".mtx" ->
                    MtxReader(Utils.getFullPathToMatrix datasetFolder matrixFilename)
                    , MtxReader(Utils.getFullPathToMatrix datasetFolder ("squared_" + matrixFilename))
                | _ -> failwith "Unsupported matrix format")

    member this.FunToBenchmark =
        match funToBenchmark with
        | None ->
            let x = buildFunToBenchmark this.OclContext this.WorkGroupSize
            funToBenchmark <- Some x
            x
        | Some x -> x

    member this.ReadMatrix (reader: MtxReader) =
        let converter =
            match reader.Field with
            | Pattern -> converterBool
            | _ -> converter

        reader.ReadMatrix converter

    member this.EWiseAddition() =
        this.ResultMatrix <- this.FunToBenchmark this.Processor HostInterop firstMatrix secondMatrix

    member this.ClearInputMatrices() =
        firstMatrix.Dispose this.Processor
        secondMatrix.Dispose this.Processor

    member this.ClearResult() =
        this.ResultMatrix.Dispose this.Processor

    member this.ReadMatrices() =
        firstMatrixHost <- this.ReadMatrix <| fst this.InputMatrixReader
        secondMatrixHost <- this.ReadMatrix <| snd this.InputMatrixReader

    member this.LoadMatricesToGPU () =
        firstMatrix <- (buildMatrix firstMatrixHost).ToDevice this.OclContext
        secondMatrix <- (buildMatrix secondMatrixHost).ToDevice this.OclContext

    abstract member GlobalSetup: unit -> unit

    abstract member Benchmark: unit -> unit

    abstract member IterationCleanup: unit -> unit

    abstract member GlobalCleanup: unit -> unit

module WithoutTransfer =
    type Benchmark<'matrixT, 'elem when 'matrixT :> IDeviceMemObject and 'elem : struct>(
            buildFunToBenchmark,
            converter: string -> 'elem,
            converterBool,
            buildMatrix) =

        inherit Benchmarks<'matrixT, 'elem>(
            buildFunToBenchmark,
            converter,
            converterBool,
            buildMatrix)

        [<GlobalSetup>]
        override this.GlobalSetup() =
            this.ReadMatrices ()
            this.LoadMatricesToGPU ()
            this.Processor.PostAndReply(Msg.MsgNotifyMe)

        [<Benchmark>]
        override this.Benchmark () =
            this.EWiseAddition()
            this.Processor.PostAndReply(Msg.MsgNotifyMe)

        [<IterationCleanup>]
        override this.IterationCleanup () =
            this.ClearResult()

        [<GlobalCleanup>]
        override this.GlobalCleanup () =
            this.ClearInputMatrices()

    module COO =
        type Float32() =

            inherit Benchmark<ClMatrix.COO<float32>,float32>(
                (Matrix.map2 ArithmeticOperations.float32SumOption),
                float32,
                (fun _ -> Utils.nextSingle (System.Random())),
                Matrix.COO
                )

            static member InputMatricesProvider =
                Benchmarks<_,_>.InputMatricesProviderBuilder "EWiseAddBenchmarks4Float32COO.txt"

        type Bool() =

            inherit Benchmark<ClMatrix.COO<bool>,bool>(
                (Matrix.map2 ArithmeticOperations.boolSumOption),
                (fun _ -> true),
                (fun _ -> true),
                Matrix.COO
                )

            static member InputMatricesProvider =
                Benchmarks<_, _>.InputMatricesProviderBuilder "EWiseAddBenchmarks4BoolCOO.txt"

    module CSR =
        type Float32() =

            inherit Benchmark<ClMatrix.CSR<float32>,float32>(
                (Matrix.map2 ArithmeticOperations.float32SumOption),
                float32,
                (fun _ -> Utils.nextSingle (System.Random())),
                (fun matrix -> Matrix.CSR matrix.ToCSR)
                )

            static member InputMatricesProvider =
                Benchmarks<_, _>.InputMatricesProviderBuilder "EWiseAddBenchmarks4Float32CSR.txt"

        type Bool() =

            inherit Benchmark<ClMatrix.CSR<bool>,bool>(
                (Matrix.map2 ArithmeticOperations.boolSumOption),
                (fun _ -> true),
                (fun _ -> true),
                (fun matrix -> Matrix.CSR matrix.ToCSR)
                )

            static member InputMatricesProvider =
                Benchmarks<_, _>.InputMatricesProviderBuilder "EWiseAddBenchmarks4BoolCSR.txt"

    module AtLeastOne =
        module COO =
            type Bool() =

                inherit Benchmark<ClMatrix.COO<bool>,bool>(
                    (Matrix.map2AtLeastOne ArithmeticOperations.boolSumAtLeastOne),
                    (fun _ -> true),
                    (fun _ -> true),
                    Matrix.COO
                    )

                static member InputMatricesProvider =
                    Benchmarks<_, _>.InputMatricesProviderBuilder "EWiseAddBenchmarks4BoolCOO.txt"

            type Float32() =

                inherit Benchmark<ClMatrix.COO<float32>,float32>(
                    (Matrix.map2AtLeastOne ArithmeticOperations.float32SumAtLeastOne),
                    float32,
                    (fun _ -> Utils.nextSingle (System.Random())),
                    Matrix.COO
                    )

                static member InputMatricesProvider =
                    Benchmarks<_,_>.InputMatricesProviderBuilder "EWiseAddBenchmarks4Float32COO.txt"

        module CSR =
            type Bool() =

                inherit Benchmark<ClMatrix.CSR<bool>,bool>(
                    (Matrix.map2AtLeastOne ArithmeticOperations.boolSumAtLeastOne),
                    (fun _ -> true),
                    (fun _ -> true),
                    (fun matrix -> Matrix.CSR matrix.ToCSR)
                    )

                static member InputMatricesProvider =
                    Benchmarks<_, _>.InputMatricesProviderBuilder "EWiseAddBenchmarks4BoolCSR.txt"

            type Float32() =

                inherit Benchmark<ClMatrix.CSR<float32>,float32>(
                    (Matrix.map2AtLeastOne ArithmeticOperations.float32SumAtLeastOne),
                    float32,
                    (fun _ -> Utils.nextSingle (System.Random())),
                    (fun matrix -> Matrix.CSR matrix.ToCSR)
                    )

                static member InputMatricesProvider =
                    Benchmarks<_,_>.InputMatricesProviderBuilder "EWiseAddBenchmarks4Float32COO.txt"

module WithTransfer =
    type Benchmark<'matrixT, 'elem when 'matrixT :> IDeviceMemObject and 'elem : struct>(
            buildFunToBenchmark,
            converter: string -> 'elem,
            converterBool,
            buildMatrix,
            resultToHost) =

        inherit Benchmarks<'matrixT, 'elem>(
            buildFunToBenchmark,
            converter,
            converterBool,
            buildMatrix)

        [<GlobalSetup>]
        override this.GlobalSetup() =
            this.ReadMatrices()

        [<GlobalCleanup>]
        override this.GlobalCleanup() = ()

        [<IterationCleanup>]
        override this.IterationCleanup() =
            this.ClearInputMatrices()
            this.ClearResult()

        [<Benchmark>]
        override this.Benchmark() =
            this.LoadMatricesToGPU()
            this.EWiseAddition()
            this.Processor.PostAndReply Msg.MsgNotifyMe
            resultToHost this.ResultMatrix this.Processor |> ignore
            this.Processor.PostAndReply Msg.MsgNotifyMe

    module COO =
        type Float32() =

            inherit Benchmark<ClMatrix.COO<float32>,float32>(
                (Matrix.map2 ArithmeticOperations.float32SumOption),
                float32,
                (fun _ -> Utils.nextSingle (System.Random())),
                Matrix.COO,
                (fun matrix -> matrix.ToHost)
                )

            static member InputMatricesProvider =
                Benchmarks<_,_>.InputMatricesProviderBuilder "EWiseAddBenchmarks4Float32COO.txt"

