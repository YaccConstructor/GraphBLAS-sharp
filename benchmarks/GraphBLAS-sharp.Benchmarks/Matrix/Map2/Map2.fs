namespace GraphBLAS.FSharp.Benchmarks.Matrix.Map2

open System.IO
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.IO
open BenchmarkDotNet.Attributes
open Brahma.FSharp
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Objects.MatrixExtensions
open GraphBLAS.FSharp.Backend.Objects.ClContext
open GraphBLAS.FSharp.Backend.Matrix
open GraphBLAS.FSharp.Benchmarks

[<AbstractClass>]
[<IterationCount(100)>]
[<WarmupCount(10)>]
[<Config(typeof<Configs.Matrix2>)>]
type Map2<'matrixT, 'elem when 'matrixT :> IDeviceMemObject and 'elem : struct>(
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

    static member AvailableContexts = Utils.avaliableContexts

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

type Map2BenchmarksWithoutDataTransfer<'matrixT, 'elem when 'matrixT :> IDeviceMemObject and 'elem : struct>(
        buildFunToBenchmark,
        converter: string -> 'elem,
        converterBool,
        buildMatrix) =

    inherit Map2<'matrixT, 'elem>(
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

type Map2BenchmarksWithDataTransfer<'matrixT, 'elem when 'matrixT :> IDeviceMemObject and 'elem : struct>(
        buildFunToBenchmark,
        converter: string -> 'elem,
        converterBool,
        buildMatrix,
        resultToHost) =

    inherit Map2<'matrixT, 'elem>(
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

type MatrixCOOMap2Float32WithoutTransferBenchmark() =

    inherit Map2BenchmarksWithoutDataTransfer<ClMatrix.COO<float32>,float32>(
        (fun context -> Matrix.map2 context ArithmeticOperations.float32SumOption),
        float32,
        (fun _ -> Utils.nextSingle (System.Random())),
        Matrix.COO
        )

    static member InputMatricesProvider =
        Map2<_,_>.InputMatricesProviderBuilder "EWiseAddBenchmarks4Float32COO.txt"

type MatrixCOOMap2Float32WithTransferBenchmark() =

    inherit Map2BenchmarksWithDataTransfer<ClMatrix.COO<float32>,float32>(
        (fun context -> Matrix.map2 context ArithmeticOperations.float32SumOption),
        float32,
        (fun _ -> Utils.nextSingle (System.Random())),
        Matrix.COO,
        (fun matrix -> matrix.ToHost)
        )

    static member InputMatricesProvider =
        Map2<_,_>.InputMatricesProviderBuilder "EWiseAddBenchmarks4Float32COO.txt"


type MatrixCOOMap2BoolWithoutTransferBenchmark() =

    inherit Map2BenchmarksWithoutDataTransfer<ClMatrix.COO<bool>,bool>(
        (fun context -> Matrix.map2 context ArithmeticOperations.boolSumOption),
        (fun _ -> true),
        (fun _ -> true),
        Matrix.COO
        )

    static member InputMatricesProvider =
        Map2<_, _>.InputMatricesProviderBuilder "EWiseAddBenchmarks4BoolCOO.txt"


type MatrixCSRMap2Float32WithoutTransferBenchmark() =

    inherit Map2BenchmarksWithoutDataTransfer<ClMatrix.CSR<float32>,float32>(
        (fun context -> Matrix.map2 context ArithmeticOperations.float32SumOption),
        float32,
        (fun _ -> Utils.nextSingle (System.Random())),
        (fun matrix -> Matrix.CSR matrix.ToCSR)
        )

    static member InputMatricesProvider =
        Map2<_, _>.InputMatricesProviderBuilder "EWiseAddBenchmarks4Float32CSR.txt"


type MatrixCSRMap2BoolWithoutTransferBenchmark() =

    inherit Map2BenchmarksWithoutDataTransfer<ClMatrix.CSR<bool>,bool>(
        (fun context -> Matrix.map2 context ArithmeticOperations.boolSumOption),
        (fun _ -> true),
        (fun _ -> true),
        (fun matrix -> Matrix.CSR matrix.ToCSR)
        )

    static member InputMatricesProvider =
        Map2<_, _>.InputMatricesProviderBuilder "EWiseAddBenchmarks4BoolCSR.txt"

// AtLeastOne

type MatrixCOOMap2AtLeastOne4BoolWithoutTransferBenchmark() =

    inherit Map2BenchmarksWithoutDataTransfer<ClMatrix.COO<bool>,bool>(
        (fun context -> Matrix.map2AtLeastOne context ArithmeticOperations.boolSumAtLeastOne),
        (fun _ -> true),
        (fun _ -> true),
        Matrix.COO
        )

    static member InputMatricesProvider =
        Map2<_, _>.InputMatricesProviderBuilder "EWiseAddBenchmarks4BoolCSR.txt"

type MatrixCSRMap2AtLeastOne4BoolWithoutTransferBenchmark() =

    inherit Map2BenchmarksWithoutDataTransfer<ClMatrix.CSR<bool>,bool>(
        (fun context -> Matrix.map2AtLeastOne context ArithmeticOperations.boolSumAtLeastOne),
        (fun _ -> true),
        (fun _ -> true),
        (fun matrix -> Matrix.CSR matrix.ToCSR)
        )

    static member InputMatricesProvider =
        Map2<_, _>.InputMatricesProviderBuilder "EWiseAddBenchmarks4BoolCSR.txt"

type MatrixCOOMap2AtLeastOne4Float32WithoutTransferBenchmark() =

    inherit Map2BenchmarksWithoutDataTransfer<ClMatrix.COO<float32>,float32>(
        (fun context -> Matrix.map2AtLeastOne context ArithmeticOperations.float32SumAtLeastOne),
        float32,
        (fun _ -> Utils.nextSingle (System.Random())),
        Matrix.COO
        )

    static member InputMatricesProvider =
        Map2<_,_>.InputMatricesProviderBuilder "EWiseAddBenchmarks4Float32COO.txt"

type MatrixCSRMap2AtLeastOne4Float32CSRWithoutTransferBenchmark() =

    inherit Map2BenchmarksWithoutDataTransfer<ClMatrix.CSR<float32>,float32>(
        (fun context -> Matrix.map2AtLeastOne context ArithmeticOperations.float32SumAtLeastOne),
        float32,
        (fun _ -> Utils.nextSingle (System.Random())),
        (fun matrix -> Matrix.CSR matrix.ToCSR)
        )

    static member InputMatricesProvider =
        Map2<_,_>.InputMatricesProviderBuilder "EWiseAddBenchmarks4Float32COO.txt"
