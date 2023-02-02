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
open GraphBLAS.FSharp.Backend.Matrix.COO
open GraphBLAS.FSharp.Backend.Matrix.CSR
open GraphBLAS.FSharp.Objects.Matrix
open GraphBLAS.FSharp.Benchmarks.MatrixExtensions
open GraphBLAS.FSharp.Backend.Objects.ClContext

[<AbstractClass>]
[<IterationCount(100)>]
[<WarmupCount(10)>]
[<Config(typeof<Config>)>]
type EWiseAddBenchmarks<'matrixT, 'elem when 'matrixT :> IDeviceMemObject and 'elem : struct>(
        buildFunToBenchmark,
        converter: string -> 'elem,
        converterBool,
        buildMatrix) =

    let mutable funToBenchmark = None
    let mutable firstMatrix = Unchecked.defaultof<'matrixT>
    let mutable secondMatrix = Unchecked.defaultof<'matrixT>
    let mutable firstMatrixHost = Unchecked.defaultof<_>
    let mutable secondMatrixHost = Unchecked.defaultof<_>

    member val ResultMatrix = Unchecked.defaultof<'matrixT> with get,set

    [<ParamsSource("AvaliableContexts")>]
    member val OclContextInfo = Unchecked.defaultof<Utils.BenchmarkContext * int> with get, set

    [<ParamsSource("InputMatricesProvider")>]
    member val InputMatrixReader = Unchecked.defaultof<MtxReader*MtxReader> with get, set

    member this.OclContext:ClContext = (fst this.OclContextInfo).ClContext
    member this.WorkGroupSize = snd this.OclContextInfo

    member this.Processor =
        let p = (fst this.OclContextInfo).Queue
        p.Error.Add(fun e -> failwithf "%A" e)
        p

    static member AvaliableContexts = Utils.avaliableContexts

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

    member this.ReadMatrix (reader:MtxReader) =
        let converter =
            match reader.Field with
            | Pattern -> converterBool
            | _ -> converter

        reader.ReadMatrix converter

    member this.EWiseAddition() =
        this.ResultMatrix <- this.FunToBenchmark this.Processor HostInterop firstMatrix secondMatrix

    member this.ClearInputMatrices() =
        (firstMatrix :> IDeviceMemObject).Dispose this.Processor
        (secondMatrix :> IDeviceMemObject).Dispose this.Processor

    member this.ClearResult() =
        (this.ResultMatrix :> IDeviceMemObject).Dispose this.Processor

    member this.ReadMatrices() =
        let leftMatrixReader = fst this.InputMatrixReader
        let rightMatrixReader = snd this.InputMatrixReader
        firstMatrixHost <- this.ReadMatrix leftMatrixReader
        secondMatrixHost <- this.ReadMatrix rightMatrixReader

    member this.LoadMatricesToGPU () =
        firstMatrix <- buildMatrix this.OclContext firstMatrixHost
        secondMatrix <- buildMatrix this.OclContext secondMatrixHost

    abstract member GlobalSetup : unit -> unit

    abstract member IterationCleanup : unit -> unit

    abstract member GlobalCleanup : unit -> unit

    abstract member Benchmark : unit -> unit

type EWiseAddBenchmarksWithoutDataTransfer<'matrixT, 'elem when 'matrixT :> IDeviceMemObject and 'elem : struct>(
        buildFunToBenchmark,
        converter: string -> 'elem,
        converterBool,
        buildMatrix) =

    inherit EWiseAddBenchmarks<'matrixT, 'elem>(
        buildFunToBenchmark,
        converter,
        converterBool,
        buildMatrix)

    [<GlobalSetup>]
    override this.GlobalSetup() =
        this.ReadMatrices ()
        this.LoadMatricesToGPU ()

    [<IterationCleanup>]
    override this.IterationCleanup () =
        this.ClearResult()

    [<GlobalCleanup>]
    override this.GlobalCleanup () =
        this.ClearInputMatrices()

    [<Benchmark>]
    override this.Benchmark () =
        this.EWiseAddition()
        this.Processor.PostAndReply(Msg.MsgNotifyMe)

type EWiseAddBenchmarksWithDataTransfer<'matrixT, 'elem when 'matrixT :> IDeviceMemObject and 'elem : struct>(
        buildFunToBenchmark,
        converter: string -> 'elem,
        converterBool,
        buildMatrix,
        resultToHost) =

    inherit EWiseAddBenchmarks<'matrixT, 'elem>(
        buildFunToBenchmark,
        converter,
        converterBool,
        buildMatrix)

    [<GlobalSetup>]
    override this.GlobalSetup () =
        this.ReadMatrices ()

    [<GlobalCleanup>]
    override this.GlobalCleanup () = ()

    [<IterationCleanup>]
    override this.IterationCleanup () =
        this.ClearInputMatrices()
        this.ClearResult()

    [<Benchmark>]
    override this.Benchmark () =
        this.LoadMatricesToGPU()
        this.EWiseAddition()
        this.Processor.PostAndReply Msg.MsgNotifyMe
        let res = resultToHost this.ResultMatrix this.Processor
        this.Processor.PostAndReply Msg.MsgNotifyMe

module M =
    let resultToHostCOO (resultMatrix: ClMatrix.COO<'a>) (processor :MailboxProcessor<_>) =
        let cols =
            let a = Array.zeroCreate resultMatrix.ColumnCount
            processor.Post(Msg.CreateToHostMsg<_>(resultMatrix.Columns,a))
            a
        let rows =
            let a = Array.zeroCreate resultMatrix.RowCount
            processor.Post(Msg.CreateToHostMsg(resultMatrix.Rows,a))
            a
        let vals =
            let a = Array.zeroCreate resultMatrix.Values.Length
            processor.Post(Msg.CreateToHostMsg(resultMatrix.Values,a))
            a
        {
            RowCount = resultMatrix.RowCount
            ColumnCount = resultMatrix.ColumnCount
            Rows = rows
            Columns = cols
            Values = vals
        }


type EWiseAddBenchmarks4Float32COOWithoutDataTransfer() =

    inherit EWiseAddBenchmarksWithoutDataTransfer<ClMatrix.COO<float32>,float32>(
        (fun context wgSize -> COOMatrix.map2 context ArithmeticOperations.float32Sum wgSize),
        float32,
        (fun _ -> Utils.nextSingle (System.Random())),
        Matrix.ToBackendCOO
        )

    static member InputMatricesProvider =
        EWiseAddBenchmarks<_,_>.InputMatricesProviderBuilder "EWiseAddBenchmarks4Float32COO.txt"

type EWiseAddBenchmarks4Float32COOWithDataTransfer() =

    inherit EWiseAddBenchmarksWithDataTransfer<ClMatrix.COO<float32>,float32>(
        (fun context wgSize -> COOMatrix.map2 context ArithmeticOperations.float32Sum wgSize),
        float32,
        (fun _ -> Utils.nextSingle (System.Random())),
        Matrix.ToBackendCOO<float32>,
        M.resultToHostCOO
        )

    static member InputMatricesProvider =
        EWiseAddBenchmarks<_,_>.InputMatricesProviderBuilder "EWiseAddBenchmarks4Float32COO.txt"


type EWiseAddBenchmarks4BoolCOOWithoutDataTransfer() =

    inherit EWiseAddBenchmarksWithoutDataTransfer<ClMatrix.COO<bool>,bool>(
        (fun context wgSize -> COOMatrix.map2 context ArithmeticOperations.boolSum wgSize),
        (fun _ -> true),
        (fun _ -> true),
        Matrix.ToBackendCOO<bool>
        )

    static member InputMatricesProvider =
        EWiseAddBenchmarks<_, _>.InputMatricesProviderBuilder "EWiseAddBenchmarks4BoolCOO.txt"


type EWiseAddBenchmarks4Float32CSRWithoutDataTransfer() =

    inherit EWiseAddBenchmarksWithoutDataTransfer<ClMatrix.CSR<float32>,float32>(
        (fun context wgSize -> CSRMatrix.map2 context ArithmeticOperations.float32Sum wgSize),
        float32,
        (fun _ -> Utils.nextSingle (System.Random())),
        Matrix.ToBackendCSR
        )

    static member InputMatricesProvider =
        EWiseAddBenchmarks<_, _>.InputMatricesProviderBuilder "EWiseAddBenchmarks4Float32CSR.txt"


type EWiseAddBenchmarks4BoolCSRWithoutDataTransfer() =

    inherit EWiseAddBenchmarksWithoutDataTransfer<ClMatrix.CSR<bool>,bool>(
        (fun context wgSize -> CSRMatrix.map2 context ArithmeticOperations.boolSum wgSize),
        (fun _ -> true),
        (fun _ -> true),
        Matrix.ToBackendCSR
        )

    static member InputMatricesProvider =
        EWiseAddBenchmarks<_, _>.InputMatricesProviderBuilder "EWiseAddBenchmarks4BoolCSR.txt"

// With AtLeastOne

type EWiseAddAtLeastOneBenchmarks4BoolCOOWithoutDataTransfer() =

    inherit EWiseAddBenchmarksWithoutDataTransfer<ClMatrix.COO<bool>,bool>(
        (fun context wgSize -> COOMatrix.map2AtLeastOne context ArithmeticOperations.boolSumAtLeastOne wgSize),
        (fun _ -> true),
        (fun _ -> true),
        Matrix.ToBackendCOO<bool>
        )

    static member InputMatricesProvider =
        EWiseAddBenchmarks<_, _>.InputMatricesProviderBuilder "EWiseAddBenchmarks4BoolCSR.txt"

type EWiseAddAtLeastOneBenchmarks4BoolCSRWithoutDataTransfer() =

    inherit EWiseAddBenchmarksWithoutDataTransfer<ClMatrix.CSR<bool>,bool>(
        (fun context wgSize -> CSRMatrix.map2AtLeastOne context ArithmeticOperations.boolSumAtLeastOne wgSize),
        (fun _ -> true),
        (fun _ -> true),
        Matrix.ToBackendCSR
        )

    static member InputMatricesProvider =
        EWiseAddBenchmarks<_, _>.InputMatricesProviderBuilder "EWiseAddBenchmarks4BoolCSR.txt"

type EWiseAddAtLeastOneBenchmarks4Float32COOWithoutDataTransfer() =

    inherit EWiseAddBenchmarksWithoutDataTransfer<ClMatrix.COO<float32>,float32>(
        (fun context wgSize -> COOMatrix.map2AtLeastOne context ArithmeticOperations.float32SumAtLeastOne wgSize),
        float32,
        (fun _ -> Utils.nextSingle (System.Random())),
        Matrix.ToBackendCOO<float32>
        )

    static member InputMatricesProvider =
        EWiseAddBenchmarks<_,_>.InputMatricesProviderBuilder "EWiseAddBenchmarks4Float32COO.txt"

type EWiseAddAtLeastOneBenchmarks4Float32CSRWithoutDataTransfer() =

    inherit EWiseAddBenchmarksWithoutDataTransfer<ClMatrix.CSR<float32>,float32>(
        (fun context wgSize -> CSRMatrix.map2AtLeastOne context ArithmeticOperations.float32SumAtLeastOne wgSize),
        float32,
        (fun _ -> Utils.nextSingle (System.Random())),
        Matrix.ToBackendCSR<float32>
        )

    static member InputMatricesProvider =
        EWiseAddBenchmarks<_,_>.InputMatricesProviderBuilder "EWiseAddBenchmarks4Float32COO.txt"
