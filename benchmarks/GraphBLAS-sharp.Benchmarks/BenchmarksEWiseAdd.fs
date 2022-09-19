namespace GraphBLAS.FSharp.Benchmarks

open System.IO
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.IO
open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Configs
open BenchmarkDotNet.Columns
open Brahma.FSharp

type Config() =
    inherit ManualConfig()

    do
        base.AddColumn(
            MatrixShapeColumn("RowCount", (fun (matrix, _) -> matrix.ReadMatrixShape().RowCount)) :> IColumn,
            MatrixShapeColumn("ColumnCount", (fun (matrix, _) -> matrix.ReadMatrixShape().ColumnCount)) :> IColumn,
            MatrixShapeColumn(
                "NNZ",
                fun (matrix, _) ->
                    match matrix.Format with
                    | Coordinate -> matrix.ReadMatrixShape().Nnz
                    | Array -> 0
            ) :> IColumn,
            MatrixShapeColumn(
                "SqrNNZ",
                fun (_, matrix) ->
                    match matrix.Format with
                    | Coordinate -> matrix.ReadMatrixShape().Nnz
                    | Array -> 0
            ) :> IColumn,
            TEPSColumn() :> IColumn,
            StatisticColumn.Min,
            StatisticColumn.Max
        )
        |> ignore

[<AbstractClass>]
[<IterationCount(100)>]
[<WarmupCount(10)>]
[<Config(typeof<Config>)>]
type EWiseAddBenchmarks<'matrixT, 'elem when 'matrixT :> IDeviceMemObject and 'elem : struct>
    (
        buildFunToBenchmark,
        converter: string -> 'elem,
        converterBool,
        buildMatrix
    ) =

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

    member this.Processor = (fst this.OclContextInfo).Queue

    static member AvaliableContexts = Utils.avaliableContexts

    static member InputMatricesProviderBuilder pathToConfig =
        let datasetFolder = "EWiseAdd"
        pathToConfig
        |> Utils.getMatricesFilenames
        |> Seq.map (fun matrixFilename ->
            printfn $"%A{matrixFilename}"

            match Path.GetExtension matrixFilename with
            | ".mtx" ->
                MtxReader(Utils.getFullPathToMatrix datasetFolder matrixFilename),
                MtxReader(Utils.getFullPathToMatrix datasetFolder ("squared_" + matrixFilename))
            | _ -> failwith "Unsupported matrix format"
        )

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
        this.ResultMatrix <- this.FunToBenchmark this.Processor firstMatrix secondMatrix

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

type EWiseAddBenchmarksWithoutDataTransfer<'matrixT, 'elem when 'matrixT :> IDeviceMemObject and 'elem : struct>
    (
        buildFunToBenchmark,
        converter: string -> 'elem,
        converterBool,
        buildMatrix
    ) =

    inherit EWiseAddBenchmarks<'matrixT, 'elem>(
        buildFunToBenchmark,
        converter,
        converterBool,
        buildMatrix
    )

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

type EWiseAddBenchmarksWithDataTransfer<'matrixT, 'elem when 'matrixT :> IDeviceMemObject and 'elem : struct>
    (
        buildFunToBenchmark,
        converter: string -> 'elem,
        converterBool,
        buildMatrix,
        resultToHost
    ) =

    inherit EWiseAddBenchmarks<'matrixT, 'elem>(
        buildFunToBenchmark,
        converter,
        converterBool,
        buildMatrix
    )

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
    let inline buildCooMatrix (context:ClContext) matrix =
        match matrix with
        | MatrixCOO m ->
            let rows =
                context.CreateClArray(
                    m.Rows,
                    hostAccessMode = HostAccessMode.ReadOnly,
                    deviceAccessMode = DeviceAccessMode.ReadOnly,
                    allocationMode = AllocationMode.CopyHostPtr
                )

            let cols =
                context.CreateClArray(
                    m.Columns,
                    hostAccessMode = HostAccessMode.ReadOnly,
                    deviceAccessMode = DeviceAccessMode.ReadOnly,
                    allocationMode = AllocationMode.CopyHostPtr
                )

            let vals =
                context.CreateClArray(
                    m.Values,
                    hostAccessMode = HostAccessMode.ReadOnly,
                    deviceAccessMode = DeviceAccessMode.ReadOnly,
                    allocationMode = AllocationMode.CopyHostPtr
                )

            { ClCooMatrix.Context = context
              ClCooMatrix.RowCount = m.RowCount
              ClCooMatrix.ColumnCount = m.ColumnCount
              ClCooMatrix.Rows = rows
              ClCooMatrix.Columns = cols
              ClCooMatrix.Values = vals }

        | _ -> failwith "Unsupported matrix format: %A"

    let inline buildCsrMatrix (context:ClContext) matrix =
        match matrix with
        | MatrixCOO m ->
            let rowPointers =
                context.CreateClArray(
                    Utils.rowIndices2rowPointers m.Rows m.RowCount,
                    hostAccessMode = HostAccessMode.ReadOnly,
                    deviceAccessMode = DeviceAccessMode.ReadOnly,
                    allocationMode = AllocationMode.CopyHostPtr
                )

            let cols =
                context.CreateClArray(
                    m.Columns,
                    hostAccessMode = HostAccessMode.ReadOnly,
                    deviceAccessMode = DeviceAccessMode.ReadOnly,
                    allocationMode = AllocationMode.CopyHostPtr
                )

            let vals =
                context.CreateClArray(
                    m.Values,
                    hostAccessMode = HostAccessMode.ReadOnly,
                    deviceAccessMode = DeviceAccessMode.ReadOnly,
                    allocationMode = AllocationMode.CopyHostPtr
                )

            { ClCsrMatrix.Context = context
              ClCsrMatrix.RowCount = m.RowCount
              ClCsrMatrix.ColumnCount = m.ColumnCount
              ClCsrMatrix.RowPointers = rowPointers
              ClCsrMatrix.Columns = cols
              ClCsrMatrix.Values = vals }

        | x -> failwith "Unsupported matrix format: %A"

    let resultToHostCOO (resultMatrix:Backend.ClCooMatrix<'a>) (procesor:MailboxProcessor<_>) =
        let cols =
            let a = Array.zeroCreate resultMatrix.ColumnCount
            procesor.Post(Msg.CreateToHostMsg<_>(resultMatrix.Columns,a))
            a
        let rows =
            let a = Array.zeroCreate resultMatrix.RowCount
            procesor.Post(Msg.CreateToHostMsg(resultMatrix.Rows,a))
            a
        let vals =
            let a = Array.zeroCreate resultMatrix.Values.Length
            procesor.Post(Msg.CreateToHostMsg(resultMatrix.Values,a))
            a

        {
            RowCount = resultMatrix.RowCount
            ColumnCount = resultMatrix.ColumnCount
            Rows = rows
            Columns = cols
            Values = vals
        }

type EWiseAddBenchmarks4Float32COOWithoutDataTransfer() =
    inherit EWiseAddBenchmarksWithoutDataTransfer<ClCooMatrix<float32>,float32>(
        (fun context wgSize -> COOMatrix.eWiseAdd context Backend.Common.StandardOperations.float32Sum wgSize),
        float32,
        (fun _ -> Utils.nextSingle (System.Random())),
        M.buildCooMatrix
    )

    static member InputMatricesProvider =
        EWiseAddBenchmarks<_,_>.InputMatricesProviderBuilder "EWiseAddBenchmarks4Float32COO.txt"

type EWiseAddBenchmarks4Float32COOWithDataTransfer() =
    inherit EWiseAddBenchmarksWithDataTransfer<ClCooMatrix<float32>,float32>(
        (fun context wgSize -> COOMatrix.eWiseAdd context Backend.Common.StandardOperations.float32Sum wgSize),
        float32,
        (fun _ -> Utils.nextSingle (System.Random())),
        M.buildCooMatrix,
        M.resultToHostCOO
    )

    static member InputMatricesProvider =
        EWiseAddBenchmarks<_,_>.InputMatricesProviderBuilder "EWiseAddBenchmarks4Float32COO.txt"

type EWiseAddBenchmarks4BoolCOOWithoutDataTransfer() =
    inherit EWiseAddBenchmarksWithoutDataTransfer<ClCooMatrix<bool>,bool>(
        (fun context wgSize -> COOMatrix.eWiseAdd context Backend.Common.StandardOperations.boolSum wgSize),
        (fun _ -> true),
        (fun _ -> true),
        M.buildCooMatrix
    )

    static member InputMatricesProvider =
        EWiseAddBenchmarks<_, _>.InputMatricesProviderBuilder "EWiseAddBenchmarks4BoolCOO.txt"

type EWiseAddBenchmarks4Float32CSRWithoutDataTransfer() =
    inherit EWiseAddBenchmarksWithoutDataTransfer<ClCsrMatrix<float32>,float32>(
        (fun context wgSize -> CSRMatrix.eWiseAdd context Backend.Common.StandardOperations.float32Sum wgSize),
        float32,
        (fun _ -> Utils.nextSingle (System.Random())),
        M.buildCsrMatrix
    )

    static member InputMatricesProvider =
        EWiseAddBenchmarks<_, _>.InputMatricesProviderBuilder "EWiseAddBenchmarks4Float32CSR.txt"

type EWiseAddBenchmarks4BoolCSRWithoutDataTransfer() =
    inherit EWiseAddBenchmarksWithoutDataTransfer<ClCsrMatrix<bool>,bool>(
        (fun context wgSize -> CSRMatrix.eWiseAdd context Backend.Common.StandardOperations.boolSum wgSize),
        (fun _ -> true),
        (fun _ -> true),
        M.buildCsrMatrix
    )

    static member InputMatricesProvider =
        EWiseAddBenchmarks<_, _>.InputMatricesProviderBuilder "EWiseAddBenchmarks4BoolCSR.txt"

(*
    With AtLeastOne
*)

type EWiseAddAtLeastOneBenchmarks4BoolCOOWithoutDataTransfer() =
    inherit EWiseAddBenchmarksWithoutDataTransfer<ClCooMatrix<bool>,bool>(
        (fun context wgSize -> COOMatrix.eWiseAddAtLeastOne context Backend.Common.StandardOperations.boolSumAtLeastOne wgSize),
        (fun _ -> true),
        (fun _ -> true),
        M.buildCooMatrix
    )

    static member InputMatricesProvider =
        EWiseAddBenchmarks<_, _>.InputMatricesProviderBuilder "EWiseAddBenchmarks4BoolCSR.txt"

type EWiseAddAtLeastOneBenchmarks4BoolCSRWithoutDataTransfer() =
    inherit EWiseAddBenchmarksWithoutDataTransfer<ClCsrMatrix<bool>,bool>(
        (fun context wgSize -> CSRMatrix.eWiseAddAtLeastOne context Backend.Common.StandardOperations.boolSumAtLeastOne wgSize),
        (fun _ -> true),
        (fun _ -> true),
        M.buildCsrMatrix
    )

    static member InputMatricesProvider =
        EWiseAddBenchmarks<_, _>.InputMatricesProviderBuilder "EWiseAddBenchmarks4BoolCSR.txt"

type EWiseAddAtLeastOneBenchmarks4Float32COOWithoutDataTransfer() =
    inherit EWiseAddBenchmarksWithoutDataTransfer<ClCooMatrix<float32>,float32>(
        (fun context wgSize -> COOMatrix.eWiseAddAtLeastOne context Backend.Common.StandardOperations.float32SumAtLeastOne wgSize),
        float32,
        (fun _ -> Utils.nextSingle (System.Random())),
        M.buildCooMatrix
    )

    static member InputMatricesProvider =
        EWiseAddBenchmarks<_,_>.InputMatricesProviderBuilder "EWiseAddBenchmarks4Float32COO.txt"

type EWiseAddAtLeastOneBenchmarks4Float32CSRWithoutDataTransfer() =
    inherit EWiseAddBenchmarksWithoutDataTransfer<ClCsrMatrix<float32>,float32>(
        (fun context wgSize -> CSRMatrix.eWiseAddAtLeastOne context Backend.Common.StandardOperations.float32SumAtLeastOne wgSize),
        float32,
        (fun _ -> Utils.nextSingle (System.Random())),
        M.buildCsrMatrix
    )

    static member InputMatricesProvider =
        EWiseAddBenchmarks<_,_>.InputMatricesProviderBuilder "EWiseAddBenchmarks4Float32COO.txt"
