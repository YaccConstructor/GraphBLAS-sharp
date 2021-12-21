namespace GraphBLAS.FSharp.Benchmarks

open System.IO
open System.Text.RegularExpressions
open GraphBLAS.FSharp
open GraphBLAS.FSharp.IO
open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Configs
open BenchmarkDotNet.Columns
open Brahma.FSharp.OpenCL
open OpenCL.Net

type Config() =
    inherit ManualConfig()

    do
        base.AddColumn(
            MatrixShapeColumn("RowCount", (fun (matrix,_) -> matrix.ReadMatrixShape().RowCount)) :> IColumn,
            MatrixShapeColumn("ColumnCount", (fun (matrix,_) -> matrix.ReadMatrixShape().ColumnCount)) :> IColumn,
            MatrixShapeColumn(
                "NNZ",
                fun (matrix,_) ->
                    match matrix.Format with
                    | Coordinate -> matrix.ReadMatrixShape().Nnz
                    | Array -> 0
            )
            :> IColumn,
            MatrixShapeColumn(
                "SqrNNZ",
                fun (_,matrix) ->
                    match matrix.Format with
                    | Coordinate -> matrix.ReadMatrixShape().Nnz
                    | Array -> 0
            )
            :> IColumn,
            TEPSColumn() :> IColumn,
            StatisticColumn.Min,
            StatisticColumn.Max
        )
        |> ignore

[<AbstractClass>]
[<IterationCount(100)>]
[<WarmupCount(10)>]
[<Config(typeof<Config>)>]
type EWiseAddBenchmarks<'matrixT, 'elem when 'matrixT :> System.IDisposable and 'elem : struct>(buildFunToBenchmark, converter: string -> 'elem, converterBool, buildMatrix) =

    let mutable funToBenchmark = None
    let mutable firstMatrix = Unchecked.defaultof<'matrixT>
    let mutable secondMatrix = Unchecked.defaultof<'matrixT>
    let mutable firstMatrixHost = Unchecked.defaultof<_>
    let mutable secondMatrixHost = Unchecked.defaultof<_>
    //let mutable resultMatrix = Unchecked.defaultof<'matrixT>
    member val ResultMatrix = Unchecked.defaultof<'matrixT> with get,set
    
    [<ParamsSource("AvaliableContexts")>]    
    member val OclContextInfo = Unchecked.defaultof<_> with get, set

    [<ParamsSource("InputMatricesProvider")>]
    member val InputMatrixReader = Unchecked.defaultof<MtxReader*MtxReader> with get, set
    
    member this.OclContext:ClContext = fst this.OclContextInfo
    member this.WorkGroupSize = snd this.OclContextInfo

    member this.Processor = 
        let p = this.OclContext.Provider.CommandQueue
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
        this.ResultMatrix <- this.FunToBenchmark this.Processor firstMatrix secondMatrix

    member this.ClearInputMatrices() =
        (firstMatrix :> System.IDisposable).Dispose()
        (secondMatrix :> System.IDisposable).Dispose()

    member this.ClearResult() =
        (this.ResultMatrix :> System.IDisposable).Dispose()

    member this.ReadMatrices() =
        let leftMatrixReader = fst this.InputMatrixReader
        let rightMatrixReader = snd this.InputMatrixReader
        firstMatrixHost <- this.ReadMatrix leftMatrixReader 
        secondMatrixHost <- this.ReadMatrix rightMatrixReader 

    member this.LoadMatricesToGPU () =        
        firstMatrix <- buildMatrix this.OclContext firstMatrixHost
        secondMatrix <- buildMatrix this.OclContext secondMatrixHost 

    abstract member GlobalSetup : unit -> unit 

   (* [<GlobalSetup>]
    default this.GlobalSetup() =
        this.PrepareInputData ()
*)
    abstract member IterationCleanup : unit -> unit 
(*
    [<IterationCleanup>]
    default this.IterationCleanup () = 
        this.ClearResult()
*)
    abstract member GlobalCleanup : unit -> unit 
(*
    [<GlobalCleanup>]
    default this.GlobalCleanup () = 
        this.ClearInputMatrices()
  *)  
    abstract member Benchmark : unit -> unit 
(*
    [<Benchmark>]
    default this.Benchmark () =
        this.EWiseAddition()
        this.Processor.PostAndReply(Msg.MsgNotifyMe)
*)

module M = 
    let inline buildCooMatrix (context:ClContext) matrix =
        match matrix with
        | MatrixCOO m ->
            let rows =
                context.CreateClArray (m.Rows, hostAccessMode = HostAccessMode.ReadOnly, deviceAccessMode = DeviceAccessMode.ReadOnly, allocationMode = AllocationMode.CopyHostPtr)

            let cols =
                context.CreateClArray (m.Columns, hostAccessMode = HostAccessMode.ReadOnly, deviceAccessMode = DeviceAccessMode.ReadOnly, allocationMode = AllocationMode.CopyHostPtr)

            let vals =
                context.CreateClArray (m.Values, hostAccessMode = HostAccessMode.ReadOnly, deviceAccessMode = DeviceAccessMode.ReadOnly, allocationMode = AllocationMode.CopyHostPtr)
            
            { Backend.COOMatrix.Context = context
              Backend.COOMatrix.RowCount = m.RowCount
              Backend.COOMatrix.ColumnCount = m.ColumnCount
              Backend.COOMatrix.Rows = rows
              Backend.COOMatrix.Columns = cols
              Backend.COOMatrix.Values = vals }                

        | x -> failwith "Unsupported matrix format: %A"
    
    let inline buildCsrMatrix (context:ClContext) matrix =
        match matrix with
        | MatrixCOO m ->
            let rowPointers =
                context.CreateClArray(
                    Utils.rowIndices2rowPointers m.Rows m.RowCount
                    ,hostAccessMode = HostAccessMode.ReadOnly
                    ,deviceAccessMode = DeviceAccessMode.ReadOnly
                    ,allocationMode = AllocationMode.CopyHostPtr)

            let cols =
                context.CreateClArray (
                    m.Columns
                    ,hostAccessMode = HostAccessMode.ReadOnly
                    ,deviceAccessMode = DeviceAccessMode.ReadOnly
                    ,allocationMode = AllocationMode.CopyHostPtr)

            let vals =
                context.CreateClArray (
                    m.Values
                    ,hostAccessMode = HostAccessMode.ReadOnly
                    ,deviceAccessMode = DeviceAccessMode.ReadOnly
                    ,allocationMode = AllocationMode.CopyHostPtr)
            
            { Backend.CSRMatrix.Context = context
              Backend.CSRMatrix.RowCount = m.RowCount
              Backend.CSRMatrix.ColumnCount = m.ColumnCount
              Backend.CSRMatrix.RowPointers = rowPointers
              Backend.CSRMatrix.Columns = cols
              Backend.CSRMatrix.Values = vals }                

        | x -> failwith "Unsupported matrix format: %A"

type EWiseAddBenchmarks4Float32COO() =
        
    inherit EWiseAddBenchmarks<Backend.COOMatrix<float32>,float32>(
        (fun context wgSize -> Backend.COOMatrix.eWiseAdd context <@ (+) @> wgSize),
        float32, 
        (fun _ -> Utils.nextSingle (System.Random())),
        M.buildCooMatrix        
        )

    static member InputMatricesProvider =
        EWiseAddBenchmarks<_,_>.InputMatricesProviderBuilder "EWiseAddBenchmarks4Float32COO.txt"


type EWiseAddBenchmarks4Float32COOWithoutDataTransfer() =
    inherit EWiseAddBenchmarks4Float32COO()

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

type EWiseAddBenchmarks4Float32COOWithDataTransfer() =
        
    inherit EWiseAddBenchmarks4Float32COO()
    
    member this.ResultToHost () =
        let cols =
            let a = Array.zeroCreate this.ResultMatrix.ColumnCount
            this.Processor.Post(Msg.CreateToHostMsg<_>(this.ResultMatrix.Columns,a))
            a
        let rows =
            let a = Array.zeroCreate this.ResultMatrix.RowCount
            this.Processor.Post(Msg.CreateToHostMsg(this.ResultMatrix.Rows,a))
            a
        let vals =
            let a = Array.zeroCreate this.ResultMatrix.Values.Length
            this.Processor.Post(Msg.CreateToHostMsg(this.ResultMatrix.Values,a))
            a

        this.Processor.PostAndReply(Msg.MsgNotifyMe)

        {
            RowCount = this.ResultMatrix.RowCount
            ColumnCount = this.ResultMatrix.ColumnCount
            Rows = rows
            Columns = cols
            Values = vals
        }

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
        this.LoadMatricesToGPU ()
        this.EWiseAddition()
        this.Processor.PostAndReply(Msg.MsgNotifyMe)        
        let res = this.ResultToHost ()        
        this.Processor.PostAndReply(Msg.MsgNotifyMe)

(*
type EWiseAddBenchmarks4BoolCOO() =
        
    inherit EWiseAddBenchmarks<Backend.COOMatrix<bool>,bool>(
        (fun context wgSize -> Backend.COOMatrix.eWiseAdd context <@ (||) @> wgSize),
        (fun _ -> true), 
        (fun _ -> true),
        M.buildCooMatrix        
        )

    static member InputMatricesProvider =
        EWiseAddBenchmarks<_, _>.InputMatricesProviderBuilder "EWiseAddBenchmarks4BoolCOO.txt"


type EWiseAddBenchmarks4Float32CSR() =
        
    inherit EWiseAddBenchmarks<Backend.CSRMatrix<float32>,float32>(
        (fun context wgSize -> Backend.CSRMatrix.eWiseAdd context <@ (+) @> wgSize),
        float32, 
        (fun _ -> Utils.nextSingle (System.Random())),
        M.buildCsrMatrix        
        )

    static member InputMatricesProvider =
        EWiseAddBenchmarks<_, _>.InputMatricesProviderBuilder "EWiseAddBenchmarks4Float32CSR.txt"


type EWiseAddBenchmarks4BoolCSR() =
        
    inherit EWiseAddBenchmarks<Backend.CSRMatrix<bool>,bool>(
        (fun context wgSize -> Backend.CSRMatrix.eWiseAdd context <@ (||) @> wgSize),
        (fun _ -> true), 
        (fun _ -> true),
        M.buildCsrMatrix        
        )

    static member InputMatricesProvider =
        EWiseAddBenchmarks<_, _>.InputMatricesProviderBuilder "EWiseAddBenchmarks4BoolCSR.txt"
*)