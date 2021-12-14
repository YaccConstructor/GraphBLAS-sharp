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
            MatrixShapeColumn("RowCount", (fun matrix -> matrix.ReadMatrixShape().RowCount)) :> IColumn,
            MatrixShapeColumn("ColumnCount", (fun matrix -> matrix.ReadMatrixShape().ColumnCount)) :> IColumn,
            MatrixShapeColumn(
                "NNZ",
                fun matrix ->
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

[<IterationCount(5)>]
[<WarmupCount(3)>]
[<Config(typeof<Config>)>]
type EWiseAddBenchmarks<'matrixT, 'elem when 'matrixT :> System.IDisposable and 'elem : struct>(buildFunToBenchmark, converter: string -> 'elem, converterBool, buildMatrix) =

    let mutable funToBenchmark = None
    let mutable firstMatrix = Unchecked.defaultof<'matrixT>
    let mutable secondMatrix = Unchecked.defaultof<'matrixT>
    let mutable resultMatrix = Unchecked.defaultof<'matrixT>
    
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

    [<Benchmark>]
    member this.EWiseAddition() =
        resultMatrix <- this.FunToBenchmark this.Processor firstMatrix secondMatrix

    [<GlobalSetup>]
    member this.PrepareInputData () =
        let leftMatrixReader = fst this.InputMatrixReader
        let rightMatrixReader = snd this.InputMatrixReader
        firstMatrix <- buildMatrix this.OclContext <| this.ReadMatrix leftMatrixReader 
        secondMatrix <- buildMatrix this.OclContext <| this.ReadMatrix rightMatrixReader 
    
    [<IterationCleanup>]
    member this.ClearResult() =
        (resultMatrix :> System.IDisposable).Dispose()

    [<GlobalCleanup>]
    member this.ClearInputMatrices() =
        (firstMatrix :> System.IDisposable).Dispose()        
        (secondMatrix :> System.IDisposable).Dispose()        


module M = 
    let inline buildCooMatrix (context:ClContext) matrix =
        match matrix with
        | MatrixCOO m ->
            let rows =
                context.CreateClArray m.Rows

            let cols =
                context.CreateClArray m.Columns

            let vals =
                context.CreateClArray m.Values
            
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
            let rows =
                context.CreateClArray m.Rows

            let cols =
                context.CreateClArray m.Columns

            let vals =
                context.CreateClArray m.Values
            
            { Backend.CSRMatrix.Context = context
              Backend.CSRMatrix.RowCount = m.RowCount
              Backend.CSRMatrix.ColumnCount = m.ColumnCount
              /// !!!! FIXME
              Backend.CSRMatrix.RowPointers = rows
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