module GraphBLAS.FSharp.Benchmarks.Matrix.SpGeMM.Expand

open System.IO
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.IO
open BenchmarkDotNet.Attributes
open Brahma.FSharp
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Matrix
open GraphBLAS.FSharp.Backend.Objects.ClContextExtensions
open GraphBLAS.FSharp.Benchmarks
open GraphBLAS.FSharp.Backend

[<AbstractClass>]
[<IterationCount(100)>]
[<WarmupCount(10)>]
[<Config(typeof<Configs.Matrix2>)>]
type Benchmarks<'elem when 'elem : struct>(
        buildFunToBenchmark,
        converter: string -> 'elem,
        converterBool,
        buildMatrix) =

    let mutable funToBenchmark = None

    let mutable firstMatrix = Unchecked.defaultof<ClMatrix<'elem>>
    let mutable secondMatrix = Unchecked.defaultof<ClMatrix<'elem>>

    let mutable firstMatrixHost = Unchecked.defaultof<_>
    let mutable secondMatrixHost = Unchecked.defaultof<_>

    member val ResultMatrix = Unchecked.defaultof<ClMatrix.COO<'elem> option> with get, set

    [<ParamsSource("AvailableContexts")>]
    member val OclContextInfo = Unchecked.defaultof<Utils.BenchmarkContext * int> with get, set

    [<ParamsSource("InputMatrixProvider")>]
    member val InputMatrixReader = Unchecked.defaultof<MtxReader> with get, set

    member this.OclContext:ClContext = (fst this.OclContextInfo).ClContext
    member this.WorkGroupSize = snd this.OclContextInfo

    member this.Processor =
        let p = (fst this.OclContextInfo).Queue
        p.Error.Add(fun e -> failwithf "%A" e)
        p

    static member AvailableContexts = Utils.availableContexts

    static member InputMatrixProviderBuilder pathToConfig =
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

    member this.ReadMatrix (reader: MtxReader) =
        let converter =
            match reader.Field with
            | Pattern -> converterBool
            | _ -> converter

        reader.ReadMatrix converter

    member this.Mxm() =
        this.ResultMatrix <- this.FunToBenchmark this.Processor DeviceOnly firstMatrix secondMatrix

    member this.ClearInputMatrices() =
        firstMatrix.Dispose this.Processor
        secondMatrix.Dispose this.Processor

    member this.ClearResult() =
        match this.ResultMatrix with
        | Some matrix -> matrix.Dispose this.Processor
        | None -> ()

    member this.ReadMatrices() =
        firstMatrixHost <- this.ReadMatrix this.InputMatrixReader
        secondMatrixHost <- this.ReadMatrix this.InputMatrixReader

    member this.LoadMatricesToGPU () =
        firstMatrix <- buildMatrix this.OclContext firstMatrixHost
        secondMatrix <- buildMatrix this.OclContext secondMatrixHost

    abstract member GlobalSetup : unit -> unit

    abstract member Benchmark : unit -> unit

    abstract member IterationCleanup : unit -> unit

    abstract member GlobalCleanup : unit -> unit

module WithoutTransfer =
    type Benchmark<'elem when 'elem : struct>(
            buildFunToBenchmark,
            converter: string -> 'elem,
            converterBool,
            buildMatrix) =

        inherit Benchmarks<'elem>(
            buildFunToBenchmark,
            converter,
            converterBool,
            buildMatrix)

        [<GlobalSetup>]
        override this.GlobalSetup() =
            this.ReadMatrices()
            this.LoadMatricesToGPU()

        [<Benchmark>]
        override this.Benchmark() =
            this.Mxm()
            this.Processor.PostAndReply(Msg.MsgNotifyMe)

        [<IterationCleanup>]
        override this.IterationCleanup () =
            this.ClearResult()

        [<GlobalCleanup>]
        override this.GlobalCleanup () =
            this.ClearInputMatrices()

    type Float32() =

        inherit Benchmark<float32>(
            Matrix.SpGeMM.expand (fst ArithmeticOperations.float32Add) (fst ArithmeticOperations.float32Mul),
            float32,
            (fun _ -> Utils.nextSingle (System.Random())),
            (fun context matrix -> ClMatrix.CSR <| matrix.ToCSR.ToDevice context)
            )

        static member InputMatrixProvider =
            Benchmarks<_>.InputMatrixProviderBuilder "SpGeMM.txt"
