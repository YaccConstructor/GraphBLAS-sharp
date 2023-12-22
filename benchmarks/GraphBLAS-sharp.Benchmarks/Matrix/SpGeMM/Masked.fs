namespace GraphBLAS.FSharp.Benchmarks.Matrix.SpGeMM

open System.IO
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.IO
open BenchmarkDotNet.Attributes
open Brahma.FSharp
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Objects.ClContextExtensions
open GraphBLAS.FSharp.Objects.MailboxProcessorExtensions
open GraphBLAS.FSharp.Benchmarks

[<AbstractClass>]
[<IterationCount(100)>]
[<WarmupCount(10)>]
[<Config(typeof<Configs.Matrix2>)>]
type Masked<'elem when 'elem : struct>(
        buildFunToBenchmark,
        converter: string -> 'elem,
        converterBool,
        buildMatrix) =

    let mutable funToBenchmark = None
    let mutable funCSR2CSC = None
    let mutable funCSC2CSR = None

    let mutable firstMatrix = Unchecked.defaultof<ClMatrix<'elem>>
    let mutable secondMatrix = Unchecked.defaultof<ClMatrix<'elem>>
    let mutable mask = Unchecked.defaultof<ClMatrix<_>>

    let mutable firstMatrixHost = Unchecked.defaultof<_>
    let mutable secondMatrixHost = Unchecked.defaultof<_>
    let mutable maskHost = Unchecked.defaultof<Matrix<_>>

    member val ResultMatrix = Unchecked.defaultof<ClMatrix<'elem>> with get, set

    [<ParamsSource("AvaliableContexts")>]
    member val OclContextInfo = Unchecked.defaultof<Utils.BenchmarkContext * int> with get, set

    [<ParamsSource("InputMatrixProvider")>]
    member val InputMatrixReader = Unchecked.defaultof<MtxReader * MtxReader> with get, set

    member this.OclContext:ClContext = (fst this.OclContextInfo).ClContext
    member this.WorkGroupSize = snd this.OclContextInfo

    member this.Processor =
        let p = (fst this.OclContextInfo).Queue
        p.Error.Add(fun e -> failwithf "%A" e)
        p

    static member AvaliableContexts = Utils.availableContexts

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
                    , MtxReader(Utils.getFullPathToMatrix datasetFolder ("squared_" + matrixFilename))
                | _ -> failwith "Unsupported matrix format")

    member this.FunToBenchmark =
        match funToBenchmark with
        | None ->
            let x = buildFunToBenchmark this.OclContext this.WorkGroupSize
            funToBenchmark <- Some x
            x
        | Some x -> x

    member this.FunCSR2CSC =
        match funCSR2CSC with
        | None ->
            let x = Matrix.toCSCInPlace this.OclContext this.WorkGroupSize
            funCSR2CSC <- Some x
            x
        | Some x -> x

    member this.FunCSC2CSR =
        match funCSC2CSR with
        | None ->
            let x = Matrix.toCSRInPlace this.OclContext this.WorkGroupSize
            funCSC2CSR <- Some x
            x
        | Some x -> x

    member this.ReadMatrix (reader: MtxReader) =
        let converter =
            match reader.Field with
            | Pattern -> converterBool
            | _ -> converter

        reader.ReadMatrix converter

    member this.Mxm() =
        this.ResultMatrix <- this.FunToBenchmark this.Processor firstMatrix secondMatrix mask

    member this.ClearInputMatrices() =
        firstMatrix.Dispose this.Processor
        secondMatrix.Dispose this.Processor
        mask.Dispose this.Processor

    member this.ClearResult() =
        this.ResultMatrix.Dispose this.Processor

    member this.ReadMask(maskReader) =
        maskHost <- Matrix.COO <| this.ReadMatrix maskReader

    member this.ReadMatrices() =
        let matrixReader, maskReader = this.InputMatrixReader
        firstMatrixHost <- this.ReadMatrix matrixReader
        secondMatrixHost <- this.ReadMatrix matrixReader
        this.ReadMask(maskReader)

    member this.LoadMatricesToGPU () =
        firstMatrix <- buildMatrix this.OclContext firstMatrixHost
        secondMatrix <- buildMatrix this.OclContext secondMatrixHost
        mask <- maskHost.ToDevice this.OclContext

    member this.ConvertSecondMatrixToCSC() =
        secondMatrix <- this.FunCSR2CSC this.Processor HostInterop secondMatrix

    member this.ConvertSecondMatrixToCSR() =
        secondMatrix <- this.FunCSC2CSR this.Processor HostInterop secondMatrix

    abstract member GlobalSetup : unit -> unit

    abstract member Benchmark : unit -> unit

    abstract member IterationCleanup : unit -> unit

    abstract member GlobalCleanup : unit -> unit

type MxmBenchmarksMultiplicationOnly<'elem when 'elem : struct>(
        buildFunToBenchmark,
        converter: string -> 'elem,
        converterBool,
        buildMatrix) =

    inherit Masked<'elem>(
        buildFunToBenchmark,
        converter,
        converterBool,
        buildMatrix)

    [<GlobalSetup>]
    override this.GlobalSetup() =
        this.ReadMatrices ()
        this.LoadMatricesToGPU ()
        this.ConvertSecondMatrixToCSC()
        finish this.Processor

    [<Benchmark>]
    override this.Benchmark () =
        this.Mxm()
        finish this.Processor

    [<IterationCleanup>]
    override this.IterationCleanup () =
        this.ClearResult()
        finish this.Processor

    [<GlobalCleanup>]
    override this.GlobalCleanup () =
        this.ClearInputMatrices()

type MxmBenchmarksWithTransposing<'elem when 'elem : struct>(
        buildFunToBenchmark,
        converter: string -> 'elem,
        converterBool,
        buildMatrix) =

    inherit Masked<'elem>(
        buildFunToBenchmark,
        converter,
        converterBool,
        buildMatrix)

    [<GlobalSetup>]
    override this.GlobalSetup() =
        this.ReadMatrices()
        this.LoadMatricesToGPU ()
        finish this.Processor

    [<Benchmark>]
    override this.Benchmark() =
        this.ConvertSecondMatrixToCSC()
        this.Mxm()
        finish this.Processor


    [<IterationCleanup>]
    override this.IterationCleanup() =
        this.ClearResult()
        this.ConvertSecondMatrixToCSR()
        finish this.Processor

    [<GlobalCleanup>]
    override this.GlobalCleanup() =
        this.ClearInputMatrices()

type Mxm4Float32MultiplicationOnlyBenchmark() =

    inherit MxmBenchmarksMultiplicationOnly<float32>(
        Operations.SpGeMM.masked  (fst ArithmeticOperations.float32Add) (fst ArithmeticOperations.float32Mul),
        float32,
        (fun _ -> Utils.nextSingle (System.Random())),
        (fun context matrix -> ClMatrix.CSR <| matrix.ToCSR.ToDevice context)
        )

    static member InputMatrixProvider =
        Masked<_>.InputMatrixProviderBuilder "MxmBenchmarks4Float32.txt"

type Mxm4Float32WithTransposingBenchmark() =

    inherit MxmBenchmarksWithTransposing<float32>(
        Operations.SpGeMM.masked (fst ArithmeticOperations.float32Add) (fst ArithmeticOperations.float32Mul),
        float32,
        (fun _ -> Utils.nextSingle (System.Random())),
        (fun context matrix -> ClMatrix.CSR <| matrix.ToCSR.ToDevice context)
        )

    static member InputMatrixProvider =
        Masked<_>.InputMatrixProviderBuilder "MxmBenchmarks4Float32.txt"

type Mxm4BoolMultiplicationOnlyBenchmark() =

    inherit MxmBenchmarksMultiplicationOnly<bool>(
        (Operations.SpGeMM.masked (fst ArithmeticOperations.boolAdd) (fst ArithmeticOperations.boolMul)),
        (fun _ -> true),
        (fun _ -> true),
        (fun context matrix -> ClMatrix.CSR <| matrix.ToCSR.ToDevice context)
        )

    static member InputMatrixProvider =
        Masked<_>.InputMatrixProviderBuilder "MxmBenchmarks4Bool.txt"

type Mxm4BoolWithTransposingBenchmark() =

    inherit MxmBenchmarksWithTransposing<bool>(
        (Operations.SpGeMM.masked (fst ArithmeticOperations.boolAdd) (fst ArithmeticOperations.boolMul)),
        (fun _ -> true),
        (fun _ -> true),
        (fun context matrix -> ClMatrix.CSR <| matrix.ToCSR.ToDevice context)
        )

    static member InputMatrixProvider =
        Masked<_>.InputMatrixProviderBuilder "MxmBenchmarks4Bool.txt"

type Mxm4Float32MultiplicationOnlyWithZerosFilterBenchmark() =

    inherit MxmBenchmarksMultiplicationOnly<float32>(
        (Operations.SpGeMM.masked (fst ArithmeticOperations.float32Add) (fst ArithmeticOperations.float32Mul)),
        float32,
        (fun _ -> Utils.nextSingle (System.Random())),
        (fun context matrix -> ClMatrix.CSR <| matrix.ToCSR.ToDevice context)
        )

    static member InputMatrixProvider =
        Masked<_>.InputMatrixProviderBuilder "MxmBenchmarks4Float32.txt"

type Mxm4Float32WithTransposingWithZerosFilterBenchmark() =

    inherit MxmBenchmarksWithTransposing<float32>(
        Operations.SpGeMM.masked (fst ArithmeticOperations.float32Add) (fst ArithmeticOperations.float32Mul),
        float32,
        (fun _ -> Utils.nextSingle (System.Random())),
        (fun context matrix -> ClMatrix.CSR <| matrix.ToCSR.ToDevice context)
        )

    static member InputMatrixProvider =
        Masked<_>.InputMatrixProviderBuilder "MxmBenchmarks4Float32.txt"
