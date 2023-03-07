namespace GraphBLAS.FSharp.Benchmarks

open System.IO
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.IO
open BenchmarkDotNet.Attributes
open MathNet.Numerics.LinearAlgebra
open MathNet.Numerics
open Microsoft.FSharp.Core

[<AbstractClass>]
[<IterationCount(100)>]
[<WarmupCount(10)>]
[<Config(typeof<CommonConfig>)>]
type MathNETBenchmark<'elem when 'elem: struct and 'elem :> System.IEquatable<'elem> and 'elem :> System.IFormattable and 'elem :> System.ValueType and 'elem: (new :
    unit -> 'elem)>(converter: string -> 'elem, converterBool) =
    do Control.UseNativeMKL()

    static member COOMatrixToMathNETSparse matrix =
        match matrix with
        | Matrix.COO matrix ->
            Matrix.Build.SparseFromCoordinateFormat(
                matrix.RowCount,
                matrix.ColumnCount,
                matrix.Values.Length,
                matrix.Rows,
                matrix.Columns,
                matrix.Values
            )
        | _ -> failwith "Unsupported"

    member this.ReadMatrix(reader: MtxReader) =
        let converter =
            match reader.Field with
            | Pattern -> converterBool
            | _ -> converter

        let gbMatrix = reader.ReadMatrix converter
        MathNETBenchmark<_>.COOMatrixToMathNETSparse gbMatrix

    abstract member GlobalSetup : unit -> unit

    abstract member IterationCleanup : unit -> unit

    abstract member Benchmark : unit -> unit

type BinOpMathNETBenchmark<'elem when 'elem: struct and 'elem :> System.IEquatable<'elem> and 'elem :> System.IFormattable and 'elem :> System.ValueType and 'elem: (new :
    unit -> 'elem)>(funToBenchmark, converter: string -> 'elem, converterBool) =
    inherit MathNETBenchmark<'elem>(converter, converterBool)

    let mutable firstMatrix = Unchecked.defaultof<Matrix<'elem>>
    let mutable secondMatrix = Unchecked.defaultof<Matrix<'elem>>

    member val ResultMatrix = Unchecked.defaultof<Matrix<'elem>> with get, set

    [<ParamsSource("InputMatricesProvider")>]
    member val InputMatrixReader = Unchecked.defaultof<MtxReader * MtxReader> with get, set

    static member InputMatricesProviderBuilder pathToConfig =
        let datasetFolder = "MathNET"

        pathToConfig
        |> Utils.getMatricesFilenames
        |> Seq.map
            (fun matrixFilename ->
                printfn "%A" matrixFilename

                match Path.GetExtension matrixFilename with
                | ".mtx" ->
                    MtxReader(Utils.getFullPathToMatrix datasetFolder matrixFilename),
                    MtxReader(Utils.getFullPathToMatrix datasetFolder ("squared_" + matrixFilename))
                | _ -> failwith "Unsupported matrix format")

    member this.ReadMatrices() =
        let leftMatrixReader = fst this.InputMatrixReader
        let rightMatrixReader = snd this.InputMatrixReader
        firstMatrix <- this.ReadMatrix leftMatrixReader
        secondMatrix <- this.ReadMatrix rightMatrixReader

    [<GlobalSetup>]
    override this.GlobalSetup() = this.ReadMatrices()

    [<IterationCleanup>]
    override this.IterationCleanup() =
        this.ResultMatrix <- Unchecked.defaultof<Matrix<'elem>>

    [<Benchmark>]
    override this.Benchmark() =
        this.ResultMatrix <- funToBenchmark firstMatrix secondMatrix

type EWiseAddMathNETBenchmarkFloat32() =

    inherit BinOpMathNETBenchmark<float32>((+), float32, (fun _ -> Utils.nextSingle (System.Random())))

    static member InputMatricesProvider =
        BinOpMathNETBenchmark<_>.InputMatricesProviderBuilder "EWiseAddMathNETBenchmarks4Float32.txt"
