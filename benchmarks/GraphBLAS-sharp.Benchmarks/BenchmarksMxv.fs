namespace GraphBLAS.FSharp.Benchmarks

open GraphBLAS.FSharp
open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Configs
open BenchmarkDotNet.Columns
open System.IO
open GraphBLAS.FSharp.IO

type MxvConfig() =
    inherit ManualConfig()

    do
        base.AddColumn(
            MatrixShapeColumn("RowCount", fun mtxReader -> mtxReader.ReadMatrixShape().RowCount) :> IColumn,
            MatrixShapeColumn("ColumnCount", fun mtxReader -> mtxReader.ReadMatrixShape().ColumnCount) :> IColumn,
            MatrixShapeColumn("NNZ", fun mtxReader -> mtxReader.ReadMatrixShape().Nnz) :> IColumn,
            TEPSColumn() :> IColumn,
            StatisticColumn.Min,
            StatisticColumn.Max
        ) |> ignore

[<IterationCount(10)>]
[<WarmupCount(3)>]
[<InvocationCount(3)>]
[<Config(typeof<MxvConfig>)>]
type MxvBenchmarks() =
    let rand = System.Random()

    let mutable matrix = Unchecked.defaultof<Matrix<float>>
    let mutable vector = Unchecked.defaultof<Vector<float>>
    let semiring = Predefined.AddMult.float

    [<ParamsSource("AvaliableContextsProvider")>]
    member val OclContext = Unchecked.defaultof<ClContext> with get, set
    member this.Context =
        let (ClContext context) = this.OclContext
        context

    [<ParamsSource("InputMatricesProvider")>]
    member val InputMatrixReader = Unchecked.defaultof<MtxReader> with get, set

    [<GlobalSetup>]
    member this.BuildMatrix() =
        let inputMatrix = this.InputMatrixReader.ReadMatrixReal(float)

        matrix <-
            graphblas {
                return! Matrix.switch CSR inputMatrix
                >>= Matrix.synchronizeAndReturn
            }
            |> EvalGB.withClContext this.Context
            |> EvalGB.runSync

    [<IterationSetup>]
    member this.BuildVector() =
        vector <-
            graphblas {
                return!
                    [ for i = 0 to matrix.ColumnCount - 1 do if rand.Next() % 2 = 0 then yield (i, 1.) ]
                    |> Vector.ofList matrix.ColumnCount
                // >>= Vector.synchronizeAndReturn
            }
            |> EvalGB.withClContext this.Context
            |> EvalGB.runSync

    [<Benchmark>]
    member this.Mxv() =
        Matrix.mxv semiring matrix vector
        |> EvalGB.withClContext this.Context
        |> EvalGB.runSync

    [<IterationCleanup>]
    member this.ClearBuffers() =
        this.Context.Provider.CloseAllBuffers()

    [<GlobalCleanup>]
    member this.ClearContext() =
        let (ClContext context) = this.OclContext
        context.Provider.Dispose()

    static member AvaliableContextsProvider = Utils.avaliableContexts

    static member InputMatricesProvider =
        "Common.txt"
        |> Utils.getMatricesFilenames
        |> Seq.map
            (fun matrixFilename ->
                match Path.GetExtension matrixFilename with
                | ".mtx" -> MtxReader(Utils.getFullPathToMatrix "Common" matrixFilename)
                | _ -> failwith "Unsupported matrix format"
            )
