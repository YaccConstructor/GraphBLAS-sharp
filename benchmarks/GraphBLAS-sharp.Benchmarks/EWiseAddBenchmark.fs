namespace GraphBLAS.FSharp.Benchmarks

open GraphBLAS.FSharp
open GraphBLAS.FSharp.Algorithms
open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Configs
open BenchmarkDotNet.Columns
open BenchmarkDotNet.Engines
open System.IO
open System
open MatrixBackend
open GraphBLAS.FSharp.Predefined
open MathNet.Numerics
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation
open Brahma.FSharp.OpenCL.Extensions
open OpenCL.Net

type COOFormat<'a> = {
    Rows: int []
    Columns: int []
    Values: 'a []
    RowCount: int
    ColumnCount: int
}

[<MinColumn; MaxColumn>]
[<Config(typeof<Config>)>]
[<SimpleJob(RunStrategy.Throughput)>]
type EWiseAddBenchmarks() =
    let mutable leftCOO = Unchecked.defaultof<Matrix<float>>
    let mutable rightCOO = Unchecked.defaultof<Matrix<float>>

    let mutable leftCSR = Unchecked.defaultof<Matrix<float>>
    let mutable rightCSR = Unchecked.defaultof<Matrix<float>>

    let mutable leftMathNetSparse =
        Unchecked.defaultof<LinearAlgebra.Matrix<float>>

    let mutable rightMathNetSparse =
        Unchecked.defaultof<LinearAlgebra.Matrix<float>>

    let mutable firstGraph = Unchecked.defaultof<COOFormat<float>>
    let mutable secondGraph = Unchecked.defaultof<COOFormat<float>>

    let contextN = OpenCLEvaluationContext("NVIDIA*", DeviceType.Gpu)
    let contextA = OpenCLEvaluationContext("AMD*", DeviceType.Gpu)

    [<ParamsSource("GraphPaths")>]
    member val PathToGraphPair = Unchecked.defaultof<string * string> with get, set

    [<GlobalSetup>]
    member this.ReadMatrices() =
        let getFullPathToGraph filename =
            Path.Join [| __SOURCE_DIRECTORY__
                         "Datasets"
                         "EWiseAddDatasets"
                         filename |]

        let getCOO (pathToGraph: string) =
            use streamReader = new StreamReader(pathToGraph)

            while streamReader.Peek() = int '%' do
                streamReader.ReadLine() |> ignore

            let matrixInfo =
                streamReader.ReadLine().Split(' ')
                |> Array.map int

            let (nrows, ncols, nnz) =
                matrixInfo.[0], matrixInfo.[1], matrixInfo.[2]

            [ 0 .. nnz - 1 ]
            |> List.map
                (fun _ ->
                    streamReader.ReadLine().Split(' ')
                    |> (fun line -> int line.[0], int line.[1], float line.[2]))
            |> List.toArray
            |> Array.sortBy (fun (first, _, _) -> first)
            |> Array.unzip3
            |> fun (rows, cols, values) ->
                let c f x y = f y x
                let rows = rows |> Array.map (c (-) 1)
                let cols = cols |> Array.map (c (-) 1)
                { Rows = rows
                  Columns = cols
                  Values = values
                  RowCount = nrows
                  ColumnCount = ncols }

        firstGraph <- fst this.PathToGraphPair |> getFullPathToGraph |> getCOO
        secondGraph <- snd this.PathToGraphPair |> getFullPathToGraph |> getCOO

    [<IterationSetup(Targets=[|"NEWiseAdditionCOO"; "AEWiseAdditionCOO"|])>]
    member this.BuildCOO() =
        leftCOO <-
            Matrix.Build<float>(
                firstGraph.RowCount,
                firstGraph.ColumnCount,
                firstGraph.Rows,
                firstGraph.Columns,
                firstGraph.Values,
                COO
            )

        rightCOO <-
            Matrix.Build<float>(
                secondGraph.RowCount,
                secondGraph.ColumnCount,
                secondGraph.Rows,
                secondGraph.Columns,
                secondGraph.Values,
                COO
            )

    [<IterationSetup(Target="EWiseAdditionCSR")>]
    member this.BuildCSR() =
        leftCSR <-
            Matrix.Build<float>(
                firstGraph.RowCount,
                firstGraph.ColumnCount,
                firstGraph.Rows,
                firstGraph.Columns,
                firstGraph.Values,
                CSR
            )

        rightCSR <-
            Matrix.Build<float>(
                secondGraph.RowCount,
                secondGraph.ColumnCount,
                secondGraph.Rows,
                secondGraph.Columns,
                secondGraph.Values,
                CSR
            )

    [<IterationSetup(Target="EWiseAdditionMathNetSparse")>]
    member this.BuildMathNetSparse() =
        leftMathNetSparse <-
            LinearAlgebra.SparseMatrix.ofListi
                firstGraph.RowCount
                firstGraph.ColumnCount
                (List.ofArray <| Array.zip3 firstGraph.Rows firstGraph.Columns firstGraph.Values)

        rightMathNetSparse <-
            LinearAlgebra.SparseMatrix.ofListi
                secondGraph.RowCount
                secondGraph.ColumnCount
                (List.ofArray <| Array.zip3 secondGraph.Rows secondGraph.Columns secondGraph.Values)

    [<Benchmark>]
    member this.NEWiseAdditionCOO() =
        leftCOO.EWiseAdd rightCOO None FloatSemiring.addMult
        |> contextN.RunSync

    [<Benchmark>]
    member this.AEWiseAdditionCOO() =
        leftCOO.EWiseAdd rightCOO None FloatSemiring.addMult
        |> contextA.RunSync

    [<Benchmark>]
    member this.EWiseAdditionCSR() =
        leftCSR.EWiseAdd rightCOO None FloatSemiring.addMult
        |> oclContext.RunSync

    [<Benchmark(Baseline=true)>]
    member this.EWiseAdditionMathNetSparse() = leftMathNetSparse + rightMathNetSparse

    /// Sequence of paths to files where data for benchmarking will be taken from
    static member GraphPaths =
        seq {
            yield ("arc130.mtx", "arc130.mtx")
        }
