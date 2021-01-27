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

type InputMatrixFormat = {
    MatrixName: string
    MatrixStructure: COOFormat<float>
}
with
    override this.ToString() =
        sprintf "%s (%i, %i)"
            this.MatrixName
            this.MatrixStructure.RowCount
            this.MatrixStructure.ColumnCount

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

    let mutable firstMatrix = Unchecked.defaultof<COOFormat<float>>
    let mutable secondMatrix = Unchecked.defaultof<COOFormat<float>>

    [<ParamsSource("InputMatricesProvider")>]
    member val InputMatrix = Unchecked.defaultof<InputMatrixFormat> with get, set

    [<GlobalSetup>]
    member this.FormInputData() =
        let transposeCOO (matrix: COOFormat<float>) =
            (matrix.Rows, matrix.Columns, matrix.Values)
            |||> Array.zip3
            |> Array.sortBy (fun (row, col, value) -> row)
            |> Array.unzip3
            |>
                fun (rows, cols, values) ->
                    {
                        Rows = cols
                        Columns = rows
                        Values = values
                        RowCount = matrix.ColumnCount
                        ColumnCount = matrix.RowCount
                    }

        firstMatrix <- this.InputMatrix.MatrixStructure
        secondMatrix <- transposeCOO this.InputMatrix.MatrixStructure

    [<IterationSetup(Target="EWiseAdditionCOO")>]
    member this.BuildCOO() =
        leftCOO <-
            Matrix.Build<float>(
                firstMatrix.RowCount,
                firstMatrix.ColumnCount,
                firstMatrix.Rows,
                firstMatrix.Columns,
                firstMatrix.Values,
                COO
            )

        rightCOO <-
            Matrix.Build<float>(
                secondMatrix.RowCount,
                secondMatrix.ColumnCount,
                secondMatrix.Rows,
                secondMatrix.Columns,
                secondMatrix.Values,
                COO
            )

    [<IterationSetup(Target="EWiseAdditionCSR")>]
    member this.BuildCSR() =
        leftCSR <-
            Matrix.Build<float>(
                firstMatrix.RowCount,
                firstMatrix.ColumnCount,
                firstMatrix.Rows,
                firstMatrix.Columns,
                firstMatrix.Values,
                CSR
            )

        rightCSR <-
            Matrix.Build<float>(
                secondMatrix.RowCount,
                secondMatrix.ColumnCount,
                secondMatrix.Rows,
                secondMatrix.Columns,
                secondMatrix.Values,
                CSR
            )

    [<IterationSetup(Target="EWiseAdditionMathNetSparse")>]
    member this.BuildMathNetSparse() =
        leftMathNetSparse <-
            LinearAlgebra.SparseMatrix.ofListi
                firstMatrix.RowCount
                firstMatrix.ColumnCount
                (List.ofArray <| Array.zip3 firstMatrix.Rows firstMatrix.Columns firstMatrix.Values)

        rightMathNetSparse <-
            LinearAlgebra.SparseMatrix.ofListi
                secondMatrix.RowCount
                secondMatrix.ColumnCount
                (List.ofArray <| Array.zip3 secondMatrix.Rows secondMatrix.Columns secondMatrix.Values)

    [<Benchmark>]
    [<ArgumentsSource("AvaliableContextsProvider")>]
    member this.EWiseAdditionCOO(context: OpenCLEvaluationContext) =
        leftCOO.EWiseAdd rightCOO None FloatSemiring.addMult
        |> context.RunSync

    [<Benchmark>]
    [<ArgumentsSource("AvaliableContextsProvider")>]
    member this.EWiseAdditionCSR(context: OpenCLEvaluationContext) =
        leftCSR.EWiseAdd rightCOO None FloatSemiring.addMult
        |> context.RunSync

    [<Benchmark(Baseline=true)>]
    member this.EWiseAdditionMathNetSparse() = leftMathNetSparse + rightMathNetSparse

    /// Sequence of paths to files where data for benchmarking will be taken from
    static member InputMatricesProvider =
        let matricesFilenames =
            seq {
                "arc130.mtx"
            }

        let getFullPathToMatrix filename =
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
            |> Array.sortBy (fun (row, _, _) -> row)
            |> Array.unzip3
            |>
                fun (rows, cols, values) ->
                    let c f x y = f y x
                    let rows = rows |> Array.map (c (-) 1)
                    let cols = cols |> Array.map (c (-) 1)
                    {
                        Rows = rows
                        Columns = cols
                        Values = values
                        RowCount = nrows
                        ColumnCount = ncols
                    }

        matricesFilenames
        |> Seq.map
            (fun matrixFilename ->
                let fullPath = getFullPathToMatrix matrixFilename
                let matrixName = Path.GetFileNameWithoutExtension matrixFilename
                let matrixStructure = getCOO fullPath
                {
                    MatrixName = matrixName
                    MatrixStructure = matrixStructure
                }
            )

    static member AvaliableContextsProvider =
        let mutable e = ErrorCode.Unknown
        Cl.GetPlatformIDs &e
        |> Array.collect (fun platform -> Cl.GetDeviceIDs(platform, DeviceType.All, &e))
        |> Seq.ofArray
        |> Seq.map
            (fun device ->
                let platform = Cl.GetDeviceInfo(device, DeviceInfo.Platform, &e).CastTo<Platform>()
                let platformName = Cl.GetPlatformInfo(platform, PlatformInfo.Name, &e).ToString()
                let deviceType = Cl.GetDeviceInfo(device, DeviceInfo.Type, &e).CastTo<DeviceType>()
                OpenCLEvaluationContext(platformName, deviceType)
            )

// не уверен, что на каждой итерации трансфер данных снова происходит
// тк ссылка на массивы присваивается одна и та же
// надо бы каждый раз в iterationSetup делать глубокую копию исходных массивов

// нужен ToString для OpenCLEvaliationContext -- печатал бы DeviceInfo.Name
